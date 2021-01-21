package net.iponweb.disthene.cleaner;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.util.concurrent.*;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
public class Cleaner {

    private static final Logger logger = Logger.getLogger(Cleaner.class);

    private static final Pattern NORMALIZATION_PATTERN = Pattern.compile("[^0-9a-zA-Z_]");
    private static final String TABLE_QUERY = "SELECT COUNT(1) FROM SYSTEM.SCHEMA_COLUMNFAMILIES WHERE KEYSPACE_NAME=? AND COLUMNFAMILY_NAME=?";

    private final DistheneCleanerParameters parameters;

    private TransportClient client;
    private Session session;
    private final Pattern excludePattern;

    Cleaner(DistheneCleanerParameters parameters) {
        this.parameters = parameters;
        excludePattern = Pattern.compile(parameters.getExclusions().stream().map(WildcardUtil::getPathsRegExFromWildcard).collect(Collectors.joining("|")));
    }

    void clean() throws ExecutionException, InterruptedException, IOException {
        connectToES();
        connectToCassandra();

        // check if tenant table exists. If not, just return
        ResultSet resultSet = session.execute(TABLE_QUERY, "metric", String.format("metric_%s_60", getNormalizedTenant(parameters.getTenant())));
        if (resultSet.one().getLong(0) <= 0) {
            // skip
            logger.info("Tenant table doesn't exist. Skipping");
            return;
        }

        logger.info("Repairing paths");
        restoreBrokenPaths();
        logger.info("Repaired paths");
        logger.info("Waiting a couple of minutes to allow reindexing");
        Thread.sleep(120000);

        logger.info("Cleaning up obsolete metrics");
        cleanup();
        logger.info("Cleaned up obsolete metrics");
        logger.info("Waiting a couple of minutes to allow reindexing");
        Thread.sleep(120000);

        logger.info("Deleting empty paths");
        deleteEmptyPaths();
        logger.info("Deleted empty paths");
    }

    private void deleteEmptyPaths() throws InterruptedException {
        PathTree tree = getPathsTree();

        List<String> pathsToDelete = getEmptyPaths(tree);

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.debug("Bulk deleted " + request.numberOfActions() + " paths");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error(failure);
                    }
                })
                .setBulkActions(10_000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .setConcurrentRequests(4)
                .build();

        for (String path : pathsToDelete) {
            if (!parameters.isNoop()) {
                bulkProcessor.add(new DeleteRequest("cyanite_paths", "path", parameters.getTenant() + "_" + path));
                logger.info("Deleted path: " + path);
            } else {
                logger.info("Deleted path: " + path + " (noop)");
            }
        }

        if (bulkProcessor.awaitClose(Long.MAX_VALUE, TimeUnit.SECONDS)) {
            logger.info("Bulk processor closed");
        } else {
            logger.error("Failed to close bulk processor");
        }

    }

    private List<String> getEmptyPaths(PathTree tree) {
        List<String> result = new ArrayList<>();

        for (Map.Entry<String, PathData> entry : tree.roots.entrySet()) {
            hasToBeDeleted(entry.getKey(), entry.getValue(), result);
        }

        return result;
    }

    private void hasToBeDeleted(String path, PathData node, List<String> emptyPaths) {
        boolean toBeDeleted = !node.leaf;

        for (Map.Entry<String, PathData> entry : node.children.entrySet()) {
            boolean childHasToBeDeleted = hasToBeDeleted(path + ".", entry.getKey(), entry.getValue(), emptyPaths);
            toBeDeleted = toBeDeleted && childHasToBeDeleted;
        }

        if (toBeDeleted) emptyPaths.add(path);
    }

    private boolean hasToBeDeleted(String prefix, String path, PathData node, List<String> emptyPaths) {
        boolean toBeDeleted = !node.leaf;

        for (Map.Entry<String, PathData> entry : node.children.entrySet()) {
            boolean childHasToBeDeleted = hasToBeDeleted(prefix + path + ".", entry.getKey(), entry.getValue(), emptyPaths);
            toBeDeleted = toBeDeleted && childHasToBeDeleted;
        }

        if (toBeDeleted) emptyPaths.add(prefix + path);

        return toBeDeleted;
    }

    private void cleanup() {
        long cutoff = System.currentTimeMillis() / 1000 - parameters.getThreshold();

        String tenantTable = String.format("metric.metric_%s_60", getNormalizedTenant(parameters.getTenant()));
        String tenantTable900 = String.format("metric.metric_%s_900", getNormalizedTenant(parameters.getTenant()));

        logger.info("Tenant tables: " + tenantTable + ", " + tenantTable900);

        final PreparedStatement commonStatement = session.prepare(
                "select path from metric.metric where tenant = '" + parameters.getTenant() +
                        "' and path = ? and rollup = 60 and period = 89280 and " + "time >= " + cutoff + " limit 1"
        );

        final PreparedStatement tenantStatement = session.prepare(
                "select path from " + tenantTable + " where path = ? and time >= " + cutoff + " limit 1"
        );

        final PreparedStatement commonDeleteStatement = session.prepare(
                "delete from metric.metric where rollup = 900 and period = 69120 and path = ? and tenant = '" + parameters.getTenant() + "'"
        );

        final PreparedStatement tenantDeleteStatement = session.prepare(
                "delete from " + tenantTable900 + " where path = ?"
        );

        CountResponse countResponse = client.prepareCount("cyanite_paths")
                .setQuery(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                                .must(QueryBuilders.termQuery("leaf", true))
                )
                .execute()
                .actionGet();

        long total = countResponse.getCount();
        logger.info("Got " + total + " paths");

        final AtomicInteger counter = new AtomicInteger(0);
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parameters.getThreads()));

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(4, TimeUnit.HOURS))
                .setSize(10_000)
                .setQuery(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                                .must(QueryBuilders.termQuery("leaf", true))
                )
                .setSearchType(SearchType.SCAN)
                .addField("path")
                .execute().actionGet();

        response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(new TimeValue(4, TimeUnit.HOURS))
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                String path = hit.field("path").getValue();

                if (!excludePattern.matcher(path).matches()) {
                    ListenableFuture<Void> future = executor.submit(
                            new SinglePathCallable(
                                    client,
                                    session,
                                    parameters.getTenant(),
                                    commonStatement,
                                    tenantStatement,
                                    commonDeleteStatement,
                                    tenantDeleteStatement,
                                    path,
                                    parameters.isNoop()
                            )
                    );
                    Futures.addCallback(future, new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(Void aVoid) {
                            int cc = counter.addAndGet(1);
                            if (cc % 100000 == 0) {
                                logger.info("Processed: " + (int)(((double) cc / total) * 100) + "%");
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            logger.error("Unexpected error:", throwable);
                        }
                    });
                } else {
                    counter.addAndGet(1);
                }

            }

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(4, TimeUnit.HOURS))
                    .execute().actionGet();
        }

        executor.shutdown();

        try {
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed: ", e);
        }

        logger.info("Processed " + counter.get() + " paths");
    }

    private static String getNormalizedTenant(String tenant) {
        return NORMALIZATION_PATTERN.matcher(tenant).replaceAll("_");
    }

    private void connectToES() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "cyanite")
                .put("client.transport.ping_timeout", "120s")
                .build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(parameters.getElasticSearchContactPoint(), 9300));
    }

    private void connectToCassandra() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReceiveBufferSize(8388608);
        socketOptions.setSendBufferSize(1048576);
        socketOptions.setTcpNoDelay(false);
        socketOptions.setReadTimeoutMillis(1_000_000);
        socketOptions.setReadTimeoutMillis(1_000_000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 32);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 32);
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, 128);
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 128);

        Cluster.Builder builder = Cluster.builder()
                .withSocketOptions(socketOptions)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(9042);

        builder.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
        builder.addContactPoint(parameters.getCassandraContactPoint());

        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        session = cluster.connect();
    }

    private void restoreBrokenPaths() throws IOException, ExecutionException, InterruptedException {
        PathTree tree = getPathsTree();

        int depth = 1;

        for (Map.Entry<String, PathData> entry : tree.roots.entrySet()) {
            if (!entry.getValue().real) {
                restoreSinglePath(entry.getKey(), entry.getValue().leaf && entry.getValue().children.size() == 0, depth);
            }

            restoreBrokenPaths(entry.getValue(), entry.getKey(), depth + 1);
        }
    }

    private void restoreBrokenPaths(PathData node, String prefix, int depth) throws IOException, ExecutionException, InterruptedException {
        for (Map.Entry<String, PathData> entry : node.children.entrySet()) {
            if (!entry.getValue().real) {
                restoreSinglePath(prefix + "." + entry.getKey(), entry.getValue().leaf && entry.getValue().children.size() == 0, depth);
            }

            restoreBrokenPaths(entry.getValue(), prefix + "." + entry.getKey(), depth + 1);
        }

    }

    private void restoreSinglePath(String path, boolean leaf, int depth) throws IOException, ExecutionException, InterruptedException {
        if (!parameters.isNoop()) {
            client.prepareIndex("cyanite_paths", "path", parameters.getTenant() + "_" + path)
                    .setSource(
                            XContentFactory.jsonBuilder().startObject()
                                    .field("tenant", parameters.getTenant())
                                    .field("path", path)
                                    .field("depth", depth)
                                    .field("leaf", leaf)
                                    .endObject()
                    )
                    .execute()
                    .get();
            logger.info("Will reindex path " + path + " with depth " + depth + " as " + (leaf ? "leaf" : "non-leaf"));
        } else {
            logger.info("Will reindex path " + path + " with depth " + depth + " as " + (leaf ? "leaf" : "non-leaf") + " (noop)");
        }
    }

    private PathTree getPathsTree() {
        PathTree tree = new PathTree();

        CountResponse countResponse = client.prepareCount("cyanite_paths")
                .setQuery(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                .execute()
                .actionGet();

        long count = countResponse.getCount();
        logger.info("A priori number of paths: " + count);
        logger.info("Retrieving paths");

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(4, TimeUnit.HOURS))
                .setSize(10_000)
                .setQuery(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                .addFields("path", "leaf")
                .execute().actionGet();

        response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(new TimeValue(4, TimeUnit.HOURS))
                .execute().actionGet();

        long counter = 0;

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                tree.addPath(hit.field("path").getValue(), hit.field("leaf").getValue());
                counter++;
            }
            logger.info("Retrieved: " + (int)(((double) counter / count) * 100) + "%");

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(4, TimeUnit.HOURS))
                    .execute().actionGet();
        }

        logger.info("Number of paths: " + counter);

        if (counter < count) {
            logger.error("Number of paths received is less than a priori count. " + counter + " VS " + count);
            throw new RuntimeException("Number of paths received is less than a priori count. " + counter + " VS " + count);
        } else {
            logger.info("Counting check passed: " + counter + " VS " + count);
        }

        return tree;
    }

    private static class PathTree {
        private final Map<String, PathData> roots = new HashMap<>();

        void addPath(String path, boolean leaf) {
            String[] parts = path.split("\\.");

            PathData node = roots.computeIfAbsent(parts[0], p -> new PathData(parts.length == 1, leaf && parts.length == 1));

            if (parts.length == 1) node.real = true;

            for (int i = 1; i < parts.length; i++) {
                boolean real = parts.length == i + 1;
                node = node.children.computeIfAbsent(parts[i], p -> new PathData(real, leaf && real));
                if (real) node.real = true;
            }
        }

    }

    private static class PathData {
        private boolean real;
        private final boolean leaf;
        private final Map<String, PathData> children = new HashMap<>();

        public PathData(boolean real, boolean leaf) {
            this.real = real;
            this.leaf = leaf;
        }
    }

    private static class SinglePathCallable implements Callable<Void> {
        private final TransportClient client;
        private final Session session;
        private final String tenant;
        private final PreparedStatement commonStatement;
        private final PreparedStatement tenantStatement;
        private final PreparedStatement commonDeleteStatement;
        private final PreparedStatement tenantDeleteStatement;
        private final String path;
        private final boolean noop;


        SinglePathCallable(
                TransportClient client,
                Session session,
                String tenant,
                PreparedStatement commonStatement,
                PreparedStatement tenantStatement,
                PreparedStatement commonDeleteStatement,
                PreparedStatement tenantDeleteStatement,
                String path,
                boolean noop
        ) {
            this.client = client;
            this.session = session;
            this.tenant = tenant;
            this.commonStatement = commonStatement;
            this.tenantStatement = tenantStatement;
            this.commonDeleteStatement = commonDeleteStatement;
            this.tenantDeleteStatement = tenantDeleteStatement;
            this.path = path;
            this.noop = noop;
        }

        @Override
        public Void call() throws Exception {
            List<ResultSetFuture> futures = new ArrayList<>();
            futures.add(session.executeAsync(commonStatement.bind(path)));
            futures.add(session.executeAsync(tenantStatement.bind(path)));

            if (Futures.allAsList(futures).get().stream().mapToInt(rs -> rs.all().size()).sum() <= 0) {
                if (!noop) {
                    session.execute(commonDeleteStatement.bind(path));
                    session.execute(tenantDeleteStatement.bind(path));

                    client.prepareDelete("cyanite_paths", "path", tenant + "_" + path).execute().get();
                    logger.info("Deleted path data: " + path);
                } else {
                    logger.info("Deleted path data: " + path + " (noop)");
                }
            }

            return null;
        }
    }
}
