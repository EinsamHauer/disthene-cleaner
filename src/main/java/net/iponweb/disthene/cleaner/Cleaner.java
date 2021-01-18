package net.iponweb.disthene.cleaner;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.util.concurrent.*;
import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
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

/**
 * Author: Andrei Ivanov
 * Date: 6/19/18
 */
@SuppressWarnings("UnstableApiUsage")
class Cleaner {
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
        long cutoff = System.currentTimeMillis() / 1000 - parameters.getThreshold();

        String tenantTable = String.format("metric.metric_%s_60", getNormalizedTenant(parameters.getTenant()));
        String tenantTable900 = String.format("metric.metric_%s_900", getNormalizedTenant(parameters.getTenant()));

        logger.info("Tenant tables: " + tenantTable + ", " + tenantTable900);

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

        logger.info("Getting paths");
        final List<String> paths = getTenantPaths(parameters.getTenant());
        logger.info("Got " + paths.size() + " paths");
        paths.removeIf(path -> excludePattern.matcher(path).matches());
        logger.info("Left " + paths.size() + " paths after excluding");

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parameters.getThreads()));

        int total = paths.size();
        final AtomicInteger counter = new AtomicInteger(0);

        for (String path : paths) {
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
                        logger.info("Processed: " + cc * 100 / total + "%");
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error("Unexpected error:", throwable);
                }
            });
        }

        executor.shutdown();

        try {
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed: ", e);
        }

        logger.info("Waiting a couple of minutes to allow reindexing");
        Thread.sleep(120000);

        logger.info("Deleting empty paths");
        deleteEmptyPaths();
    }

    private void connectToES() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "cyanite").build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(parameters.getElasticSearchContactPoint(), 9300));
    }

    private void connectToCassandra() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReceiveBufferSize(8388608);
        socketOptions.setSendBufferSize(1048576);
        socketOptions.setTcpNoDelay(false);
        socketOptions.setReadTimeoutMillis(1000000);
        socketOptions.setReadTimeoutMillis(1000000);

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

    private List<String> getTenantPaths(String tenant) {
        final List<String> paths = new ArrayList<>();

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setScroll(new TimeValue(120000))
                .setSize(100000)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.filteredQuery(
                        QueryBuilders.regexpQuery("path", ".*"),
                        FilterBuilders.termFilter("tenant", tenant)), FilterBuilders.termFilter("leaf", true)))
                .addField("path")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                paths.add(hit.field("path").getValue());
            }

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(120000))
                    .execute().actionGet();
        }

        return paths;
    }

    private static String getNormalizedTenant(String tenant) {
        return NORMALIZATION_PATTERN.matcher(tenant).replaceAll("_");
    }

    private void deleteEmptyPaths() throws ExecutionException, InterruptedException {
        PathTree tree = getPathsTree();

        // traverse tree
        List<String> pathsToDelete = getEmptyPaths(tree);

        for (String path : pathsToDelete) {
            if (!parameters.isNoop()) {
                client.prepareDelete("cyanite_paths", "path", parameters.getTenant() + "_" + path).execute().get();
                logger.info("Deleted path: " + path);
            } else {
                logger.info("Deleted path: " + path + " (noop)");
            }
        }
    }

    private List<String> getEmptyPaths(PathTree tree) {
        List<String> result = new ArrayList<>();

        for (PathNode node : tree.roots) {
            hasToBeDeleted(node, result);
        }

        return result;
    }

    private boolean hasToBeDeleted(PathNode node, List<String> emptyPaths) {
        boolean toBeDeleted = !node.leaf;

        for (PathNode child : node.children) {
            boolean childHasToBeDeleted = hasToBeDeleted(child, emptyPaths);
            toBeDeleted = toBeDeleted && childHasToBeDeleted;
        }

        if (toBeDeleted) emptyPaths.add(node.path);

        return toBeDeleted;
    }

    private void restoreBrokenPaths() throws ExecutionException, InterruptedException, IOException {
        PathTree tree = getPathsTree();
        Set<String> repairedPaths = new HashSet<>();

        // restore all paths with depth > 1 and no parent
        for (PathNode node : tree.roots) {
            if (node.depth > 1) {

                String[] parts = node.path.split("\\.");

                final StringBuilder sb = new StringBuilder();

                for (int i = 0; i < parts.length; i++) {
                    if (sb.toString().length() > 0) {
                        sb.append(".");
                    }
                    sb.append(parts[i]);

                    String pathToIndex = sb.toString();

                    if (!tree.map.containsKey(pathToIndex) && !repairedPaths.contains(pathToIndex)) {

                        if (!parameters.isNoop()) {
                            client.prepareIndex("cyanite_paths", "path", parameters.getTenant() + "_" + pathToIndex)
                                    .setSource(
                                            XContentFactory.jsonBuilder().startObject()
                                                    .field("tenant", parameters.getTenant())
                                                    .field("path", pathToIndex)
                                                    .field("depth", (i + 1))
                                                    .field("leaf", (i == parts.length - 1 && node.children.size() <= 0))
                                                    .endObject()
                                    )
                                    .execute()
                                    .get();
                            logger.info("Will reindex path " + pathToIndex + " with depth " + (i + 1) + " as " + ((i == parts.length - 1 && node.children.size() <= 0) ? "leaf" : "non-leaf"));
                        } else {
                            logger.info("Will reindex path " + pathToIndex + " with depth " + (i + 1) + " as " + ((i == parts.length - 1 && node.children.size() <= 0) ? "leaf" : "non-leaf") + " (noop)");
                        }

                        repairedPaths.add(pathToIndex);
                    }
                }
            }
        }
    }

    private PathTree getPathsTree() {
        PathTree tree = new PathTree();
        List<Path> paths = new ArrayList<>();

        CountResponse countResponse = client.prepareCount("cyanite_paths")
                .setQuery(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                .execute()
                .actionGet();

        long count = countResponse.getCount();

        logger.info("A priori number of paths: " + count);

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setScroll(new TimeValue(120000))
                .setSize(100000)
                .setQuery(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                .addFields("path", "leaf", "depth")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                paths.add(new Path(hit.field("path").getValue(), hit.field("leaf").getValue(), hit.field("depth").getValue()));
            }

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(120000))
                    .execute().actionGet();
        }

        logger.info("Number of paths: " + paths.size());

        if (paths.size() < count) {
            logger.error("Number of paths received is less than a priori count. " + paths.size() + " VS " + count);
            throw new RuntimeException("Number of paths received is less than a priori count. " + paths.size() + " VS " + count);
        } else {
            logger.info("Counting check passed: " + paths.size() + " VS " + count);
        }

        Collections.sort(paths);
        for (Path path : paths) {
            tree.addNode(new PathNode(path.path, path.leaf, path.depth));
        }

        return tree;
    }


    private static class PathTree {
        private final List<PathNode> roots = new ArrayList<>();
        private final Map<String, PathNode> map = new HashMap<>();

        void addNode(PathNode node) {
            String parent = node.getParent();
            if (parent != null && map.containsKey(parent)) {
                map.get(parent).addChild(node);
            } else {
                roots.add(node);
            }
            map.put(node.path, node);
        }
    }

    private static class PathNode {
        private final String path;
        private final List<PathNode> children = new ArrayList<>();
        private final boolean leaf;
        private final int depth;

        PathNode(String path, boolean leaf, int depth) {
            this.path = path;
            this.leaf = leaf;
            this.depth = depth;
        }

        String getParent() {
            if (path != null) {
                int lastDot = path.lastIndexOf(".");
                if (lastDot != -1) {
                    return path.substring(0, lastDot);
                }
            }

            return null;
        }

        void addChild(PathNode node) {
            this.children.add(node);
        }
    }

    private static class Path implements Comparable<Path> {
        private final String path;
        private final boolean leaf;
        private final int depth;

        Path(String path, boolean leaf, int depth) {
            this.path = path;
            this.leaf = leaf;
            this.depth = depth;
        }

        @Override
        public int compareTo(Path o) {
            return this.path.compareTo(o.path);
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
