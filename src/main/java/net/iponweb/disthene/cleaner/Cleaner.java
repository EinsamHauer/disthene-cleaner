package net.iponweb.disthene.cleaner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Cleaner {

    private static final Logger logger = LogManager.getLogger(Cleaner.class);

    private static final Pattern NORMALIZATION_PATTERN = Pattern.compile("[^0-9a-zA-Z_]");
    private static final String TABLE_QUERY = "SELECT COUNT(1) FROM SYSTEM_SCHEMA.TABLES WHERE KEYSPACE_NAME=? AND TABLE_NAME=?";
    private static final String INDEX_NAME = "disthene";

    private final DistheneCleanerParameters parameters;

    private RestHighLevelClient client;
    private CqlSession session;
    private final Pattern excludePattern;

    Cleaner(DistheneCleanerParameters parameters) {
        this.parameters = parameters;
        excludePattern = Pattern.compile(parameters.getExclusions().stream().map(WildcardUtil::getPathsRegExFromWildcard).collect(Collectors.joining("|")));
    }

    void clean() throws InterruptedException, IOException {
        connectToES();
        connectToCassandra();

        // check if tenant table exists. If not, just return
        ResultSet resultSet = session.execute(TABLE_QUERY, "metric", String.format("metric_%s_60", getNormalizedTenant(parameters.getTenant())));
        if (Objects.requireNonNull(resultSet.one()).getLong(0) <= 0) {
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

    private void deleteEmptyPaths() throws InterruptedException, IOException {
        PathTree tree = getPathsTree();

        List<String> pathsToDelete = getEmptyPaths(tree);

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, listener) -> client.bulkAsync(request, RequestOptions.DEFAULT, listener),
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
                }, "deleteEmptyPaths")
                .setBulkActions(10_000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .setConcurrentRequests(4)
                .build();

        for (String path : pathsToDelete) {
            if (!parameters.isNoop()) {
                bulkProcessor.add(new DeleteRequest(INDEX_NAME, parameters.getTenant() + "_" + path));
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

    private void cleanup() throws IOException {
        long cutoff = System.currentTimeMillis() / 1000 - parameters.getThreshold();

        String table60 = String.format("metric.metric_%s_60", getNormalizedTenant(parameters.getTenant()));
        String table900 = String.format("metric.metric_%s_900", getNormalizedTenant(parameters.getTenant()));

        logger.info("Tenant tables: " + table60 + ", " + table900);

        final PreparedStatement statement = session.prepare(
                "select path from " + table60 + " where path = ? and time >= " + cutoff + " limit 1"
        );

        final PreparedStatement deleteStatement60 = session.prepare(
                "delete from " + table60 + " where path = ?"
        );

        final PreparedStatement deleteStatement900 = session.prepare(
                "delete from " + table900 + " where path = ?"
        );

        CountRequest countRequest = new CountRequest(INDEX_NAME)
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                        .must(QueryBuilders.termQuery("leaf", true)));


        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);

        long total = countResponse.getCount();
        logger.info("Got " + total + " paths");

        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(parameters.getThreads());

        final Scroll scroll = new Scroll(TimeValue.timeValueHours(4L));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource("path", null)
                .size(10_000)
                .query(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("tenant", parameters.getTenant()))
                                .must(QueryBuilders.termQuery("leaf", true))
                );

        SearchRequest request = new SearchRequest(INDEX_NAME)
                .source(sourceBuilder)
                .scroll(scroll);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();

        SearchHits hits = response.getHits();

        while (hits.getHits().length > 0) {
            for (SearchHit hit : hits) {
                String path = String.valueOf(hit.getSourceAsMap().get("path"));

                if (!excludePattern.matcher(path).matches()) {

                    CompletableFuture.supplyAsync(
                            new SinglePathSupplier(
                                    client,
                                    session,
                                    parameters.getTenant(),
                                    statement,
                                    deleteStatement60,
                                    deleteStatement900,
                                    path,
                                    parameters.isNoop()
                            ), executor)
                            .whenComplete((unused, throwable) -> {
                                if (throwable != null) {
                                    logger.error("Path processing failed", throwable);
                                } else {
                                    int cc = counter.addAndGet(1);
                                    if (cc % 100000 == 0) {
                                        logger.info("Processed: " + (int) (((double) cc / total) * 100) + "%");
                                    }
                                }

                            });
                } else {
                    counter.addAndGet(1);
                }

            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            hits = response.getHits();
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
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(parameters.getElasticSearchContactPoint(), 9200)));
    }

    private void connectToCassandra() {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .withStringList(DefaultDriverOption.CONTACT_POINTS, List.of(parameters.getCassandraContactPoint() + ":9042"))
                        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 128)
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(1_000_000))
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE")
                        .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class)
                        .build();

        session = CqlSession.builder().withConfigLoader(loader).build();

        Metadata metadata = session.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Node node : metadata.getNodes().values()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s",
                    node.getDatacenter(),
                    node.getBroadcastAddress().isPresent() ? node.getBroadcastAddress().get().toString() : "unknown", node.getRack()));
        }
    }

    private void restoreBrokenPaths() throws IOException {
        PathTree tree = getPathsTree();

        int depth = 1;

        for (Map.Entry<String, PathData> entry : tree.roots.entrySet()) {
            if (!entry.getValue().real) {
                restoreSinglePath(entry.getKey(), entry.getValue().leaf && entry.getValue().children.size() == 0, depth);
            }

            restoreBrokenPaths(entry.getValue(), entry.getKey(), depth + 1);
        }
    }

    private void restoreBrokenPaths(PathData node, String prefix, int depth) throws IOException {
        for (Map.Entry<String, PathData> entry : node.children.entrySet()) {
            if (!entry.getValue().real) {
                restoreSinglePath(prefix + "." + entry.getKey(), entry.getValue().leaf && entry.getValue().children.size() == 0, depth);
            }

            restoreBrokenPaths(entry.getValue(), prefix + "." + entry.getKey(), depth + 1);
        }

    }

    private void restoreSinglePath(String path, boolean leaf, int depth) throws IOException {
        if (!parameters.isNoop()) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(parameters.getTenant() + "_" + path).source(
                    XContentFactory.jsonBuilder().startObject()
                            .field("tenant", parameters.getTenant())
                            .field("path", path)
                            .field("depth", depth)
                            .field("leaf", leaf)
                            .endObject()
            );

            client.index(request, RequestOptions.DEFAULT);

            logger.info("Will reindex path " + path + " with depth " + depth + " as " + (leaf ? "leaf" : "non-leaf"));
        } else {
            logger.info("Will reindex path " + path + " with depth " + depth + " as " + (leaf ? "leaf" : "non-leaf") + " (noop)");
        }
    }

    private PathTree getPathsTree() throws IOException {
        PathTree tree = new PathTree();

        CountRequest countRequest = new CountRequest(INDEX_NAME)
                .query(QueryBuilders.termQuery("tenant", parameters.getTenant()));

        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);

        long count = countResponse.getCount();
        logger.info("A priori number of paths: " + count);
        logger.info("Retrieving paths");


        final Scroll scroll = new Scroll(TimeValue.timeValueHours(4L));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource(new String[]{"path", "leaf"}, null)
                .size(10_000)
                .query(QueryBuilders.termQuery("tenant", parameters.getTenant()));

        SearchRequest request = new SearchRequest(INDEX_NAME)
                .source(sourceBuilder)
                .scroll(scroll);


        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();

        SearchHits hits = response.getHits();

        long counter = 0;

        while (hits.getHits().length > 0) {
            for (SearchHit hit : hits) {
                tree.addPath((String) hit.getSourceAsMap().get("path"), (Boolean) hit.getSourceAsMap().get("leaf"));
                counter++;
            }

            logger.info("Retrieved: " + (int) (((double) counter / count) * 100) + "%");

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            hits = response.getHits();
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

    private static class SinglePathSupplier implements Supplier<Void> {

        private final RestHighLevelClient client;
        private final CqlSession session;
        private final String tenant;
        private final PreparedStatement statement;
        private final PreparedStatement deleteStatement60;
        private final PreparedStatement deleteStatement900;
        private final String path;
        private final boolean noop;

        public SinglePathSupplier(RestHighLevelClient client, CqlSession session, String tenant, PreparedStatement statement, PreparedStatement deleteStatement60, PreparedStatement deleteStatement900, String path, boolean noop) {
            this.client = client;
            this.session = session;
            this.tenant = tenant;
            this.statement = statement;
            this.deleteStatement60 = deleteStatement60;
            this.deleteStatement900 = deleteStatement900;
            this.path = path;
            this.noop = noop;
        }

        @Override
        public Void get() {
            session.executeAsync(statement.bind(path))
                    .whenComplete((asyncResultSet, throwable) -> {
                        if (throwable != null) {
                            logger.error("Select statement failed", throwable);
                        } else {
                            if (Objects.requireNonNull(asyncResultSet.one()).getLong(0) <= 0) {
                                if (!noop) {
                                    try {
                                        session.execute(deleteStatement60.bind(path));
                                        session.execute(deleteStatement900.bind(path));

                                        DeleteRequest request = new DeleteRequest(INDEX_NAME, tenant + "_" + path);
                                        client.delete(request, RequestOptions.DEFAULT);

                                        logger.info("Deleted path data: " + path);
                                    } catch (IOException e) {
                                        logger.error("Delete failed", e);
                                    }
                                } else {
                                    logger.info("Deleted path data: " + path + " (noop)");
                                }
                            }
                        }
                    });

            return null;
        }
    }


}
