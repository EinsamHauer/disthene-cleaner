package net.iponweb.disthene.cleaner;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Andrei Ivanov
 * Date: 6/19/18
 */
public class DistheneCleanerParameters {

    private String tenant;
    private int threads = 1;
    private String cassandraContactPoint;
    private String elasticSearchContactPoint;
//    private List<Rollup> rollups = new ArrayList<>();
    private long threshold;
    private List<String> exclusions = new ArrayList<>();
    private boolean noop = false;

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public String getCassandraContactPoint() {
        return cassandraContactPoint;
    }

    public void setCassandraContactPoint(String cassandraContactPoint) {
        this.cassandraContactPoint = cassandraContactPoint;
    }

    public String getElasticSearchContactPoint() {
        return elasticSearchContactPoint;
    }

    public void setElasticSearchContactPoint(String elasticSearchContactPoint) {
        this.elasticSearchContactPoint = elasticSearchContactPoint;
    }

/*
    public void addRollup(String rollupString) {
        rollups.add(new Rollup(rollupString));
    }

    public List<Rollup> getRollups() {
        return rollups;
    }

*/
    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public void addExclusion(String exclusion) {
        exclusions.add(exclusion);
    }

    public List<String> getExclusions() {
        return exclusions;
    }

    public void setExclusions(List<String> exclusions) {
        this.exclusions = exclusions;
    }

    public boolean isNoop() {
        return noop;
    }

    public void setNoop(boolean noop) {
        this.noop = noop;
    }

    @Override
    public String toString() {
        return "DistheneCleanerParameters{" +
                "tenant='" + tenant + '\'' +
                ", threads=" + threads +
                ", cassandraContactPoint='" + cassandraContactPoint + '\'' +
                ", elasticSearchContactPoint='" + elasticSearchContactPoint + '\'' +
                ", threshold=" + threshold +
                ", exclusions=" + exclusions +
                ", noop=" + noop +
                '}';
    }
}
