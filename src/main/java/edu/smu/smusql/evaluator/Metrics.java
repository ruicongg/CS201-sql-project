package edu.smu.smusql.evaluator;

import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final AtomicLong totalQueries = new AtomicLong(0);
    private AtomicLong totalTimeNano = new AtomicLong(0);
    private AtomicLong minLatencyNano = new AtomicLong(Long.MAX_VALUE);
    private AtomicLong maxLatencyNano = new AtomicLong(0);

    public void recordQuery(long timeNano) {
        totalQueries.incrementAndGet();
        totalTimeNano.addAndGet(timeNano);
        updateMinLatency(timeNano);
        updateMaxLatency(timeNano);
    }

    private void updateMinLatency(long timeNano) {
        long currentMin;
        do {
            currentMin = minLatencyNano.get();
            if (timeNano >= currentMin) {
                break;
            }
        } while (!minLatencyNano.compareAndSet(currentMin, timeNano));
    }

    private void updateMaxLatency(long timeNano) {
        long currentMax;
        do {
            currentMax = maxLatencyNano.get();
            if (timeNano <= currentMax) {
                break;
            }
        } while (!maxLatencyNano.compareAndSet(currentMax, timeNano));
    }

    public double getThroughput() {
        // Assuming totalTimeNano is in nanoseconds
        return (totalQueries.get() / (totalTimeNano.get() / 1e9));
    }

    public double getAverageLatency() {
        return totalQueries.get() > 0 ? (totalTimeNano.get() / (double) totalQueries.get()) / 1e6 : 0; // in milliseconds
    }

    public double getMinLatency() {
        return minLatencyNano.get() == Long.MAX_VALUE ? 0 : minLatencyNano.get() / 1e6;
    }

    public double getMaxLatency() {
        return maxLatencyNano.get() / 1e6;
    }

    public long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory(); // in bytes
    }

    public long getMaxMemory() {
        return Runtime.getRuntime().maxMemory(); // in bytes
    }

    public long getTotalMemory() {
        return Runtime.getRuntime().totalMemory(); // in bytes
    }
}
