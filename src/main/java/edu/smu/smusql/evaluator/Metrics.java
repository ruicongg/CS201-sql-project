package edu.smu.smusql.evaluator;

public class Metrics {

    /**
     * Calculates the used memory in megabytes (MB).
     *
     * @return Used memory in MB, rounded to the nearest whole number.
     */
    public long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        double usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0); // Convert bytes to MB
        return Math.round(usedMemoryMB);
    }
}
