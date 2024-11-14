package edu.smu.smusql.cache;

import edu.smu.smusql.interfaces.RowEntry;
import java.util.*;

public class CacheEntry {
    private final List<RowEntry> results;
    private final long creationTime;
    private static final long TTL = 30000; // 30 seconds TTL

    /**
     * creates a new cache entry with the given results.
     * @param results The query results to cache
     */
    public CacheEntry(List<RowEntry> results) {
        this.results = new ArrayList<>(results);
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * returns copy of the cached results.
     * @return a new list containing the cached results
     */
    public List<RowEntry> getResults() {
        return new ArrayList<>(results);
    }

    /**
     * checks if this cache entry has expired.
     * @return true if the entry has exceeded its TTL, false otherwise
     */
    public boolean isExpired() {
        return System.currentTimeMillis() - creationTime > TTL;
    }
}
