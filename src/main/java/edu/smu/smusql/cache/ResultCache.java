package edu.smu.smusql.cache;

import edu.smu.smusql.interfaces.RowEntry;
import java.util.*;

/**
* cache implementation for SQL query results using an LRU eviction strategy.
* stores SELECT query results to improve performance of repeated queries.
* @author gav
* @since 2024-10-30
*/
public class ResultCache {
    private final int capacity;
    private final Map<String, CacheEntry> cache = new HashMap<>();
    private final Map<String, Long> lastAccessed = new HashMap<>();

    /**
     * constructs a new ResultCache with specified capacity.
     *
     * @param capacity the maximum number of queries that can be stored in the cache
     */
    public ResultCache(int capacity) {
        this.capacity = capacity;
    }

    /**
     * stores a query result in the cache.
     * if cache size = capacity, the least recently used entry will be evicted.
     *
     * @param query   SQL query string used as the cache key
     * @param results list of row entries representing the query results
     */
    public void put(String query, List<RowEntry> results) {
        if (cache.size() >= capacity) {
            evictLRU();
        }
        cache.put(query, new CacheEntry(results));
    }

    /**
     * retrieves cached results for a given query.
     * @param query SQL query string to look up
     * @return optional containing the cached results if present and not expired,
     *         empty Optional otherwise
     */
    public Optional<List<RowEntry>> get(String query) {
        CacheEntry entry = cache.get(query);

        if (entry != null && !entry.isExpired()) {
            return Optional.of(entry.getResults());
        }

        cache.remove(query);
        return Optional.empty();
    }

    /**
     * invalidates all entries in the cache
     * called when tables are modified (INSERT, UPDATE, DELETE operations).
     */
    public void invalidateCache() {
        cache.clear();
        lastAccessed.clear();
    }

    /**
     * removes the least recently used entry from the cache.
     */
    private void evictLRU() {
        if (lastAccessed.isEmpty()) return;

        String lruQuery = lastAccessed.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);

        if (lruQuery != null) {
            cache.remove(lruQuery);
            lastAccessed.remove(lruQuery);
        }
    }
}