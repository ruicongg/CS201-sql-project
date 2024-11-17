package edu.smu.smusql.bloomfilter;

import java.util.BitSet;
import java.util.Random;

public class BloomFilter {
    private BitSet bitSet;
    private int size;
    private int[] hashSeeds;
    private int hashCount;

    /*
     * Allow us to disable the bloom filter
     */
    public BloomFilter() {
        size = 0;
    }

    public int getSize() {
        return size;
    }

    /*
     * Using the bloom filter
     */
    public BloomFilter(int size, int hashCount) {
        this.size = size;
        this.hashCount = hashCount;
        this.bitSet = new BitSet(size);
        this.hashSeeds = new int[hashCount];
        Random random = new Random(2012024);

        for (int i = 0; i < hashCount; i++) {
            hashSeeds[i] = random.nextInt();
        }
    }

    private int hash(String data, int seed) {
        int result = 0;
        for (int i = 0; i < data.length(); i++) {
            result = result * seed + data.charAt(i);
        }
        return (result & 0x7fffffff) % size;
    }

    public void add(String data) {
        for (int seed : hashSeeds) {
            int hash = hash(data, seed);
            bitSet.set(hash);
        }
    }

    public boolean mightContain(String data) {
        for (int seed : hashSeeds) {
            int hash = hash(data, seed);
            if (!bitSet.get(hash)) {
                return false;
            }
        }
        return true;
    }
}
