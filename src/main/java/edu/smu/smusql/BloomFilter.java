package edu.smu.smusql;

public class BloomFilter {
    private final boolean[] bitArray;
    private final int size;

    public BloomFilter(int size) {
        this.size = size;
        this.bitArray = new boolean[size];
    }

    public void add(String key) {
        int hash1 = key.hashCode() % size;
        int hash2 = (key.hashCode() / 2) % size;
        bitArray[Math.abs(hash1)] = true;
        bitArray[Math.abs(hash2)] = true;
    }

    public boolean mightContain(String key) {
        int hash1 = key.hashCode() % size;
        int hash2 = (key.hashCode() / 2) % size;
        return bitArray[Math.abs(hash1)] && bitArray[Math.abs(hash2)];
    }
}