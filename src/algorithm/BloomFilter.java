package algorithm;

import java.util.BitSet;

public class BloomFilter {
    private final BitSet bits;
    private final int m;      // number of bits
    private final int k;      // number of hash functions

    public BloomFilter(int mBits, int kHashes) {
        this.m = Math.max(1, mBits);
        this.k = Math.max(1, kHashes);
        this.bits = new BitSet(m);
    }

    public void add(Object value) {
        int h1 = hash1(value);
        int h2 = hash2(value);

        for (int i = 0; i < k; i++) {
            int combined = h1 + i * h2;
            int idx = positiveMod(combined, m);
            bits.set(idx);
        }
    }

    public boolean mightContain(Object value) {
        int h1 = hash1(value);
        int h2 = hash2(value);

        for (int i = 0; i < k; i++) {
            int combined = h1 + i * h2;
            int idx = positiveMod(combined, m);
            if (!bits.get(idx)) return false;
        }
        return true;
    }

    private int hash1(Object value) {
        return (value == null) ? 0 : value.hashCode();
    }

    private int hash2(Object value) {
        // A second hash derived from hashCode with mixing
        int h = (value == null) ? 0 : value.hashCode();
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        return h | 1; // ensure non-zero odd step
    }

    private int positiveMod(int x, int mod) {
        int r = x % mod;
        return (r < 0) ? r + mod : r;
    }
}

