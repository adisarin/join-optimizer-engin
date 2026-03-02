package algorithm;

import datamodel.*;
import java.util.*;

public class BloomSemiJoinOperator {

    /**
     * Approximate semi-join using Bloom filter:
     * Build Bloom filter from S.sColumn, then filter R by membership.
     *
     * IMPORTANT: Bloom filter has false positives, so afterSize may be larger
     * than exact semi-join, but should never be smaller (no false negatives),
     * assuming correct hashing and construction.
     */
    public static Result semiJoin(Relation R, Relation S,
                                  String rColumn, String sColumn,
                                  int mBits, int kHashes) {

        long startTime = System.nanoTime();
        int beforeSize = R.size();

        // Build Bloom filter from S
        BloomFilter bf = new BloomFilter(mBits, kHashes);
        for (Tuple t : S.getTuples()) {
            bf.add(t.get(sColumn));
        }

        // Filter R using Bloom membership
        Set<Tuple> result = new HashSet<>();
        int bloomMaybeCount = 0;

        for (Tuple rTuple : R.getTuples()) {
            Object rVal = rTuple.get(rColumn);
            if (rVal != null && bf.mightContain(rVal)) {
                result.add(rTuple);
                bloomMaybeCount++;
            }
        }

        Relation resultRel = new Relation(R.getName(), R.getColumns());
        resultRel.setTuples(result);

        long endTime = System.nanoTime();
        double timeMs = (endTime - startTime) / 1_000_000.0;

        return new Result(resultRel, beforeSize, result.size(), bloomMaybeCount, timeMs, mBits, kHashes);
    }

    public static class Result {
        public final Relation relation;
        public final int beforeSize;
        public final int afterSize;
        public final int bloomMaybeCount;
        public final double timeMs;
        public final int mBits;
        public final int kHashes;

        public Result(Relation relation, int beforeSize, int afterSize,
                      int bloomMaybeCount, double timeMs,
                      int mBits, int kHashes) {
            this.relation = relation;
            this.beforeSize = beforeSize;
            this.afterSize = afterSize;
            this.bloomMaybeCount = bloomMaybeCount;
            this.timeMs = timeMs;
            this.mBits = mBits;
            this.kHashes = kHashes;
        }

        public double getPruningPercent() {
            return (beforeSize == 0) ? 0 : 100.0 * (beforeSize - afterSize) / beforeSize;
        }
    }
}

