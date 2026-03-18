package algorithm;

import datamodel.*;
import java.util.*;

public class PredicateTransferChainOperator {

    public static class Result {
        public final Relation leftFiltered;
        public final Relation middleFiltered;
        public final Relation rightFiltered;
        public final int leftBefore;
        public final int middleBefore;
        public final int rightBefore;
        public final double timeMs;

        public Result(Relation leftFiltered,
                      Relation middleFiltered,
                      Relation rightFiltered,
                      int leftBefore,
                      int middleBefore,
                      int rightBefore,
                      double timeMs) {
            this.leftFiltered = leftFiltered;
            this.middleFiltered = middleFiltered;
            this.rightFiltered = rightFiltered;
            this.leftBefore = leftBefore;
            this.middleBefore = middleBefore;
            this.rightBefore = rightBefore;
            this.timeMs = timeMs;
        }
    }

    /**
     * Chain predicate transfer for L -- M -- R
     *
     * Example:
     * Customer(cid) -- Order(cid, oid) -- Payment(oid)
     *
     * Step 1: build Bloom filter on R.rightJoinCol
     * Step 2: filter M on middleToRightCol
     * Step 3: transform filtered M into Bloom filter on middleToLeftCol
     * Step 4: filter L on leftJoinCol
     */
    public static Result transferThreeRelationChain(
            Relation left,
            Relation middle,
            Relation right,
            String leftJoinCol,         // e.g. Customer.cid
            String middleToLeftCol,     // e.g. Order.cid
            String middleToRightCol,    // e.g. Order.oid
            String rightJoinCol,        // e.g. Payment.oid
            int mBits,
            int kHashes) {

        long start = System.nanoTime();

        int leftBefore = left.size();
        int middleBefore = middle.size();
        int rightBefore = right.size();

        // Step 1: right -> middle
        BloomFilter rightFilter = new BloomFilter(mBits, kHashes);
        for (Tuple t : right.getTuples()) {
            rightFilter.add(t.get(rightJoinCol));
        }

        Set<Tuple> middleKept = new HashSet<>();
        for (Tuple t : middle.getTuples()) {
            Object val = t.get(middleToRightCol);
            if (val != null && rightFilter.mightContain(val)) {
                middleKept.add(t);
            }
        }

        Relation middleFiltered = new Relation(middle.getName(), middle.getColumns());
        middleFiltered.setTuples(middleKept);

        // Step 2: transform filtered middle -> left
        BloomFilter leftFilter = new BloomFilter(mBits, kHashes);
        for (Tuple t : middleFiltered.getTuples()) {
            leftFilter.add(t.get(middleToLeftCol));
        }

        Set<Tuple> leftKept = new HashSet<>();
        for (Tuple t : left.getTuples()) {
            Object val = t.get(leftJoinCol);
            if (val != null && leftFilter.mightContain(val)) {
                leftKept.add(t);
            }
        }

        Relation leftFiltered = new Relation(left.getName(), left.getColumns());
        leftFiltered.setTuples(leftKept);

        long end = System.nanoTime();
        double timeMs = (end - start) / 1_000_000.0;

        return new Result(
                leftFiltered,
                middleFiltered,
                right.copy(),
                leftBefore,
                middleBefore,
                rightBefore,
                timeMs
        );
    }
}

