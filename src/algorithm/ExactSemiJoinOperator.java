package algorithm;

import datamodel.*;
import java.util.*;

public class ExactSemiJoinOperator {

    /**
     * Exact semi-join: R ⋉ S
     * Keep only R tuples where rColumn value exists in S.sColumn
     */
    public static SemiJoinResult semiJoin(Relation R, Relation S,
                                          String rColumn, String sColumn) {
        long startTime = System.nanoTime();

        int beforeSize = R.size();

        // Extract all values from S on sColumn
        Set<Object> sValues = new HashSet<>();
        for (Tuple t : S.getTuples()) {
            sValues.add(t.get(sColumn));
        }

        // Keep only R tuples where rColumn is in sValues
        Set<Tuple> result = new HashSet<>();
        for (Tuple rTuple : R.getTuples()) {
            Object rVal = rTuple.get(rColumn);
            if (rVal != null && sValues.contains(rVal)) {
                result.add(rTuple);
            }
        }

        Relation resultRel = new Relation(R.getName(), R.getColumns());
        resultRel.setTuples(result);

        long endTime = System.nanoTime();
        double timeMs = (endTime - startTime) / 1_000_000.0;

        return new SemiJoinResult(resultRel, beforeSize, result.size(), timeMs);
    }

    // Data class for metrics
    public static class SemiJoinResult {
        public final Relation relation;
        public final int beforeSize;
        public final int afterSize;
        public final double timeMs;

        public SemiJoinResult(Relation rel, int before, int after, double time) {
            this.relation = rel;
            this.beforeSize = before;
            this.afterSize = after;
            this.timeMs = time;
        }

        public int getPrunedCount() {
            return beforeSize - afterSize;
        }

        public double getPruningPercent() {
            return (beforeSize == 0) ? 0 : 100.0 * (beforeSize - afterSize) / beforeSize;
        }
    }
}

