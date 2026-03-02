package algorithm;

import datamodel.*;
import query.*;
import java.util.*;

public class YannakakisReducer {

    public static class ReducerMetrics {
        public final Map<String, Integer> beforeSizes = new HashMap<>();
        public final Map<String, Integer> afterSizes = new HashMap<>();
        public final Map<String, Double> times = new HashMap<>();

        public void record(String rel, int before, int after, double timeMs) {
            beforeSizes.put(rel, before);
            afterSizes.put(rel, after);
            times.put(rel, timeMs);
        }

        public void print() {
            System.out.println("\n[METRICS] Semi-Join Reduction Summary:");
            System.out.println("Relation      | Before | After | Pruned | Time (ms)");
            System.out.println("--------------|--------|-------|--------|----------");
            for (String rel : beforeSizes.keySet()) {
                int before = beforeSizes.get(rel);
                int after = afterSizes.get(rel);
                double pct = (before == 0) ? 0 : 100.0 * (before - after) / before;
                double time = times.get(rel);
                System.out.printf("%-14s| %6d | %5d | %5.1f%% | %8.2f\n",
                        rel, before, after, pct, time);
            }
        }
    }

    /**
     * Phase 1: Bottom-up semi-join reduction for chain query
     *
     * For chain [R1, R2, R3, ...]:
     * R(n-1) ⋉ R(n), then R(n-2) ⋉ R(n-1), etc.
     */
    public static ReducerMetrics reduceBottomUp(ChainQuery query,
                                               Map<String, Relation> relations) {

        System.out.println("\n=== PHASE 1: BOTTOM-UP SEMI-JOIN REDUCTION ===\n");

        ReducerMetrics metrics = new ReducerMetrics();
        List<String> chainOrder = query.getRelationNames();

        // Process from right to left
        for (int i = chainOrder.size() - 1; i > 0; i--) {

            String currentName = chainOrder.get(i - 1);
            String nextName = chainOrder.get(i);
            ChainQuery.JoinSpec joinSpec = query.getJoinSpec(i - 1);

            Relation currentRel = relations.get(currentName);
            Relation nextRel = relations.get(nextName);

            // current = current ⋉ next
            ExactSemiJoinOperator.SemiJoinResult result =
                    ExactSemiJoinOperator.semiJoin(
                            currentRel,
                            nextRel,
                            joinSpec.leftColumn,
                            joinSpec.rightColumn
                    );

            relations.put(currentName, result.relation);

            System.out.printf(
                    "[BU] %s ⋉ %s (on %s): %d -> %d tuples (%.1f%% pruned, %.2f ms)\n",
                    currentName,
                    nextName,
                    joinSpec,
                    result.beforeSize,
                    result.afterSize,
                    result.getPruningPercent(),
                    result.timeMs
            );

            metrics.record(currentName, result.beforeSize, result.afterSize, result.timeMs);
        }

        System.out.println();
        return metrics;
    }
}
