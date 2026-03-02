package algorithm;

import datamodel.*;
import query.*;
import java.util.*;

public class ChainQueryExecutor {

    /**
     * Execute Yannakakis algorithm on chain query (scoped baseline).
     * - Phase 1: bottom-up semi-join reduction
     * - Phase 3: final join
     */
    public static Relation executeChainQuery(ChainQuery query,
                                             Map<String, Relation> relationsInput) {

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║   YANNAKAKIS CHAIN QUERY EXECUTION     ║");
        System.out.println("╚════════════════════════════════════════╝");

        // Make copies (don't modify originals)
        Map<String, Relation> relations = new HashMap<>();
        for (Map.Entry<String, Relation> e : relationsInput.entrySet()) {
            relations.put(e.getKey(), e.getValue().copy());
        }

        // Print input
        System.out.println("\n[INPUT] Query: " + query);
        System.out.println("\n[INPUT] Relations:");
        for (String name : query.getRelationNames()) {
            Relation rel = relations.get(name);
            System.out.printf("  %s: %d tuples\n", name, rel.size());
        }

        // Phase 1: Bottom-up reduction
        YannakakisReducer.ReducerMetrics metrics =
                YannakakisReducer.reduceBottomUp(query, relations);

        // (Optional) print nice table
        metrics.print();

        // Phase 3: Final join
        System.out.println("\n=== PHASE 3: FINAL JOIN ===\n");

        Relation result = relations.get(query.getRelationNames().get(0));

        for (int i = 1; i < query.getRelationNames().size(); i++) {
            String nextRelName = query.getRelationNames().get(i);
            Relation nextRel = relations.get(nextRelName);

            int beforeJoinSize = result.size();
            result = naturalJoin(result, nextRel);

            System.out.printf("[FJ] After joining %s: %d -> %d tuples\n",
                    nextRelName, beforeJoinSize, result.size());
        }

        System.out.printf("\n[RESULT] Final: %d tuples\n", result.size());
        System.out.println();

        return result;
    }

    // Simple natural join (matches on all common column names)
    private static Relation naturalJoin(Relation R, Relation S) {

        Set<String> rCols = new HashSet<>(R.getColumns());
        Set<String> sCols = new HashSet<>(S.getColumns());

        Set<String> commonCols = new HashSet<>(rCols);
        commonCols.retainAll(sCols);

        Set<Tuple> resultTuples = new HashSet<>();

        for (Tuple rTuple : R.getTuples()) {
            for (Tuple sTuple : S.getTuples()) {
                if (matchOnCommon(rTuple, sTuple, commonCols)) {
                    resultTuples.add(mergeTuples(rTuple, sTuple, rCols, sCols));
                }
            }
        }

        Set<String> resultCols = new HashSet<>(rCols);
        resultCols.addAll(sCols);

        Relation result = new Relation(
                "joined_" + R.getName() + "_" + S.getName(),
                new ArrayList<>(resultCols)
        );
        result.setTuples(resultTuples);

        return result;
    }

    private static boolean matchOnCommon(Tuple t1, Tuple t2, Set<String> commonCols) {
        for (String col : commonCols) {
            if (!Objects.equals(t1.get(col), t2.get(col))) {
                return false;
            }
        }
        return true;
    }

    private static Tuple mergeTuples(Tuple t1, Tuple t2,
                                    Set<String> cols1, Set<String> cols2) {

        Map<String, Object> merged = new HashMap<>(t1.getValues());
        for (String col : cols2) {
            if (!cols1.contains(col)) {
                merged.put(col, t2.get(col));
            }
        }
        return new Tuple(merged);
    }
}

