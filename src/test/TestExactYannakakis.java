package test;

import algorithm.*;
import datamodel.*;
import java.util.*;
import query.*;

public class TestExactYannakakis {

    public static void main(String[] args) throws Exception {
        System.out.println("\n████████████████████████████████████████████\n");
        System.out.println("  YANNAKAKIS EXACT SEMI-JOIN IMPLEMENTATION");
        System.out.println("  Chain Queries Only\n");
        System.out.println("████████████████████████████████████████████\n");

        testTwoRelations();
        testThreeRelations();
        testLargePruning();
        testBloomVsExact();
        testBloomParameterSweep();
    }

    public static void testTwoRelations() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 1: Two-Relation Chain (Dangling Detection)");
        System.out.println("=".repeat(50));

        Relation customer = createCustomers();
        Relation order = createOrders();

        System.out.println("\n[SETUP]");
        System.out.println("  " + customer);
        System.out.println("  " + order);
        System.out.println("  Join: Customer.cid = Order.cid");

        ChainQuery query = new ChainQuery(
                Arrays.asList("Customer", "Order"),
                Arrays.asList(new ChainQuery.JoinSpec("cid", "cid"))
        );

        Map<String, Relation> relations = new HashMap<>();
        relations.put("Customer", customer);
        relations.put("Order", order);

        Relation result = ChainQueryExecutor.executeChainQuery(query, relations);

        System.out.println("\n[VERIFICATION]");
        System.out.println("Expected: 3 tuples (2 orders for Alice, 1 for Bob)");
        System.out.println("Actual:   " + result.size() + " tuples");

        if (result.size() == 3) System.out.println("✓ TEST 1 PASSED");
        else System.out.println("✗ TEST 1 FAILED");
    }

    public static void testThreeRelations() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 2: Three-Relation Chain");
        System.out.println("=".repeat(50));

        Relation customer = createCustomers();
        Relation order = createOrders();
        Relation payment = createPayments();

        System.out.println("\n[SETUP]");
        System.out.println("  " + customer);
        System.out.println("  " + order);
        System.out.println("  " + payment);
        System.out.println("  Chain: Customer -> Order -> Payment");

        ChainQuery query = new ChainQuery(
                Arrays.asList("Customer", "Order", "Payment"),
                Arrays.asList(
                        new ChainQuery.JoinSpec("cid", "cid"),
                        new ChainQuery.JoinSpec("oid", "oid")
                )
        );

        Map<String, Relation> relations = new HashMap<>();
        relations.put("Customer", customer);
        relations.put("Order", order);
        relations.put("Payment", payment);

        Relation result = ChainQueryExecutor.executeChainQuery(query, relations);

        System.out.println("\n[VERIFICATION]");
        System.out.println("Expected: 3 tuples");
        System.out.println("Actual:   " + result.size() + " tuples");

        if (result.size() == 3) System.out.println("✓ TEST 2 PASSED");
        else System.out.println("✗ TEST 2 FAILED");
    }

    public static void testLargePruning() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 3: Large Pruning (80% of tuples removed)");
        System.out.println("=".repeat(50));

        Relation customer = new Relation("Customer", Arrays.asList("cid", "name"));
        for (int i = 1; i <= 100; i++) {
            customer.addTuple(new Tuple(Map.of("cid", i, "name", "Customer_" + i)));
        }

        Relation order = new Relation("Order", Arrays.asList("oid", "cid", "amount"));
        for (int i = 1; i <= 20; i++) {
            order.addTuple(new Tuple(Map.of("oid", 100 + i, "cid", i, "amount", i * 100)));
        }

        System.out.println("\n[SETUP]");
        System.out.println("  " + customer);
        System.out.println("  " + order);
        System.out.println("  Query: Customers with orders");

        ChainQuery query = new ChainQuery(
                Arrays.asList("Customer", "Order"),
                Arrays.asList(new ChainQuery.JoinSpec("cid", "cid"))
        );

        Map<String, Relation> relations = new HashMap<>();
        relations.put("Customer", customer.copy());
        relations.put("Order", order);

        Relation result = ChainQueryExecutor.executeChainQuery(query, relations);

        System.out.println("\n[VERIFICATION]");
        System.out.println("Expected: 20 tuples (only customers 1-20)");
        System.out.println("Actual:   " + result.size() + " tuples");
        System.out.println("Pruning:  80% of customers removed (100 → 20)");

        if (result.size() == 20) System.out.println("✓ TEST 3 PASSED");
        else System.out.println("✗ TEST 3 FAILED");
    }

    public static void testBloomVsExact() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 4: Approximate Semi-Join (Bloom) vs Exact");
        System.out.println("=".repeat(50));

        // Same data shape as Test 3
        Relation customer = new Relation("Customer", Arrays.asList("cid", "name"));
        for (int i = 1; i <= 100; i++) {
            customer.addTuple(new Tuple(Map.of("cid", i, "name", "Customer_" + i)));
        }

        Relation order = new Relation("Order", Arrays.asList("oid", "cid", "amount"));
        for (int i = 1; i <= 20; i++) {
            order.addTuple(new Tuple(Map.of("oid", 100 + i, "cid", i, "amount", i * 100)));
        }

        // Exact semi-join
        ExactSemiJoinOperator.SemiJoinResult exact =
                ExactSemiJoinOperator.semiJoin(customer, order, "cid", "cid");

        // Bloom semi-join (tune mBits/kHashes)
        int mBits = 256;
        int kHashes = 3;
        BloomSemiJoinOperator.Result bloom =
                BloomSemiJoinOperator.semiJoin(customer, order, "cid", "cid", mBits, kHashes);

        System.out.println("\n[EXACT]");
        System.out.printf("Customer: %d -> %d (%.1f%% pruned), time %.2f ms\n",
                exact.beforeSize, exact.afterSize, exact.getPruningPercent(), exact.timeMs);

        System.out.println("\n[BLOOM]");
        System.out.printf("Params: mBits=%d, k=%d\n", mBits, kHashes);
        System.out.printf("Customer: %d -> %d (%.1f%% pruned), time %.2f ms\n",
                bloom.beforeSize, bloom.afterSize, bloom.getPruningPercent(), bloom.timeMs);

        int extraKept = bloom.afterSize - exact.afterSize;
        System.out.println("\n[COMPARISON]");
        System.out.printf("Exact after-size: %d\n", exact.afterSize);
        System.out.printf("Bloom after-size: %d\n", bloom.afterSize);
        System.out.printf("Extra tuples kept due to false positives: %d\n", extraKept);

        // Sanity check: Bloom should not remove true matches (should not be < exact)
        if (bloom.afterSize < exact.afterSize) {
            System.out.println("WARNING: Bloom produced fewer tuples than exact (possible bug / false negatives)!");
        } else {
            System.out.println("✓ Bloom produced >= exact (no false negatives observed)");
        }
    }

    public static void testBloomParameterSweep() {

        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 5: Bloom Filter Parameter Sweep");
        System.out.println("=".repeat(50));

        Relation customer = new Relation("Customer", Arrays.asList("cid", "name"));

        for (int i = 1; i <= 100; i++) {
            customer.addTuple(new Tuple(Map.of("cid", i, "name", "Customer_" + i)));
        }

        Relation order = new Relation("Order", Arrays.asList("oid", "cid", "amount"));

        for (int i = 1; i <= 20; i++) {
            order.addTuple(new Tuple(Map.of("oid", 100 + i, "cid", i, "amount", i * 100)));
        }

        ExactSemiJoinOperator.SemiJoinResult exact =
                ExactSemiJoinOperator.semiJoin(customer, order, "cid", "cid");

        int[] mValues = {16, 32, 64, 128, 256, 512, 1024};

        int k = 3;

        System.out.println("\nResults Table:");
        System.out.println("mBits | ExactAfter | BloomAfter | ExtraKept | Prune%");

        for (int m : mValues) {

            BloomSemiJoinOperator.Result bloom =
                    BloomSemiJoinOperator.semiJoin(customer, order, "cid", "cid", m, k);

            int extra = bloom.afterSize - exact.afterSize;

            System.out.printf("%5d | %10d | %10d | %9d | %6.1f%%\n",
                    m,
                    exact.afterSize,
                    bloom.afterSize,
                    extra,
                    bloom.getPruningPercent());
        }
    }

    private static Relation createCustomers() {
        Relation rel = new Relation("Customer", Arrays.asList("cid", "name"));
        rel.addTuple(new Tuple(Map.of("cid", 1, "name", "Alice")));
        rel.addTuple(new Tuple(Map.of("cid", 2, "name", "Bob")));
        rel.addTuple(new Tuple(Map.of("cid", 3, "name", "Carol")));
        return rel;
    }

    private static Relation createOrders() {
        Relation rel = new Relation("Order", Arrays.asList("oid", "cid", "amount"));
        rel.addTuple(new Tuple(Map.of("oid", 101, "cid", 1, "amount", 100)));
        rel.addTuple(new Tuple(Map.of("oid", 102, "cid", 1, "amount", 200)));
        rel.addTuple(new Tuple(Map.of("oid", 103, "cid", 2, "amount", 150)));
        return rel;
    }

    private static Relation createPayments() {
        Relation rel = new Relation("Payment", Arrays.asList("pid", "oid", "status"));
        rel.addTuple(new Tuple(Map.of("pid", 1001, "oid", 101, "status", "Paid")));
        rel.addTuple(new Tuple(Map.of("pid", 1002, "oid", 102, "status", "Pending")));
        rel.addTuple(new Tuple(Map.of("pid", 1003, "oid", 103, "status", "Paid")));
        return rel;
    }
}

