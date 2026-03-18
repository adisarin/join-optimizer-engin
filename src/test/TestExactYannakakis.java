package test;

import algorithm.*;
import datamodel.*;
import java.util.*;
import query.*;

public class TestExactYannakakis {

    public static void main(String[] args) throws Exception {
        System.out.println("\n████████████████████████████████████████████\n");
        System.out.println("  ACYCLIC JOIN QUERY EVALUATION");
        System.out.println("  Exact, Bloom, and Predicate Transfer Tests\n");
        System.out.println("████████████████████████████████████████████\n");

        testTwoRelations();
        testThreeRelations();
        testLargePruning();
        testBloomVsExact();
        testBloomParameterSweep();
        testOneHopBloomChain();
        testPredicateTransferChain();
        testLargeScaleComparison();
        testScaledBloomComparison();
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

    public static void testOneHopBloomChain() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 6: One-Hop Bloom Filtering on Chain");
        System.out.println("=".repeat(50));

        Relation customer = createLargeCustomers(1000);
        Relation order = createLargeOrders(200, 200);   // only customers 1..200 have orders
        Relation payment = createLargePaymentsFromOrders(order, 50); // only 50 orders paid

        int mBits = 256;
        int kHashes = 3;

        // One-hop only: Payment -> Order
        BloomSemiJoinOperator.Result bloomOrder =
                BloomSemiJoinOperator.semiJoin(order, payment, "oid", "oid", mBits, kHashes);

        // Exact baselines for reference
        ExactSemiJoinOperator.SemiJoinResult exactOrder =
                ExactSemiJoinOperator.semiJoin(order, payment, "oid", "oid");
        ExactSemiJoinOperator.SemiJoinResult exactCustomer =
                ExactSemiJoinOperator.semiJoin(customer, exactOrder.relation, "cid", "cid");

        System.out.println("\n[ONE-HOP BLOOM]");
        System.out.printf("Order: %d -> %d (%.1f%% pruned)\n",
                bloomOrder.beforeSize, bloomOrder.afterSize, bloomOrder.getPruningPercent());

        System.out.println("\n[EXACT BASELINE]");
        System.out.printf("Exact Order after Payment filter: %d\n", exactOrder.afterSize);
        System.out.printf("Exact Customer after propagated exact filter: %d\n", exactCustomer.afterSize);

        System.out.println("\n[TAKEAWAY]");
        System.out.println("One-hop Bloom only filters the directly adjacent relation (Order),");
        System.out.println("but does not yet transfer pruning effect upstream to Customer.");
    }

    public static void testPredicateTransferChain() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 7: Multi-Hop Predicate Transfer on Chain");
        System.out.println("=".repeat(50));

        Relation customer = createLargeCustomers(1000);
        Relation order = createLargeOrders(200, 200);
        Relation payment = createLargePaymentsFromOrders(order, 50);

        int mBits = 256;
        int kHashes = 3;

        // Exact chain baseline:
        ExactSemiJoinOperator.SemiJoinResult exactOrder =
                ExactSemiJoinOperator.semiJoin(order, payment, "oid", "oid");
        ExactSemiJoinOperator.SemiJoinResult exactCustomer =
                ExactSemiJoinOperator.semiJoin(customer, exactOrder.relation, "cid", "cid");

        // Predicate transfer chain: Payment -> Order -> Customer
        PredicateTransferChainOperator.Result transfer =
                PredicateTransferChainOperator.transferThreeRelationChain(
                        customer, order, payment,
                        "cid",   // Customer.cid
                        "cid",   // Order.cid
                        "oid",   // Order.oid
                        "oid",   // Payment.oid
                        mBits, kHashes
                );

        System.out.println("\n[EXACT]");
        System.out.printf("Order after exact filter: %d\n", exactOrder.afterSize);
        System.out.printf("Customer after exact propagation: %d\n", exactCustomer.afterSize);

        System.out.println("\n[PREDICATE TRANSFER]");
        System.out.printf("Order: %d -> %d\n", transfer.middleBefore, transfer.middleFiltered.size());
        System.out.printf("Customer: %d -> %d\n", transfer.leftBefore, transfer.leftFiltered.size());
        System.out.printf("Transfer phase time: %.2f ms\n", transfer.timeMs);

        int orderExtra = transfer.middleFiltered.size() - exactOrder.afterSize;
        int customerExtra = transfer.leftFiltered.size() - exactCustomer.afterSize;

        System.out.println("\n[COMPARISON]");
        System.out.printf("Extra Order tuples kept vs exact: %d\n", orderExtra);
        System.out.printf("Extra Customer tuples kept vs exact: %d\n", customerExtra);

        if (transfer.middleFiltered.size() < exactOrder.afterSize ||
            transfer.leftFiltered.size() < exactCustomer.afterSize) {
            System.out.println("WARNING: predicate transfer produced fewer tuples than exact baseline.");
        } else {
            System.out.println("✓ Predicate transfer preserved exact matches while propagating filter upstream.");
        }
    }

    public static void testLargeScaleComparison() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 8: Large-Scale Comparison");
        System.out.println("=".repeat(50));

        int[] customerSizes = {1000, 10000, 50000};
        int mBits = 1024;
        int kHashes = 3;

        System.out.println("Customers | Orders | Payments | ExactOrderAfter | ExactCustAfter | OneHopOrderAfter | TransferOrderAfter | TransferCustAfter");

        for (int nCustomers : customerSizes) {
            int activeCustomers = Math.max(100, nCustomers / 10);   // 10% active
            int totalOrders = activeCustomers;
            int paidOrders = Math.max(20, totalOrders / 4);

            Relation customer = createLargeCustomers(nCustomers);
            Relation order = createLargeOrders(totalOrders, activeCustomers);
            Relation payment = createLargePaymentsFromOrders(order, paidOrders);

            ExactSemiJoinOperator.SemiJoinResult exactOrder =
                    ExactSemiJoinOperator.semiJoin(order, payment, "oid", "oid");
            ExactSemiJoinOperator.SemiJoinResult exactCustomer =
                    ExactSemiJoinOperator.semiJoin(customer, exactOrder.relation, "cid", "cid");

            BloomSemiJoinOperator.Result oneHop =
                    BloomSemiJoinOperator.semiJoin(order, payment, "oid", "oid", mBits, kHashes);

            PredicateTransferChainOperator.Result transfer =
                    PredicateTransferChainOperator.transferThreeRelationChain(
                            customer, order, payment,
                            "cid", "cid", "oid", "oid",
                            mBits, kHashes
                    );

            System.out.printf("%9d | %6d | %8d | %15d | %14d | %15d | %18d | %17d\n",
                    nCustomers,
                    order.size(),
                    payment.size(),
                    exactOrder.afterSize,
                    exactCustomer.afterSize,
                    oneHop.afterSize,
                    transfer.middleFiltered.size(),
                    transfer.leftFiltered.size());
        }
    }

    public static void testScaledBloomComparison() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("TEST 9: Scaled Bloom Filter Comparison");
        System.out.println("=".repeat(50));

        int nCustomers = 50000;
        int activeCustomers = 5000;
        int totalOrders = 5000;
        int paidOrders = 1250;
        int kHashes = 3;

        Relation customer = createLargeCustomers(nCustomers);
        Relation order = createLargeOrders(totalOrders, activeCustomers);
        Relation payment = createLargePaymentsFromOrders(order, paidOrders);

        ExactSemiJoinOperator.SemiJoinResult exactOrder =
                ExactSemiJoinOperator.semiJoin(order, payment, "oid", "oid");
        ExactSemiJoinOperator.SemiJoinResult exactCustomer =
                ExactSemiJoinOperator.semiJoin(customer, exactOrder.relation, "cid", "cid");

        int[] mValues = {1024, 4096, 16384, 65536};

        System.out.println("mBits | ExactOrd | TransferOrd | ExtraOrd | ExactCust | TransferCust | ExtraCust");

        for (int mBits : mValues) {
            PredicateTransferChainOperator.Result transfer =
                    PredicateTransferChainOperator.transferThreeRelationChain(
                            customer, order, payment,
                            "cid", "cid", "oid", "oid",
                            mBits, kHashes
                    );

            int extraOrder = transfer.middleFiltered.size() - exactOrder.afterSize;
            int extraCustomer = transfer.leftFiltered.size() - exactCustomer.afterSize;

            System.out.printf("%5d | %8d | %11d | %8d | %9d | %12d | %9d\n",
                    mBits,
                    exactOrder.afterSize,
                    transfer.middleFiltered.size(),
                    extraOrder,
                    exactCustomer.afterSize,
                    transfer.leftFiltered.size(),
                    extraCustomer);
        }

        System.out.println("\n[INTERPRETATION]");
        System.out.println("As mBits increases, false positives should decrease,");
        System.out.println("so TransferOrderAfter and TransferCustAfter should move closer to the exact baseline.");
    }

    private static Relation createLargeCustomers(int n) {
        Relation rel = new Relation("Customer", Arrays.asList("cid", "name"));
        for (int i = 1; i <= n; i++) {
            rel.addTuple(new Tuple(Map.of("cid", i, "name", "Customer_" + i)));
        }
        return rel;
    }

    private static Relation createLargeOrders(int totalOrders, int activeCustomers) {
        Relation rel = new Relation("Order", Arrays.asList("oid", "cid", "amount"));
        for (int i = 1; i <= totalOrders; i++) {
            int cid = ((i - 1) % activeCustomers) + 1;
            rel.addTuple(new Tuple(Map.of("oid", 100000 + i, "cid", cid, "amount", i * 10)));
        }
        return rel;
    }

    private static Relation createLargePaymentsFromOrders(Relation order, int paidOrderLimit) {
        Relation rel = new Relation("Payment", Arrays.asList("pid", "oid", "status"));

        List<Tuple> sortedOrders = new ArrayList<>(order.getTuples());
        sortedOrders.sort(Comparator.comparingInt(t -> (Integer) t.get("oid")));

        int count = 0;
        for (Tuple t : sortedOrders) {
            if (count >= paidOrderLimit) break;
            rel.addTuple(new Tuple(Map.of(
                    "pid", 200000 + count,
                    "oid", t.get("oid"),
                    "status", "Paid"
            )));
            count++;
        }
        return rel;
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

