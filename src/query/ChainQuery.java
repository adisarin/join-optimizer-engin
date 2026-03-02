package query;

import java.util.*;

public class ChainQuery {

    private final List<String> relationNames;
    private final List<JoinSpec> joinSpecs;

    public static class JoinSpec {

        public final String leftColumn;
        public final String rightColumn;

        public JoinSpec(String left, String right) {
            this.leftColumn = left;
            this.rightColumn = right;
        }

        @Override
        public String toString() {
            return leftColumn + " = " + rightColumn;
        }
    }

    public ChainQuery(List<String> relationNames,
                      List<JoinSpec> joinSpecs) {

        if (relationNames.size() != joinSpecs.size() + 1) {

            throw new IllegalArgumentException(
                "Need n relations and n-1 join specs"
            );
        }

        this.relationNames = new ArrayList<>(relationNames);
        this.joinSpecs = new ArrayList<>(joinSpecs);
    }

    public List<String> getRelationNames() {

        return new ArrayList<>(relationNames);
    }

    public JoinSpec getJoinSpec(int i) {

        return joinSpecs.get(i);
    }

    public int getChainLength() {

        return relationNames.size();
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < relationNames.size(); i++) {

            sb.append(relationNames.get(i));

            if (i < joinSpecs.size()) {

                sb.append(" -[")
                  .append(joinSpecs.get(i))
                  .append("]-> ");
            }
        }

        return sb.toString();
    }
}

