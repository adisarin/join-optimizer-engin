package datamodel;

import java.util.*;

public class Relation {
    private final String name;
    private final List<String> columns;
    private Set<Tuple> tuples;

    public Relation(String name, List<String> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.tuples = new HashSet<>();
    }

    public void addTuple(Tuple t) {
        tuples.add(t);
    }

    public Set<Tuple> getTuples() {
        return new HashSet<>(tuples);
    }

    public void setTuples(Set<Tuple> newTuples) {
        this.tuples = new HashSet<>(newTuples);
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    public int size() {
        return tuples.size();
    }

    public Set<Tuple> projectColumns(Set<String> columns) {
        Set<Tuple> result = new HashSet<>();
        for (Tuple t : tuples) {
            result.add(t.project(columns));
        }
        return result;
    }

    public Relation copy() {
        Relation copy = new Relation(this.name, this.columns);
        copy.setTuples(this.getTuples());
        return copy;
    }

    @Override
    public String toString() {
        return String.format("%s(%s): %d tuples",
                name, String.join(",", columns), tuples.size());
    }
}

