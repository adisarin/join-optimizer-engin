package datamodel;

import java.util.*;

public class Tuple {
    private final Map<String, Object> values;
    
    public Tuple(Map<String, Object> values) {
        this.values = new HashMap<>(values);
    }
    
    public Object get(String column) {
        return values.get(column);
    }
    
    public void set(String column, Object value) {
        values.put(column, value);
    }
    
    public Map<String, Object> getValues() {
        return new HashMap<>(values);
    }
    
    public Set<String> getColumns() {
        return values.keySet();
    }
    
    public Tuple project(Set<String> columns) {
        Map<String, Object> projected = new HashMap<>();
        for (String col : columns) {
            if (values.containsKey(col)) {
                projected.put(col, values.get(col));
            }
        }
        return new Tuple(projected);
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Tuple)) return false;
        Tuple other = (Tuple) o;
        return this.values.equals(other.values);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
    
    @Override
    public String toString() {
        return values.toString();
    }
}

