package de.caluga.morphium;


import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FilterExpression {
    private String field;
    private Object value;
    private List<FilterExpression> children;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public List<FilterExpression> getChildren() {
        return children;
    }

    public void setChildren(List<FilterExpression> children) {
        this.children = children;
    }

    public void addChild(FilterExpression e) {
        if (children == null) {
            children = new ArrayList<FilterExpression>();
        }
        children.add(e);
    }

    public Document dbObject() {
        Document o = new Document();
        if (children != null) {
            Document expression = new Document();
            for (FilterExpression flt : children) {
                expression.put(flt.getField(), flt.getValue());
            }
            o.put(field, expression);
        } else {
            if (value != null && value.getClass().isEnum()) {
                o.put(field, ((Enum) value).name());
            } else {
                o.put(field, value);
            }
        }
        return o;
    }

    @Override
    public String toString() {
        StringBuilder c = new StringBuilder();
        if (children != null && children.size() > 0) {
            c.append("[ ");
            for (FilterExpression fe : children) {
                c.append(fe.toString());
                c.append(", ");
            }
            c.deleteCharAt(c.length() - 1);
            c.deleteCharAt(c.length() - 1);
            c.append(" ]");
        }
        return "FilterExpression{" +
                "field='" + field + '\'' +
                ", value=" + value +
                ", children=" + c.toString() +
                '}';
    }
}