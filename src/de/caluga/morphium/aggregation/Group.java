package de.caluga.morphium.aggregation;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 08:48
 * <p/>
 * Aggregator-Group
 */
@SuppressWarnings("UnusedDeclaration")
public class Group<T, R> {
    private Aggregator<T, R> aggregator;
    private Document id;

    private List<Document> operators = new ArrayList<Document>();

    public Group(Aggregator<T, R> ag, Map<String, Object> idSubObject) {
        aggregator = ag;
        id = new Document("_id", new Document(idSubObject));
    }

    public Group(Aggregator<T, R> ag, String id) {
        aggregator = ag;
        this.id = new Document("_id", id);
    }

    public Group(Aggregator<T, R> ag, Document id) {
        aggregator = ag;
        this.id = new Document("_id", new Document(id));
    }

    public Group<T, R> addToSet(Document param) {
        Document o = new Document("$addToSet", param);
        operators.add(o);
        return this;
    } //don't know what this actually should do???

    public Group<T, R> first(String name, Object p) {
        Document o = new Document(name, new Document("$first", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> last(String name, Object p) {
        Document o = new Document(name, new Document("$last", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> max(String name, Object p) {
        Document o = new Document(name, new Document("$max", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> min(String name, Object p) {
        Document o = new Document(name, new Document("$min", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> avg(String name, Object p) {
        Document o = new Document(name, new Document("$avg", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> push(String name, Object p) {
        Document o = new Document(name, new Document("$push", p));
        operators.add(o);
        return this;
    }


    public Group<T, R> sum(String name, int p) {
        return sum(name, Integer.valueOf(p));
    }

    public Group<T, R> sum(String name, long p) {
        return sum(name, Long.valueOf(p));
    }

    public Group<T, R> sum(String name, Object p) {
        Document o = new Document(name, new Document("$sum", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> sum(String name, String p) {
        return sum(name, (Object) p);
    }

    public Aggregator<T, R> end() {
        Document params = new Document();
        params.putAll(id);
        for (Document o : operators) {
            params.putAll(o);
        }
        Document obj = new Document("$group", params);
        aggregator.addOperator(obj);
        return aggregator;
    }

    public List<Document> getOperators() {
        return operators;
    }
}
