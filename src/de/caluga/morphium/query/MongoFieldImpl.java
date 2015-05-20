package de.caluga.morphium.query;

import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.MongoType;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Reference;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:18
 * <p/>
 * default implementation of a MongoField
 *
 * @see MongoField
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoFieldImpl<T> implements MongoField<T> {
    private Query<T> query;
    private ObjectMapper mapper;
    private String fldStr;

    private FilterExpression fe;


    public MongoFieldImpl() {
    }

    public MongoFieldImpl(Query<T> q, ObjectMapper map) {
        query = q;
        mapper = map;
    }

    @Override
    public String getFieldString() {
        return fldStr;
    }

    @Override
    public void setFieldString(String fldStr) {
        fe = new FilterExpression();
        fe.setField(fldStr);
        this.fldStr = fldStr;
    }

    @Override
    public ObjectMapper getMapper() {
        return mapper;
    }

    @Override
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Query<T> all(List<Object> val) {
        add("$all", val);
        return query;
    }

    @Override
    public Query<T> eq(Object val) {
        // checking for Ids in references...
        if (val != null) {
            Class<?> cls = val.getClass();
            Field field = mapper.getMorphium().getARHelper().getField(query.getType(), fldStr);
            if (mapper.getMorphium().getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class) || val instanceof ObjectId) {
                if (field.isAnnotationPresent(Reference.class)) {
                    Object id;
                    if (val instanceof ObjectId) {
                        id = val;
                    } else {
                        id = mapper.getMorphium().getARHelper().getId(val);
                    }
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        // list of references, this should be part of
                        //
                        // need to compare DBRefs

                        val = new MorphiumReference(id, mapper.getCollectionName(val.getClass()));
                    } else {
                        // Reference
                        // here - query value is an entity AND it is referenced by the query type
                        // => we need to compeare ID's

                        val = id;
                    }
                }

            }
            if (field != null) {
                if (val instanceof ObjectId && field.getType().equals(String.class)) {
                    val = val.toString();
                } else if (val instanceof String && field.getType().equals(ObjectId.class)) {
                    try {
                        val = new ObjectId((String) val);
                    } catch (Exception e) {
                    }
                }
            }
        }

        fe.setValue(val);
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        query.addChild(fe);
        return query;  // To change body of implemented methods use File | Settings | File Templates.
    }

    private void add(String op, Object value) {
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        FilterExpression child = new FilterExpression();
        child.setField(op);
        child.setValue(value);
        fe.addChild(child);

        query.addChild(fe);
    }

    private void add(List<FilterExpression> expressionList) {
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        fe.setChildren(expressionList);
        query.addChild(fe);
    }

    @Override
    public Query<T> ne(Object val) {
        add("$ne", val);
        return query;
    }

    @Override
    public Query<T> size(int val) {
        add("$size", val);
        return query;
    }

    @Override
    public Query<T> lt(Object val) {
        add("$lt", val);
        return query;
    }

    @Override
    public Query<T> lte(Object val) {
        add("$lte", val);
        return query;
    }

    @Override
    public Query<T> gt(Object val) {
        add("$gt", val);
        return query;
    }

    @Override
    public Query<T> gte(Object val) {
        add("$gte", val);
        return query;
    }

    @Override
    public Query<T> exists() {
        add("$exists", true);
        return query;
    }

    @Override
    public Query<T> notExists() {
        add("$exists", false);
        return query;
    }

    @Override
    public Query<T> mod(int base, int val) {
        BsonArray lst = new BsonArray();
        lst.add(new BsonInt32(base));
        lst.add(new BsonInt32(val));
        add("$mod", lst);
        return query;
    }

    @Override
    public Query<T> matches(Pattern p) {
        fe.setValue(p);
        fe.setField(mapper.getMorphium().getARHelper().getFieldName(query.getType(), fldStr));
        query.addChild(fe);
        return query;
    }

    @Override
    public Query<T> matches(String ptrn) {
        return matches(Pattern.compile(ptrn));
    }

    @Override
    public Query<T> type(MongoType t) {
        add("$type", t.getNumber());
        return query;
    }

    @Override
    public Query<T> in(Collection<?> vals) {
//        BsonArray lst = new BsonArray();
//        lst.addAll(vals);
//        add("$in", lst);
//        return query;
        throw new RuntimeException("not implemented yet!");
    }

    @Override
    public Query<T> nin(Collection<?> vals) {
//        BsonArray lst = new BsonArray();
//        lst.addAll(vals);
//        add("$nin", lst);
//        return query;
        throw new RuntimeException("not implemented yet!");

    }

    @Override
    public Query<T> near(double x, double y) {
        BsonArray lst = new BsonArray();
        lst.add(new BsonDouble(x));
        lst.add(new BsonDouble(y));
        add("$near", lst);
        return query;
    }


    @Override
    public Query<T> nearSphere(double x, double y) {
        BsonArray lst = new BsonArray();
        lst.add(new BsonDouble(x));
        lst.add(new BsonDouble(y));
        add("$nearSphere", lst);
        return query;
    }

    @Override
    /**
     * search for entries with geo coordinates wihtin the given rectancle - x,y upper left, x2,y2 lower right corner
     */
    public Query<T> box(double x, double y, double x2, double y2) {
        BsonArray lst = new BsonArray();
        BsonArray p1 = new BsonArray();
        p1.add(new BsonDouble(x));
        p1.add(new BsonDouble(y));
        BsonArray p2 = new BsonArray();
        p2.add(new BsonDouble(x2));
        p2.add(new BsonDouble(y2));

        lst.add(p1);
        lst.add(p2);

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$within");

        Document box = new Document();
        box.put("$box", lst);
        withinExpression.setValue(box);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> polygon(double... p) {
        if (p.length % 2 == 1) {
            throw new IllegalArgumentException("Need a list of coordinates: x,y, x1,y1, x2,y2....");
        }
        BsonArray lst = new BsonArray();
        for (int i = 0; i < p.length; i += 2) {
            BsonArray p1 = new BsonArray();
            p1.add(new BsonDouble(p[i]));
            p1.add(new BsonDouble(p[i + 1]));
            lst.add(p1);
        }

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$within");

        Document box = new Document();
        box.put("$polygon", lst);
        withinExpression.setValue(box);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> center(double x, double y, double r) {
        BsonArray lst = new BsonArray();
        BsonArray p1 = new BsonArray();
        p1.add(new BsonDouble(x));
        p1.add(new BsonDouble(y));

        lst.add(p1);
        lst.add(new BsonDouble(r));

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$within");

        Document cnt = new Document();
        cnt.put("$center", lst);
        withinExpression.setValue(cnt);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> centerSphere(double x, double y, double r) {
        BsonArray lst = new BsonArray();
        BsonArray p1 = new BsonArray();
        p1.add(new BsonDouble(x));
        p1.add(new BsonDouble(y));

        lst.add(p1);
        lst.add(new BsonDouble(r));

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression withinExpression = new FilterExpression();
        withinExpression.setField("$within");

        Document cnt = new Document();
        cnt.put("$centerSphere", lst);
        withinExpression.setValue(cnt);

        expressionList.add(withinExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> nearSphere(double x, double y, double maxDistance) {
        BsonArray location = new BsonArray();
        location.add(new BsonDouble(x));
        location.add(new BsonDouble(y));

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$nearSphere");
        nearExpression.setValue(location);
        expressionList.add(nearExpression);

        FilterExpression maxDistanceExpression = new FilterExpression();
        maxDistanceExpression.setField("$maxDistance");
        maxDistanceExpression.setValue(maxDistance);
        expressionList.add(maxDistanceExpression);

        add(expressionList);
        return query;
    }


    @Override
    public Query<T> near(double x, double y, double maxDistance) {
        BsonArray location = new BsonArray();
        location.add(new BsonDouble(x));
        location.add(new BsonDouble(y));

        List<FilterExpression> expressionList = new ArrayList<FilterExpression>();

        FilterExpression nearExpression = new FilterExpression();
        nearExpression.setField("$near");
        nearExpression.setValue(location);
        expressionList.add(nearExpression);

        FilterExpression maxDistanceExpression = new FilterExpression();
        maxDistanceExpression.setField("$maxDistance");
        maxDistanceExpression.setValue(maxDistance);
        expressionList.add(maxDistanceExpression);

        add(expressionList);
        return query;
    }

    @Override
    public Query<T> getQuery() {
        return query;
    }

    @Override
    public void setQuery(Query<T> q) {
        query = q;
    }

}
