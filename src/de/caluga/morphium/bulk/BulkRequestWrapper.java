package de.caluga.morphium.bulk;

import com.mongodb.client.model.*;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.query.Query;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 28.04.14
 * Time: 23:04
 * <p/>
 * TODO: Add documentation here
 */
public class BulkRequestWrapper {

    private Morphium morphium;
    private BulkOperationContext bc;
    private Query query;
    private WriteModel model = null;

    private MorphiumStorageListener.UpdateTypes updateType;

    public BulkRequestWrapper(Morphium m, BulkOperationContext bulk, Query q) {
        morphium = m;
        bc = bulk;
        query = q;
    }


//    public BulkRequestWrapper upsert() {
//
//        builder.upsert();
//        return this;
//    }

    public void insertMany(List obj) {
        for (Object o : obj) {
            insert(o);
        }
    }

    public void insert(Object t) {
        model = new InsertOneModel(morphium.getMapper().marshall(t));
        updateType = null;
    }

    public void removeOne() {
        updateType = null;
        model = new DeleteOneModel(query.toQueryObject());
    }

    public void replaceOne(Object obj, boolean upsert) {
        updateType = MorphiumStorageListener.UpdateTypes.SET;
        UpdateOptions opts = new UpdateOptions();
        opts.upsert(upsert);
        model = new ReplaceOneModel(query.toQueryObject(), morphium.getMapper().marshall(obj), opts);
    }

    public void remove() {
        updateType = null;
        model = new DeleteManyModel(query.toQueryObject());
    }

    private void writeOp(String operation, String field, Object value, boolean upsert, boolean multiple) {
        UpdateOptions opts = new UpdateOptions();
        opts.upsert(upsert);
        Bson b = null;
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class) || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
            b = new Document(operation, new Document(field, morphium.getMapper().marshall(value)));
//            builder.update(new BasicDBObject(operation, new BasicDBObject(field, morphium.getMapper().marshall(value))));
        } else if (Map.class.isAssignableFrom(value.getClass())) {

            Document map = new Document();
            Map valueMap = (Map) value;
            for (Object o : valueMap.keySet()) {
                map.put((String) o, morphium.getMapper().marshall(valueMap.get(o)));
            }
            b = new Document(operation, new Document(field, map));
//            builder.update(new BasicDBObject(operation, new BasicDBObject(field, map)));

        } else if (List.class.isAssignableFrom(value.getClass())) {
            //list handling
            List lst = new ArrayList();
            List valList = (List) value;
            for (Object o : valList) {
                lst.add(morphium.getMapper().marshall(o));
            }
            b = new Document(operation, new Document(field, valList));
//            builder.update(new BasicDBObject(operation, new BasicDBObject(field, lst)));
        } else {
            b = new Document(operation, new Document(field, value));
//            builder.update(new BasicDBObject(operation, new BasicDBObject(field, value)));
        }
        if (multiple) {
            model = new UpdateManyModel(query.toQueryObject(), b, opts);
        } else {
            model = new UpdateOneModel(query.toQueryObject(), b, opts);
        }
    }

    public void set(String field, Object val, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.SET;
        writeOp("$set", field, val, upsert, multiple);
    }

    public void unset(String field, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.UNSET;

        writeOp("$unset", field, 1, upsert, multiple);
    }

    public void inc(String field, boolean upsert, boolean multiple) {
        inc(field, 1, upsert, multiple);
    }

    public void inc(String field, int amount, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.INC;
        writeOp("$inc", field, amount, upsert, multiple);
    }

    public void mul(String field, int val, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MUL;

        writeOp("$mul", field, val, upsert, multiple);
    }

    public void min(String field, int val, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MIN;

        writeOp("$min", field, val, upsert, multiple);
    }

    public void max(String field, int val, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MAX;

        writeOp("$max", field, val, upsert, multiple);
    }


    public void rename(String fieldOld, String fieldNew) {
        updateType = MorphiumStorageListener.UpdateTypes.RENAME;

        writeOp("$rename", fieldOld, fieldNew, false, false);
    }

    public void dec(String field, boolean upsert, boolean multiple) {

        dec(field, -1, upsert, multiple);
    }

    public void dec(String field, int amount, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.DEC;
        writeOp("$inc", field, -amount, upsert, multiple);
    }

    public void pull(String field, boolean upsert, boolean multiple, Object... value) {
        updateType = MorphiumStorageListener.UpdateTypes.PULL;

        if (value.length == 1) {
            writeOp("$pull", field, value[0], upsert, multiple);

        } else {
            writeOp("$pullAll", field, Arrays.asList(value), upsert, multiple);
        }

    }

    public void push(String field, boolean upsert, boolean multiple, Object... value) {
        updateType = MorphiumStorageListener.UpdateTypes.PUSH;

        if (value.length == 1) {
            writeOp("push", field, value[0], upsert, multiple);

        } else {
            writeOp("$pushAll", field, Arrays.asList(value), upsert, multiple);

        }

    }

    public void pop(String field, boolean first, boolean upsert, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.POP;

        writeOp("$pop", field, first ? -1 : 1, upsert, multiple);
    }

    public void preExec() {
        if (updateType == null) {
            morphium.firePreRemoveEvent(query);

        } else {
            morphium.firePreUpdateEvent(query.getType(), updateType);
        }

    }

    public MorphiumStorageListener.UpdateTypes getUpdateType() {
        return updateType;
    }

    public Query getQuery() {
        return query;
    }

    public void postExec() {
        if (updateType == null) {
            morphium.firePostRemoveEvent(query);
        } else {
            morphium.firePostUpdateEvent(query.getType(), updateType);
        }
        morphium.getCache().clearCacheIfNecessary(query.getType());
    }
}
