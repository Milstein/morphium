package de.caluga.morphium.writer;

import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.BulkOperationContext;
import de.caluga.morphium.query.Query;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:38
 * <p/>
 * default writer implementation - uses a ThreadPoolExecutor for execution of asynchornous calls
 * maximum Threads are limited to 0.9* MaxConnections configured in MorphiumConfig
 *
 * @see MorphiumWriter
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class MorphiumWriterImpl implements MorphiumWriter {
    private static Logger logger = new Logger(MorphiumWriterImpl.class);
    private Morphium morphium;
    private int maximumRetries = 10;
    private int pause = 250;
    private ThreadPoolExecutor executor = null;

    @Override
    public void setMaximumQueingTries(int n) {
        maximumRetries = n;
    }

    @Override
    public void setPauseBetweenTries(int p) {
        pause = p;
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        if (m != null) {
            executor = new ThreadPoolExecutor(m.getConfig().getMaxConnections() / 2, (int) (m.getConfig().getMaxConnections() * m.getConfig().getBlockingThreadsMultiplier() * 0.9),
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }
    }

    /**
     * @param obj - object to store
     */
    @Override
    public <T> void store(final T obj, final String collection, AsyncOperationCallback<T> callback) {
        if (obj instanceof List) {
            store((List) obj, callback);
            return;
        }
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            public void run() {
                long start = System.currentTimeMillis();

                try {
//                    final T o = obj;
                    final Class type = morphium.getARHelper().getRealClass(obj.getClass());
                    if (!morphium.getARHelper().isAnnotationPresentInHierarchy(type, Entity.class)) {
                        throw new RuntimeException("Not an entity: " + type.getSimpleName() + " Storing not possible!");
                    }
                    morphium.inc(StatisticKeys.WRITES);
                    Object id = morphium.getARHelper().getId(obj);
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(type, PartialUpdate.class)) {
                        if ((obj instanceof PartiallyUpdateable)) {
                            updateUsingFields(obj, collection, callback, ((PartiallyUpdateable) obj).getAlteredFields().toArray(new String[((PartiallyUpdateable) obj).getAlteredFields().size()]));
                            ((PartiallyUpdateable) obj).clearAlteredFields();

                            return;
                        }
                    }
                    final T o = morphium.getARHelper().getRealObject(obj);
                    if (o == null) {
                        logger.warn("Illegal Reference? - cannot store Lazy-Loaded / Partial Update Proxy without delegate!");
                        return;
                    }
                    boolean isNew = id == null;
                    Object reread = null;
                    if (morphium.isAutoValuesEnabledForThread()) {
                        CreationTime creationTime = morphium.getARHelper().getAnnotationFromHierarchy(type, CreationTime.class);
                        if (id != null && (morphium.getConfig().isCheckForNew() || (creationTime != null && creationTime.checkForNew()))
                                && !morphium.getARHelper().getIdField(o).getType().equals(ObjectId.class)) {
                            //check if it exists
                            reread = morphium.findById(o.getClass(), id);
                            isNew = reread == null;
                        }
                        isNew = morphium.getARHelper().setAutoValues(morphium, o, type, id, isNew, reread);

                    }
                    morphium.firePreStoreEvent(o, isNew);

                    Document marshall = morphium.getMapper().marshall(o);

                    String c = collection;
                    if (c == null) {
                        c = morphium.getMapper().getCollectionName(type);
                    }
                    final String coll = c;


                    final BsonDocument cmd = new BsonDocument("collMod", new BsonString(coll));
                    final SingleResultCallback<Document> srcb = new SingleResultCallback<Document>() {

                        @Override
                        public void onResult(Document result, Throwable t) {
                            if (t == null) {
                                if (result.get("ok").equals(new BsonInt32(1))) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Collection " + coll + " already exists - index creation necessary");
                                    }
                                } else {
                                    if (logger.isDebugEnabled())
                                        logger.debug("Collection " + coll + " does not exist - ensuring indices");
                                    createCappedColl(o.getClass());
                                    morphium.ensureIndicesFor(type, coll, callback);
                                }
                            } else {
                                logger.error("Error checking for collection " + coll, t);
                            }
                        }
                    };

                    WriteConcern wc = morphium.getWriteConcernForClass(type);
                    BulkWriteOptions opts = new BulkWriteOptions();
                    opts.ordered(false);
                    ArrayList<InsertOneModel<Document>> lst = new ArrayList<>();
                    InsertOneModel<Document> m = new InsertOneModel<>(marshall);
                    lst.add(m);
                    MongoCollection<Document> co = morphium.getDatabase().getCollection(coll);

                    if (wc != null) {
                        co = co.withWriteConcern(wc);
                    }
                    co.withWriteConcern(wc).bulkWrite(lst, opts, new SingleResultCallback<BulkWriteResult>() {
                        @Override
                        public void onResult(BulkWriteResult result, Throwable t) {

                        }
                    });
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), marshall, dur, true, WriteAccessType.SINGLE_INSERT);
//                    if (logger.isDebugEnabled()) {
//                        String n = "";
//                        if (isNew) {
//                            n = "NEW ";
//                        }
//                        logger.debug(n + "stored " + type.getSimpleName() + " after " + dur + " ms length:" + marshall.toString().length());
//                    }
                    if (isNew) {
                        List<String> flds = morphium.getARHelper().getFields(o.getClass(), Id.class);
                        if (flds == null) {
                            throw new RuntimeException("Object does not have an ID field!");
                        }
                        try {
                            //Setting new ID (if object was new created) to Entity

                            Field fld = morphium.getARHelper().getField(o.getClass(), flds.get(0));
                            if (fld.getType().equals(marshall.get("_id").getClass())) {
                                fld.set(o, marshall.get("_id"));
                            } else {
                                logger.warn("got default generated key, but ID-Field is not of type ObjectID... trying string conversion");
                                if (fld.getType().equals(String.class)) {
                                    fld.set(o, ((ObjectId) marshall.get("_id")).toString());
                                } else {
                                    throw new IllegalArgumentException("cannot convert ID for given object - id type is: " + fld.getType().getName() + "! Please set ID before write");
                                }
                            }
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    morphium.getCache().clearCacheIfNecessary(o.getClass());
                    morphium.firePostStoreEvent(o, isNew);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, (Class<T>) o.getClass(), coll, null, obj);
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
                            //using reflection to get fields etc... in order to remove strong dependency
                            try {
                                Method m = e.getClass().getMethod("getConstraintViolations");

                                Set violations = (Set) m.invoke(e);
                                for (Object v : violations) {
                                    m = v.getClass().getMethod("getMessage");
                                    String msg = (String) m.invoke(v);
                                    m = v.getClass().getMethod("getRootBean");
                                    Object bean = m.invoke(v);
                                    String s = morphium.toJsonString(bean);
                                    String type = bean.getClass().getName();
                                    m = v.getClass().getMethod("getInvalidValue");
                                    Object invalidValue = m.invoke(v);
                                    m = v.getClass().getMethod("getPropertyPath");
                                    Iterable<?> pth = (Iterable) m.invoke(v);
                                    String path = "";
                                    for (Object p : pth) {
                                        m = p.getClass().getMethod("getName");
                                        String name = (String) m.invoke(p);
                                        path = path + "." + name;
                                    }
                                    logger.error("Validation of " + type + " failed: " + msg + " - Invalid Value: " + invalidValue + " for path: " + path + "\n Tried to store: " + s);
                                }
                            } catch (Exception e1) {
                                logger.fatal("Could not get more information about validation error ", e1);
                            }
                        }
                    }
                    if (callback == null) {
                        if (e instanceof RuntimeException) {
                            throw ((RuntimeException) e);
                        }
                        throw new RuntimeException(e);
                    }
                    callback.onOperationError(AsyncOperationType.WRITE, null, System.currentTimeMillis() - start, (Class<T>) obj.getClass(), collection, e.getMessage(), e, obj);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void store(final List<T> lst, String collectionName, final AsyncOperationCallback<T> callback) {
        if (lst == null || lst.size() == 0) return;
        ArrayList<Document> dbLst = new ArrayList<Document>();
        WriteConcern wc = morphium.getWriteConcernForClass(lst.get(0).getClass());
        BulkOperationContext bctx = new BulkOperationContext(morphium, false);
        bctx.insertList(lst);
        bctx.execute(callback);
//
//        BulkWriteOperation bulkWriteOperation = collection.initializeUnorderedBulkOperation();
//        HashMap<Object, Boolean> isNew = new HashMap<Object, Boolean>();
//        if (!morphium.getDatabase().collectionExists(collectionName)) {
//            logger.warn("collection does not exist while storing list -  taking first element of list to ensure indices");
//            morphium.ensureIndicesFor((Class<T>) lst.get(0).getClass(), collectionName, callback);
//        }
//        long start = System.currentTimeMillis();
//        int cnt = 0;
//        List<Class<?>> types = new ArrayList<Class<?>>();
//        for (Object record : lst) {
//            Document marshall = morphium.getMapper().marshall(record);
//            Object id = morphium.getARHelper().getId(record);
//            boolean isn = id == null;
//            Object reread = null;
//
//            isNew.put(record, isn);
//
//            if (!types.contains(record.getClass())) types.add(record.getClass());
//            if (isNew.get(record)) {
//                dbLst.add(marshall);
//            } else {
//                //single update
//                WriteResult result = null;
//                cnt++;
//                BulkUpdateRequestBuilder up = bulkWriteOperation.find(new Document("_id", morphium.getARHelper().getId(record))).upsert();
//                up.updateOne(new Document("$set", marshall));
//            }
//        }
//        morphium.firePreStoreEvent(isNew);
//        if (cnt > 0) {
//            for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
//                try {
//                    //storing updates
//                    if (wc == null) {
//                        bulkWriteOperation.execute();
//                    } else {
//                        bulkWriteOperation.execute(wc);
//                    }
//                } catch (Exception e) {
//                    morphium.handleNetworkError(i, e);
//                }
//            }
//            for (Class<?> c : types) {
//                morphium.getCache().clearCacheIfNecessary(c);
//            }
//        }


    }

    @Override
    public void flush() {
        //nothing to do
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> callback) {

    }


    private void createCappedColl(Class c) {
        if (logger.isDebugEnabled())
            logger.debug("Collection does not exist - ensuring indices / capped status");
        final Document cmd = new Document();
        cmd.put("create", morphium.getMapper().getCollectionName(c));
        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
        if (capped != null) {
            cmd.put("capped", true);
            cmd.put("size", capped.maxSize());
            cmd.put("max", capped.maxEntries());
        } else {
//            logger.warn("cannot cap collection for class " + c.getName() + " not @Capped");
            return;
        }
        cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(ObjectId.class)));
        morphium.getDatabase().runCommand(cmd, new SingleResultCallback<Document>() {
            @Override
            public void onResult(Document result, Throwable t) {
                cmd.notifyAll();
            }
        });
        try {
            cmd.wait(morphium.getConfig().getAsyncOperationTimeout());
        } catch (InterruptedException e) {
            logger.error(e);
        }

    }


    public <T> void convertToCapped(final Class<T> c, final AsyncOperationCallback<T> callback) {
        convertToCapped(c, null, callback);
    }

    public <T> void convertToCapped(final Class<T> c, final String collectionName, final AsyncOperationCallback<T> callback) {
        WriteConcern wc = morphium.getWriteConcernForClass(c);

        final String coll = collectionName != null ? collectionName : morphium.getMapper().getCollectionName(c);

        MongoCollection<Document> collection = morphium.getDatabase().getCollection(coll);
        final BsonDocument cmd = new BsonDocument("collMod", new BsonString(morphium.getMapper().getCollectionName(c)));
        final SingleResultCallback<Document> srcb = new SingleResultCallback<Document>() {
            final long start = System.currentTimeMillis();
            @Override
            public void onResult(Document result, Throwable t) {
                try {
                    if (result.get("ok").equals(new BsonInt32(1))) {
                        if (logger.isDebugEnabled())
                            logger.debug("Collection does not exist - ensuring indices / capped status");
                        Document cmd = new Document();
                        cmd.put("create", coll);
                        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                        if (capped != null) {
                            cmd.put("capped", true);
                            cmd.put("size", capped.maxSize());
                            cmd.put("max", capped.maxEntries());
                        }
                        cmd.put("autoIndexId", (morphium.getARHelper().getIdField(c).getType().equals(ObjectId.class)));
                        morphium.getDatabase().runCommand(cmd, new SingleResultCallback<Document>() {
                            @Override
                            public void onResult(Document result, Throwable t) {
                                if (t != null) {
                                    callback.onOperationError(AsyncOperationType.CONVERT_TO_CAPPED, null, System.currentTimeMillis() - start, c, coll, t.getMessage(), t, null, result);
                                } else {
                                    callback.onOperationSucceeded(AsyncOperationType.CONVERT_TO_CAPPED, null, System.currentTimeMillis() - start, c, coll, null, null, result);
                                }
                            }
                        });
                    } else {
                        Capped capped = morphium.getARHelper().getAnnotationFromHierarchy(c, Capped.class);
                        if (capped != null) {
                            Document cmd = new Document();
                            cmd.put("convertToCapped", coll);
                            cmd.put("size", capped.maxSize());
                            cmd.put("max", capped.maxEntries());
                            morphium.getDatabase().runCommand(cmd, new SingleResultCallback<Document>() {
                                @Override
                                public void onResult(Document result, Throwable t) {
                                    if (t != null) {
                                        callback.onOperationError(AsyncOperationType.CONVERT_TO_CAPPED, null, System.currentTimeMillis() - start, c, coll, t.getMessage(), t, null, result);
                                    } else {
                                        callback.onOperationSucceeded(AsyncOperationType.CONVERT_TO_CAPPED, null, System.currentTimeMillis() - start, c, coll, null, null, result);
                                    }
                                }
                            });
                            //Indexes are not available after converting - recreate them
                            morphium.ensureIndicesFor(c, callback);
                        }
                    }
                } finally {

                }
            }
        };


    }


    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object v, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask<T> r = new WriterTask<T>() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toSet.getClass();
                Object value = v;
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                value = marshallIfNecessary(value);
                String coll = collection;
                if (coll == null) {
                    morphium.getMapper().getCollectionName(cls);
                }
                Document query = new Document();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Document update = new Document("$set", new Document(fieldName, value));
                List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                if (lastChangeFields != null && lastChangeFields.size() != 0) {
                    for (String fL : lastChangeFields) {
                        Field fld = morphium.getARHelper().getField(cls, fL);
                        if (fld.getType().equals(Date.class)) {
                            ((Document) update.get("$set")).put(fL, new Date());
                        } else {
                            ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }

                List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
                if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                    long cnt = morphium.getDatabase().getCollection(coll).count(query);
                    if (cnt == 0) {
                        //not found, would insert
                        for (String fL : creationTimeFields) {
                            Field fld = morphium.getARHelper().getField(cls, fL);
                            if (fld.getType().equals(Date.class)) {
                                ((Document) update.get("$set")).put(fL, new Date());
                            } else {
                                ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                            }
                        }
                    }
                }


                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();


                try {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }

                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                        }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    try {
                        f.set(toSet, value);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, null, System.currentTimeMillis() - start, null, toSet, field, v);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field, v);
                }
                morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    public <T> void submitAndBlockIfNecessary(AsyncOperationCallback<T> callback, WriterTask<T> r) {
        if (callback == null) {
            r.run();
        } else {
            r.setCallback(callback);
            int tries = 0;
            boolean retry = true;
            while (retry) {
                try {
                    tries++;
                    executor.submit(r);
                    retry = false;
                } catch (OutOfMemoryError om) {
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    if (tries > maximumRetries) {
                        throw new RuntimeException("Could not write - not even after " + maximumRetries + " and pause of " + pause + "ms", e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.warn("thread pool exceeded - waiting");
                    }
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException e1) {

                    }

                }
            }
        }
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> callback, final String... fields) {
        if (ent == null) return;
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(ent);
                if (id == null) {
                    //new object - update not working
                    logger.warn("trying to partially update new object - storing it in full!");
                    store(ent, collection, callback);
                    return;
                }

                morphium.firePreStoreEvent(ent, false);
                morphium.inc(StatisticKeys.WRITES);

                Document find = new Document();

                find.put("_id", id);
                Document update = new Document();
                for (String f : fields) {
                    try {
                        Object value = morphium.getARHelper().getValue(ent, f);
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)) {
                            value = morphium.getMapper().marshall(value);
                        }
                        update.put(f, value);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                Class<?> type = morphium.getARHelper().getRealClass(ent.getClass());

                LastChange t = morphium.getARHelper().getAnnotationFromHierarchy(type, LastChange.class); //(StoreLastChange) type.getAnnotation(StoreLastChange.class);
                if (t != null) {
                    List<String> lst = morphium.getARHelper().getFields(ent.getClass(), LastChange.class);

                    long now = System.currentTimeMillis();
                    for (String ctf : lst) {
                        Field f = morphium.getARHelper().getField(type, ctf);
                        if (f != null) {
                            try {
                                f.set(ent, now);
                            } catch (IllegalAccessException e) {
                                logger.error("Could not set modification time", e);

                            }
                        }
                        update.put(ctf, now);
                    }
                }


                update = new Document("$set", update);
                WriteConcern wc = morphium.getWriteConcernForClass(type);
                long start = System.currentTimeMillis();
                try {
                    String collectionName = morphium.getMapper().getCollectionName(ent.getClass());
                    if (collectionName == null) {
                        collectionName = collection;
                    }

                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(collectionName)) {
                                morphium.ensureIndicesFor((Class<T>) ent.getClass(), collectionName, callback);
                            }
                            if (wc != null) {
                                morphium.getDatabase().getCollection(collectionName).update(find, update, false, false, wc);
                            } else {
                                morphium.getDatabase().getCollection(collectionName).update(find, update, false, false);
                            }
                            break;
                        } catch (Throwable th) {
                            morphium.handleNetworkError(i, th);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(ent.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(morphium.getARHelper().getRealClass(ent.getClass()));
                    morphium.firePostStoreEvent(ent, false);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, null, ent, fields);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UPDATE, null, System.currentTimeMillis() - start, e.getMessage(), e, ent, fields);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                HashMap<Class<T>, List<Query<T>>> sortedMap = new HashMap<Class<T>, List<Query<T>>>();

                for (T o : lst) {
                    if (sortedMap.get(o.getClass()) == null) {
                        List<Query<T>> queries = new ArrayList<Query<T>>();
                        sortedMap.put((Class<T>) o.getClass(), queries);
                    }
                    Query<T> q = (Query<T>) morphium.createQueryFor(o.getClass());
                    q.f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o));
                    sortedMap.get(o.getClass()).add(q);
                }
                morphium.firePreRemove(lst);
                long start = System.currentTimeMillis();
                try {
                    for (Class<T> cls : sortedMap.keySet()) {
                        Query<T> orQuery = morphium.createQueryFor(cls);
                        orQuery = orQuery.or(sortedMap.get(cls));
                        remove(orQuery, (AsyncOperationCallback<T>) null); //sync call
                    }
                    morphium.firePostRemove(lst);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, null, lst);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, null, lst);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * deletes all objects matching the given query
     *
     * @param q - query
     */
    @Override
    public <T> void remove(final Query<T> q, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                morphium.firePreRemoveEvent(q);
                WriteConcern wc = morphium.getWriteConcernForClass(q.getType());
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc == null) {
                                morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject());
                            } else {
                                morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).remove(q.toQueryObject(), wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(q.getType(), q.toQueryObject(), dur, false, WriteAccessType.BULK_DELETE);
                    morphium.getCache().clearCacheIfNecessary(q.getType());
                    morphium.firePostRemoveEvent(q);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, null, null);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, q, System.currentTimeMillis() - start, e.getMessage(), e, null);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);


    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> callback) {

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Object id = morphium.getARHelper().getId(o);
                morphium.firePreRemove(o);
                Document db = new Document();
                db.append("_id", id);
                WriteConcern wc = morphium.getWriteConcernForClass(o.getClass());

                long start = System.currentTimeMillis();
                try {
                    String collectionName = collection;
                    if (collectionName == null) {
                        morphium.getMapper().getCollectionName(o.getClass());
                    }
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc == null) {
                                morphium.getDatabase().getCollection(collectionName).remove(db);
                            } else {
                                morphium.getDatabase().getCollection(collectionName).remove(db, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(o.getClass(), o, dur, false, WriteAccessType.SINGLE_DELETE);
                    morphium.clearCachefor(o.getClass());
                    morphium.inc(StatisticKeys.WRITES);
                    morphium.firePostRemoveEvent(o);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, null, o);
                } catch (Exception e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.REMOVE, null, System.currentTimeMillis() - start, e.getMessage(), e, o);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);

    }

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    @Override
    public <T> void inc(final T toInc, final String collection, final String field, final double amount, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toInc.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                Document query = new Document();
                query.put("_id", morphium.getId(toInc));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Document update = new Document("$inc", new Document(fieldName, amount));
                WriteConcern wc = morphium.getWriteConcernForClass(toInc.getClass());

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }

                    morphium.getCache().clearCacheIfNecessary(cls);

                    if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                        try {
                            f.set(toInc, ((Integer) f.get(toInc)) + (int) amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                        try {
                            f.set(toInc, ((Double) f.get(toInc)) + amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                        try {
                            f.set(toInc, ((Float) f.get(toInc)) + (float) amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                        try {
                            f.set(toInc, ((Long) f.get(toInc)) + (long) amount);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        logger.error("Could not set increased value - unsupported type " + cls.getName());
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    morphium.fireProfilingWriteEvent(toInc.getClass(), toInc, System.currentTimeMillis() - start, false, WriteAccessType.SINGLE_UPDATE);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, null, System.currentTimeMillis() - start, null, toInc, field, amount);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, null, System.currentTimeMillis() - start, e.getMessage(), e, toInc, field, amount);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(final Query<T> query, final Map<String, Double> fieldsToInc, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<? extends T> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();

                Document update = new Document();
                update.put("$inc", new Document(fieldsToInc));
                Document qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            WriteConcern wc = morphium.getWriteConcernForClass(cls);
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, fieldsToInc);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fieldsToInc);

                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void inc(final Query<T> query, final String field, final double amount, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                String coll = query.getCollectionName();
                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                Document update = new Document("$inc", new Document(fieldName, amount));
                Document qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }

                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            WriteConcern wc = morphium.getWriteConcernForClass(cls);
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.INC);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.INC, query, System.currentTimeMillis() - start, null, null, field, amount);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.INC, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, amount);

                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query            - query to specify which objects should be set
     * @param values           - map fieldName->Value, which values are to be set!
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple         - update several documents, if false, only first hit will be updated
     */
    @Override
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask $set = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                Document toSet = new Document();
                for (Map.Entry<String, Object> ef : values.entrySet()) {
                    String fieldName = morphium.getARHelper().getFieldName(cls, ef.getKey());
                    toSet.put(fieldName, marshallIfNecessary(ef.getValue()));
                }
                Document update = new Document("$set", toSet);
                Document qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                    List<String> creationTimeFlds = morphium.getARHelper().getFields(cls, CreationTime.class);
                    if (creationTimeFlds != null && creationTimeFlds.size() != 0 && morphium.getDatabase().getCollection(coll).find(qobj).count() == 0) {
                        if (creationTimeFlds != null && creationTimeFlds.size() != 0) {
                            for (String fL : creationTimeFlds) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((Document) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                        }
                    }
                }


                List<String> latChangeFlds = morphium.getARHelper().getFields(cls, LastChange.class);
                if (latChangeFlds != null && latChangeFlds.size() != 0) {
                    for (String fL : latChangeFlds) {
                        Field fld = morphium.getARHelper().getField(cls, fL);
                        if (fld.getType().equals(Date.class)) {
                            ((Document) update.get("$set")).put(fL, new Date());
                        } else {
                            ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }


                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, values, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, values, insertIfNotExist, multiple);
                }
            }
        };
        WriterTask r = $set;
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final Enum... fields) {
        ArrayList<String> flds = new ArrayList<String>();
        for (Enum e : fields) {
            flds.add(e.name());
        }
        unset(query, callback, multiple, flds.toArray(new String[fields.length]));
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> callback, final boolean multiple, final String... fields) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                String coll = query.getCollectionName();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                Document qobj = query.toQueryObject();

                Map<String, String> toSet = new HashMap<String, String>();
                for (String f : fields) {
                    toSet.put(f, ""); //value is ignored
                }
                Document update = new Document("$unset", toSet);
                WriteConcern wc = morphium.getWriteConcernForClass(cls);
                long start = System.currentTimeMillis();
                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, false, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, false, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, false, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.SET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.SET, query, System.currentTimeMillis() - start, null, null, fields, false, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.SET, query, System.currentTimeMillis() - start, e.getMessage(), e, null, fields, false, multiple);
                }
            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    @Override
    public <T> void unset(final T toSet, final String collection, final String field, AsyncOperationCallback<T> callback) {
        if (toSet == null) throw new RuntimeException("Cannot update null!");
        if (morphium.getARHelper().getId(toSet) == null) {
            logger.info("just storing object as it is new...");
            store(toSet, collection, callback);
        }

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = toSet.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                Document query = new Document();
                query.put("_id", morphium.getId(toSet));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Document update = new Document("$unset", new Document(fieldName, 1));
                WriteConcern wc = morphium.getWriteConcernForClass(toSet.getClass());

                long start = System.currentTimeMillis();

                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }

                        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                        if (lastChangeFields != null && lastChangeFields.size() != 0) {
                            update = new Document("$set", new Document());
                            for (String fL : lastChangeFields) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((Document) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }

                        }

                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(toSet.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);

                    try {
                        f.set(toSet, null);
                    } catch (IllegalAccessException e) {
                        //May happen, if null is not allowed for example
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, null, toSet, field);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, e.getMessage(), e, toSet, field);
                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> callback) {
        if (obj == null) throw new RuntimeException("Cannot update null!");
        if (morphium.getARHelper().getId(obj) == null) {
            logger.info("just storing object as it is new...");
            store(obj, collection, callback);
        }

        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class cls = obj.getClass();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);

                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                Document query = new Document();
                query.put("_id", morphium.getId(obj));
                Field f = morphium.getARHelper().getField(cls, field);
                if (f == null) {
                    throw new RuntimeException("Unknown field: " + field);
                }
                String fieldName = morphium.getARHelper().getFieldName(cls, field);

                Document update = new Document("$pop", new Document(fieldName, first ? -1 : 1));
                WriteConcern wc = morphium.getWriteConcernForClass(obj.getClass());

                long start = System.currentTimeMillis();

                try {
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (!morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor(cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }

                        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                        if (lastChangeFields != null && lastChangeFields.size() != 0) {
                            update = new Document("$set", new Document());
                            for (String fL : lastChangeFields) {
                                Field fld = morphium.getARHelper().getField(cls, fL);
                                if (fld.getType().equals(Date.class)) {
                                    ((Document) update.get("$set")).put(fL, new Date());
                                } else {
                                    ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                                }
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(query, update);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(query, update, false, false, wc);
                            }

                        }

                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(obj.getClass(), update, dur, false, WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);

                    try {
                        f.set(obj, null);
                    } catch (IllegalAccessException e) {
                        //May happen, if null is not allowed for example
                    }
                    morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), MorphiumStorageListener.UpdateTypes.UNSET);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, null, obj, field);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.UNSET, null, System.currentTimeMillis() - start, e.getMessage(), e, obj, field);
                }

            }
        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void unset(Query<T> query, String field, boolean multiple, AsyncOperationCallback<T> callback) {
        unset(query, callback, multiple, field);
    }

    @Override
    public <T> void pushPull(final boolean push, final Query<T> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                Class<?> cls = query.getType();
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

                String coll = morphium.getMapper().getCollectionName(cls);

                Document qobj = query.toQueryObject();
                if (insertIfNotExist) {
                    qobj = morphium.simplifyQueryObject(qobj);
                }
                Object v = marshallIfNecessary(value);

                String fieldName = morphium.getARHelper().getFieldName(cls, field);
                Document set = new Document(fieldName, v);
                Document update = new Document(push ? "$push" : "$pull", set);

                long start = System.currentTimeMillis();

                try {
                    pushIt(push, insertIfNotExist, multiple, cls, coll, qobj, update);
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null)
                        callback.onOperationSucceeded(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, null, null, field, value, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(AsyncOperationType.PUSH, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, insertIfNotExist, multiple);
                }
            }
        };

        submitAndBlockIfNecessary(callback, r);
    }

    private Object marshallIfNecessary(Object value) {
        if (value != null) {
            if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class)
                    || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                //need to marshall...
                Document marshall = morphium.getMapper().marshall(value);
                marshall.put("class_name", morphium.getARHelper().getRealClass(value.getClass()).getName());
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();
                for (Object o : (List) value) {
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Embedded.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class)
                            ) {
                        Document marshall = morphium.getMapper().marshall(o);
                        marshall.put("class_name", morphium.getARHelper().getRealClass(o.getClass()).getName());

                        lst.add(marshall);
                    } else {
                        lst.add(o);
                    }
                }
                value = lst;
            } else if (Map.class.isAssignableFrom(value.getClass())) {
                for (Object e : ((Map) value).entrySet()) {
                    Map.Entry en = (Map.Entry) e;
                    if (!String.class.isAssignableFrom(((Map.Entry) e).getKey().getClass())) {
                        throw new IllegalArgumentException("Can't push maps with Key not of type String!");
                    }
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Entity.class) ||
                            morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Embedded.class)
                            ) {
                        Document marshall = morphium.getMapper().marshall(en.getValue());
                        marshall.put("class_name", morphium.getARHelper().getRealClass(en.getValue().getClass()).getName());
                        ((Map) value).put(en.getKey(), marshall);
                    }
                }
            }
        }
        return value;
    }

    private void pushIt(boolean push, boolean insertIfNotExist, boolean multiple, Class<?> cls, String coll, Document qobj, Document update) {
        morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);

        List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
        if (lastChangeFields != null && lastChangeFields.size() != 0) {
            update.put("$set", new Document());
            for (String fL : lastChangeFields) {
                Field fld = morphium.getARHelper().getField(cls, fL);

                if (fld.getType().equals(Date.class)) {

                    ((Document) update.get("$set")).put(fL, new Date());
                } else {
                    ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                }
            }
        }
        if (insertIfNotExist) {
            List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
            if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                long cnt = morphium.getDatabase().getCollection(coll).count(qobj);
                if (cnt == 0) {
                    //not found, would insert
                    if (update.get("$set") == null) {
                        update.put("$set", new Document());
                    }
                    for (String fL : creationTimeFields) {
                        Field fld = morphium.getARHelper().getField(cls, fL);

                        if (fld.getType().equals(Date.class)) {
                            ((Document) update.get("$set")).put(fL, new Date());
                        } else {
                            ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                        }
                    }
                }
            }
        }

        WriteConcern wc = morphium.getWriteConcernForClass(cls);
        long start = System.currentTimeMillis();
        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                if (!morphium.getDatabase().collectionExists(coll) && insertIfNotExist) {
                    morphium.ensureIndicesFor(cls, coll);
                }
                if (wc == null) {
                    morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                } else {
                    morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                }
                break;
            } catch (Exception e) {
                morphium.handleNetworkError(i, e);
            }
        }
        long dur = System.currentTimeMillis() - start;
        morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
        morphium.getCache().clearCacheIfNecessary(cls);
        morphium.firePostUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> query, final String f, final List<?> v, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            @Override
            public void run() {
                List<?> value = v;
                String field = f;
                Class<?> cls = query.getType();
                String coll = morphium.getMapper().getCollectionName(cls);
                morphium.firePreUpdateEvent(morphium.getARHelper().getRealClass(cls), push ? MorphiumStorageListener.UpdateTypes.PUSH : MorphiumStorageListener.UpdateTypes.PULL);
                long start = System.currentTimeMillis();
                List lst = new ArrayList();
                for (Object o : value) {
                    lst.add(marshallIfNecessary(o));
                }
                value = lst;
                try {
                    Document qobj = query.toQueryObject();
                    if (insertIfNotExist) {
                        qobj = morphium.simplifyQueryObject(qobj);
                    }

                    field = morphium.getARHelper().getFieldName(cls, field);
                    Document set = new Document(field, value);
                    Document update = new Document(push ? "$pushAll" : "$pullAll", set);

                    List<String> lastChangeFields = morphium.getARHelper().getFields(cls, LastChange.class);
                    if (lastChangeFields != null && lastChangeFields.size() != 0) {
                        update.put("$set", new Document());
                        for (String fL : lastChangeFields) {
                            Field fld = morphium.getARHelper().getField(cls, fL);

                            if (fld.getType().equals(Date.class)) {

                                ((Document) update.get("$set")).put(fL, new Date());
                            } else {
                                ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                            }
                        }
                    }
                    if (insertIfNotExist) {
                        List<String> creationTimeFields = morphium.getARHelper().getFields(cls, CreationTime.class);
                        if (insertIfNotExist && creationTimeFields != null && creationTimeFields.size() != 0) {
                            long cnt = morphium.getDatabase().getCollection(coll).count(qobj);
                            if (cnt == 0) {
                                //not found, would insert
                                if (update.get("$set") == null) {
                                    update.put("$set", new Document());
                                }
                                for (String fL : creationTimeFields) {
                                    Field fld = morphium.getARHelper().getField(cls, fL);

                                    if (fld.getType().equals(Date.class)) {
                                        ((Document) update.get("$set")).put(fL, new Date());
                                    } else {
                                        ((Document) update.get("$set")).put(fL, System.currentTimeMillis());
                                    }
                                }
                            }
                        }
                    }

                    WriteConcern wc = morphium.getWriteConcernForClass(cls);
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            if (insertIfNotExist && !morphium.getDatabase().collectionExists(coll)) {
                                morphium.ensureIndicesFor((Class<T>) cls, coll, callback);
                            }
                            if (wc == null) {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple);
                            } else {
                                morphium.getDatabase().getCollection(coll).update(qobj, update, insertIfNotExist, multiple, wc);
                            }
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, update, dur, insertIfNotExist, multiple ? WriteAccessType.BULK_UPDATE : WriteAccessType.SINGLE_UPDATE);
                    morphium.getCache().clearCacheIfNecessary(cls);
                    morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    if (callback != null)
                        callback.onOperationSucceeded(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, null, null, field, value, insertIfNotExist, multiple);
                } catch (RuntimeException e) {
                    if (callback == null) throw new RuntimeException(e);
                    callback.onOperationError(push ? AsyncOperationType.PUSH : AsyncOperationType.PULL, query, System.currentTimeMillis() - start, e.getMessage(), e, null, field, value, insertIfNotExist, multiple);
                }
            }

        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> callback) {
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(cls, Entity.class)) {
            WriterTask r = new WriterTask() {
                private AsyncOperationCallback<T> callback;

                @Override
                public void setCallback(AsyncOperationCallback cb) {
                    callback = cb;
                }

                public void run() {
                    morphium.firePreDropEvent(cls);
                    long start = System.currentTimeMillis();
                    String co = collection;
                    if (co == null) {
                        co = morphium.getMapper().getCollectionName(cls);
                    }
                    DBCollection coll = morphium.getDatabase().getCollection(co);
//            coll.setReadPreference(com.mongodb.ReadPreference.PRIMARY);
                    for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                        try {
                            coll.drop();
                            break;
                        } catch (Throwable t) {
                            morphium.handleNetworkError(i, t);
                        }
                    }
                    long dur = System.currentTimeMillis() - start;
                    morphium.fireProfilingWriteEvent(cls, null, dur, false, WriteAccessType.DROP);
                    morphium.firePostDropEvent(cls);
                }
            };
            submitAndBlockIfNecessary(callback, r);
        } else {
            throw new RuntimeException("No entity class: " + cls.getName());
        }
    }

    @Override
    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, final Map<String, Object> options, AsyncOperationCallback<T> callback) {
        WriterTask r = new WriterTask() {
            private AsyncOperationCallback<T> callback;

            @Override
            public void setCallback(AsyncOperationCallback cb) {
                callback = cb;
            }

            public void run() {
                List<String> fields = morphium.getARHelper().getFields(cls);

                Map<String, Object> idx = new LinkedHashMap<String, Object>();
                for (Map.Entry<String, Object> es : index.entrySet()) {
                    String k = es.getKey();
                    if (!fields.contains(k) && !fields.contains(morphium.getARHelper().convertCamelCase(k))) {
                        throw new IllegalArgumentException("Field unknown for type " + cls.getSimpleName() + ": " + k);
                    }
                    String fn = morphium.getARHelper().getFieldName(cls, k);
                    idx.put(fn, es.getValue());
                }
                long start = System.currentTimeMillis();
                Document keys = new Document(idx);
                String coll = collection;
                if (coll == null) coll = morphium.getMapper().getCollectionName(cls);
                for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
                    try {
                        if (options == null) {
                            morphium.getDatabase().getCollection(coll).ensureIndex(keys);
                        } else {
                            Document opts = new Document(options);
                            morphium.getDatabase().getCollection(coll).ensureIndex(keys, opts);
                        }
                    } catch (Exception e) {
                        morphium.handleNetworkError(i, e);
                    }
                }
                long dur = System.currentTimeMillis() - start;
                morphium.fireProfilingWriteEvent(cls, keys, dur, false, WriteAccessType.ENSURE_INDEX);
            }


        };
        submitAndBlockIfNecessary(callback, r);
    }

    @Override
    public int writeBufferCount() {
        return executor.getActiveCount();
    }
}
