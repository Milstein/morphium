package de.caluga.morphium.bulk;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.BulkWriteOptions;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 28.04.14
 * Time: 22:33
 * <p/>
 * TODO: Add documentation here
 */
public class BulkOperationContext {
    private Morphium morphium;

    private boolean ordered;

    private Map<String, List<BulkRequestWrapper>> requestsByCollection;
    private final BulkWriteOptions opts;
    private Logger log = new Logger(BulkOperationContext.class);

    public BulkOperationContext(Morphium m, boolean ordered) {
        morphium = m;
        this.ordered = ordered;
        requestsByCollection = new HashMap<>();//new ArrayList<BulkRequestWrapper>();
        opts = new BulkWriteOptions();
        opts.ordered(ordered);
    }

    public <T> void insertList(List<T> o) {
        insertList(o, null);
    }

    public <T> void insertList(List<T> o, String collection) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, null);
        w.insertMany(o);
        Class<? extends Object> cls = o.get(0).getClass();
        if (collection != null) {
            for (Object l : o) {
                if (!cls.getClasses().equals(cls)) {
                    log.info("writing different types into one collection " + collection);
                }
            }
        }
        addRequest(w, collection != null ? collection : morphium.getMapper().getCollectionName(cls));
    }

    public <T> void insert(T o) {
        insert(o, null);
    }

    public <T> void insert(T o, String collection) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, null);
        w.insert(o);
        Class<? extends Object> cls = o.getClass();
        addRequest(w, collection != null ? collection : morphium.getMapper().getCollectionName(cls));
    }

    private void addRequest(BulkRequestWrapper w, String collectionName) {
        if (requestsByCollection.get(collectionName) == null) {
            requestsByCollection.put(collectionName, new ArrayList<BulkRequestWrapper>());
        }
        requestsByCollection.get(collectionName).add(w);
    }

    public <T> BulkRequestWrapper addFind(Query<T> q) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, q);
        String collectionName = q.getCollectionName();
        addRequest(w, collectionName != null ? collectionName : morphium.getMapper().getCollectionName(q.getType()));
        return w;
    }

    public BulkWriteResult execute(final AsyncOperationCallback cb) {
        BulkWriteOptions opt = new BulkWriteOptions();
        opt.ordered(ordered);

        final MorphiumBulkWriteResult ret = new MorphiumBulkWriteResult();
        final long start = System.currentTimeMillis();
        for (final Map.Entry<String, List<BulkRequestWrapper>> entry : requestsByCollection.entrySet()) {
            morphium.getDatabase().getCollection(entry.getKey()).bulkWrite((List) entry.getValue(), opt, new SingleResultCallback<BulkWriteResult>() {
                @Override

                public void onResult(BulkWriteResult result, Throwable t) {
                    try {
                        for (BulkRequestWrapper w : entry.getValue()) {
                            w.preExec();
                        }
                        if (cb != null) {
                            if (t == null) {
                                cb.onOperationSucceeded(AsyncOperationType.BULK, null, System.currentTimeMillis() - start, null, entry.getKey(), null, null, result, entry.getValue());
                            } else {
                                cb.onOperationError(AsyncOperationType.BULK, null, System.currentTimeMillis() - start, null, entry.getKey(), t.getMessage(), t, null, result, entry.getValue());
                            }
                        } else {
                            ret.addResults(result);
                        }
                        for (BulkRequestWrapper w : entry.getValue()) {
                            w.postExec();
                        }
                    } finally {
                        result.notifyAll();
                    }
                }
            });
        }

        if (cb != null) {
            try {
                ret.wait(morphium.getConfig().getAsyncOperationTimeout());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return ret;
        }
        return null;
    }

    private class MorphiumBulkWriteResult extends BulkWriteResult {
        private boolean wasAcknoledged = false;
        private int insertedCount = 0;
        private int matchedCount = 0;
        private int deletedCount;
        private boolean isModifiedCountAvailable;
        private int modifiedCount;
        private List<BulkWriteUpsert> upserts;


        public MorphiumBulkWriteResult() {
            this.wasAcknoledged = false;
            this.insertedCount = 0;
            this.matchedCount = 0;
            this.deletedCount = 0;
            this.isModifiedCountAvailable = false;
            this.modifiedCount = 0;
            this.upserts = new ArrayList<>();
        }

        public void setWasAcknoledged(boolean wasAcknoledged) {
            this.wasAcknoledged = wasAcknoledged;
        }

        public void setInsertedCount(int insertedCount) {
            this.insertedCount = insertedCount;
        }

        public void setMatchedCount(int matchedCount) {
            this.matchedCount = matchedCount;
        }

        public void setDeletedCount(int deletedCount) {
            this.deletedCount = deletedCount;
        }

        public void setIsModifiedCountAvailable(boolean isModifiedCountAvailable) {
            this.isModifiedCountAvailable = isModifiedCountAvailable;
        }

        public void setGetModifiedCount(int modifiedCount) {
            this.modifiedCount = modifiedCount;
        }

        public void setUpserts(List<BulkWriteUpsert> upserts) {
            this.upserts = upserts;
        }

        @Override
        public boolean wasAcknowledged() {
            return wasAcknoledged;
        }

        @Override
        public int getInsertedCount() {
            return insertedCount;
        }

        @Override
        public int getMatchedCount() {
            return matchedCount;
        }

        @Override
        public int getDeletedCount() {
            return deletedCount;
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return isModifiedCountAvailable;
        }

        @Override
        public int getModifiedCount() {
            return modifiedCount;
        }

        @Override
        public List<BulkWriteUpsert> getUpserts() {
            return upserts;
        }

        public void addResults(BulkWriteResult res) {
            if (res.isModifiedCountAvailable()) {
                modifiedCount += res.getModifiedCount();
                isModifiedCountAvailable = true;
            } else {
                isModifiedCountAvailable = false;
            }
            if (res.getUpserts() != null) upserts.addAll(res.getUpserts());
            matchedCount += res.getMatchedCount();
            insertedCount += res.getInsertedCount();
            if (!res.wasAcknowledged()) {
                wasAcknoledged = false;
            }
        }
    }
}
