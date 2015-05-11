package de.caluga.morphium.bulk;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.BulkWriteOptions;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.async.AsyncOperationCallback;
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

    public BulkOperationContext(Morphium m, boolean ordered) {
        morphium = m;
        this.ordered = ordered;
        requestsByCollection = new HashMap<>();//new ArrayList<BulkRequestWrapper>();
        opts = new BulkWriteOptions();
        opts.ordered(ordered);
    }

    public <T> void insertList(List<T> o) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, null);
        w.insertMany(o);
        Class<? extends Object> cls = o.getClass();
        addRequest(w, cls);
    }

    public <T> void insert(T o) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, null);
        w.insert(o);
        Class<? extends Object> cls = o.getClass();
        addRequest(w, cls);
    }

    private void addRequest(BulkRequestWrapper w, Class<? extends Object> cls) {
        String collectionName = morphium.getMapper().getCollectionName(cls);
        if (requestsByCollection.get(collectionName) == null) {
            requestsByCollection.put(collectionName, new ArrayList<BulkRequestWrapper>());
        }
        requestsByCollection.get(collectionName).add(w);
    }

    public <T> BulkRequestWrapper addFind(Query<T> q) {
        BulkRequestWrapper w = new BulkRequestWrapper(morphium, this, q);
        addRequest(w, q.getType());
        return w;
    }

    public BulkWriteResult execute(AsyncOperationCallback cb) {
        BulkWriteOptions opt = new BulkWriteOptions();
        opt.ordered(ordered);

        final MorphiumBulkWriteResult result = new MorphiumBulkWriteResult();

        for (Map.Entry<String, List<BulkRequestWrapper>> entry : requestsByCollection.entrySet()) {
            morphium.getDatabase().getCollection(entry.getKey()).bulkWrite((List) entry.getValue(), opt, new SingleResultCallback<BulkWriteResult>() {
                @Override
                public void onResult(BulkWriteResult result, Throwable t) {

                }
            });
        }

//        List<? extends WriteModel<? extends Document>> bulk = null;
//
//        morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).bulkWrite(bulk, opts, new SingleResultCallback<BulkWriteResult>() {
//            @Override
//            public void onResult(BulkWriteResult result, Throwable t) {
//
//            }
//        });
//        bulk.insert(morphium.getMapper().marshall(o));
//        if (bulk == null) return new BulkWriteResult() {
//            @Override
//            public boolean isAcknowledged() {
//                return false;
//            }
//
//            @Override
//            public int getInsertedCount() {
//                return 0;
//            }
//
//            @Override
//            public int getMatchedCount() {
//                return 0;
//            }
//
//            @Override
//            public int getRemovedCount() {
//                return 0;
//            }
//
//            @Override
//            public boolean isModifiedCountAvailable() {
//                return false;
//            }
//
//            @Override
//            public int getModifiedCount() {
//                return 0;
//            }
//
//            @Override
//            public List<BulkWriteUpsert> getUpserts() {
//                return null;
//            }
//        };
//        for (BulkRequestWrapper w : requestsByCollection) {
//            w.preExec();
//        }
        long dur = System.currentTimeMillis();
        BulkWriteResult res = bulk.execute();
        dur = System.currentTimeMillis() - dur;
        for (BulkRequestWrapper w : requestsByCollection) {
            w.postExec();
        }
        for (BulkRequestWrapper w : requestsByCollection) {
            morphium.fireProfilingWriteEvent(w.getQuery().getType(), this, dur, false, WriteAccessType.BULK_UPDATE);
        }
        return res;
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
