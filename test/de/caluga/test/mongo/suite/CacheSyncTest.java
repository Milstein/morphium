package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.cache.CacheSyncListener;
import de.caluga.morphium.cache.CacheSyncVetoException;
import de.caluga.morphium.cache.CacheSynchronizer;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 12.06.12
 * Time: 16:40
 * <p/>
 */
public class CacheSyncTest extends MongoTest {
    private boolean preSendClear = false;
    private boolean postSendClear = false;
    private boolean preClear = false;
    private boolean postclear = false;

    @Test
    public void sendClearMsgTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Msg.class);
        Messaging msg = new Messaging(MorphiumSingleton.get(), 100, true);
        msg.start();
        CacheSynchronizer cs = new CacheSynchronizer(msg, MorphiumSingleton.get());

        Query<Msg> q = MorphiumSingleton.get().createQueryFor(Msg.class);
        long cnt = q.countAll();
        assert (cnt == 0) : "Already a message?!?! " + cnt;

        cs.sendClearMessage(CachedObject.class, "test");
        Thread.sleep(2000);
        waitForWrites();
        cnt = q.countAll();
        assert (cnt == 1) : "there should be one msg, there are " + cnt;
        msg.setRunning(false);
        cs.detach();
    }

    @Test
    public void removeFromCacheTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            CachedObject o = new CachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        for (int i = 0; i < 100; i++) {
            Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
            c = c.f("counter").eq(i);
            c.asList();
        }
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) != null) : "Cache entries not set?";
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) > 0) : "Cache entries not set? " + MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name());
        Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        c = c.f("counter").eq(10);
        ObjectId id = c.get().getId();
        Double cnt = MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name());
        MorphiumSingleton.get().getCache().removeEntryFromCache(CachedObject.class, id);
        Double cnt2 = MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name());
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) <= cnt - 1) : "Cache entries not set?";
        log.info("Count 1: " + cnt + " ---> " + cnt2);
    }

    @Test
    public void clearCacheTest() throws Exception {

        Messaging msg1 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg1.start();
        Messaging msg2 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg2.start();
        CacheSynchronizer cs1 = new CacheSynchronizer(msg1, MorphiumSingleton.get());
        CacheSynchronizer cs2 = new CacheSynchronizer(msg2, MorphiumSingleton.get());


        for (int i = 0; i < 100; i++) {
            CachedObject o = new CachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        for (int i = 0; i < 100; i++) {
            Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
            c = c.f("counter").eq(i);
            c.asList();
        }
        System.out.println("Stats " + MorphiumSingleton.get().getStatistics().toString());
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) != null) : "Cache entries not set?";
        cs1.sendClearAllMessage("test");
        Thread.sleep(2500);
        if ((MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) != 0)) {
            throw new AssertionError("Cache entries set? Entries: " + MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()));
        }
        msg1.setRunning(false);
        msg2.setRunning(false);
        cs1.detach();
        cs2.detach();
    }

    @Test
    public void idCacheTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Msg.class);
        MorphiumSingleton.get().dropCollection(IdCachedObject.class);
        //Making sure, indices are only created once...
        IdCachedObject o = new IdCachedObject();
        o.setCounter(0);
        o.setValue("a value");
        MorphiumSingleton.get().store(o);
        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        Thread.sleep(2000);
        long start = System.currentTimeMillis();
        for (int i = 1; i < 100; i++) {
            o = new IdCachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWriteToStart(1000000);
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("Storing without synchronizer: " + dur + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            Query<IdCachedObject> q = MorphiumSingleton.get().createQueryFor(IdCachedObject.class);
            IdCachedObject obj = (IdCachedObject) q.f("counter").eq(i).get();
            obj.setCounter(i + 1000);
            MorphiumSingleton.get().store(obj);
        }
        waitForWriteToStart(1000000);
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Updating without synchronizer: " + dur + " ms");


        MorphiumSingleton.get().clearCollection(IdCachedObject.class);
        Messaging msg1 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg1.start();

        CacheSynchronizer cs1 = new CacheSynchronizer(msg1, MorphiumSingleton.get());
        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            o = new IdCachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Storing with synchronizer: " + dur + " ms");

        Thread.sleep(15000);
        start = System.currentTimeMillis();
        int notFoundCounter = 0;
        for (int i = 0; i < 100; i++) {
            Query<IdCachedObject> q = MorphiumSingleton.get().createQueryFor(IdCachedObject.class);
            q = q.f("counter").eq(i);
            IdCachedObject obj = q.get();
            if (obj == null) {
                log.info("Object not found... waiting");
                Thread.sleep(1550); //wait a moment
                obj = q.get();
            }
            if (obj == null) {
                notFoundCounter++;
                continue;
            } else {
                obj.setCounter(i + 2000);
            }
            assert (notFoundCounter < 10) : "too many objects not found";
            MorphiumSingleton.get().store(obj);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Updates queued... " + dur + "ms");
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Updating with synchronizer: " + dur + " ms");


        msg1.setRunning(false);
        cs1.detach();

    }

    private void waitForWriteToStart(int max) {
        int cnt = 0;
        while (MorphiumSingleton.get().getWriteBufferCount() == 0) {
            //wait for things to get started...
            Thread.yield();
            cnt++;
            if (cnt > max) return;
        }
    }


    @Cache(readCache = true, syncCache = Cache.SyncCacheStrategy.UPDATE_ENTRY)
    @WriteBuffer(timeout = 1000)
    @WriteSafety(waitForJournalCommit = true, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    public static class IdCachedObject extends CachedObject {

    }

    @Test
    public void testListeners() throws Exception {
        MorphiumSingleton.get().dropCollection(IdCachedObject.class);
        final Messaging msg1 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg1.start();
        final Messaging msg2 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg2.start();


        final CacheSynchronizer cs1 = new CacheSynchronizer(msg1, MorphiumSingleton.get());
        cs1.addSyncListener(new CacheSyncListener() {
            @Override
            public void preClear(Class cls, Msg m) throws CacheSyncVetoException {
            }

            @Override
            public void postClear(Class cls, Msg m) {
            }

            @Override
            public void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException {
                log.info("in preSendClearMsg");
                preSendClear = true;
            }

            @Override
            public void postSendClearMsg(Class cls, Msg m) {
                log.info("in postSendClearMsg");
                postSendClear = true;
            }
        });

        final CacheSynchronizer cs2 = new CacheSynchronizer(msg2, MorphiumSingleton.get());
        cs2.addSyncListener(new CacheSyncListener() {
            @Override
            public void preClear(Class cls, Msg m) throws CacheSyncVetoException {
                log.info("in preClear");
                preClear = true;
            }

            @Override
            public void postClear(Class cls, Msg m) {
                log.info("In postClear");
                postclear = true;
            }

            @Override
            public void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException {
            }

            @Override
            public void postSendClearMsg(Class cls, Msg m) {
            }
        });


        new Thread() {
            public void run() {
                MorphiumSingleton.get().store(new CachedObject());
                waitForWrites();
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                msg1.setRunning(false);
                msg2.setRunning(false);
                cs1.detach();
                cs2.detach();

            }
        }.start();

        while (cs1.isAttached()) {
            log.info("still attached - waiting");
            Thread.sleep(500);
        }
        assert (preClear);
        assert (postclear);
        assert (preSendClear);
        assert (postSendClear);

    }


    @Test
    public void cacheSyncTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Msg.class);
        createCachedObjects(1000);

        Morphium m1 = MorphiumSingleton.get();
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setAdr(m1.getConfig().getAdr());
        cfg2.setDatabase(m1.getConfig().getDatabase());

        Morphium m2 = new Morphium(cfg2);
        Messaging msg1 = new Messaging(m1, 200, true);
        Messaging msg2 = new Messaging(m2, 200, true);

        msg1.start();
        msg2.start();

        CacheSynchronizer cs1 = new CacheSynchronizer(msg1, m1);
        CacheSynchronizer cs2 = new CacheSynchronizer(msg2, m2);
        waitForWrites();

        //fill caches
        log.info("Filling cches...");
        for (int i = 0; i < 100; i++) {
            m1.createQueryFor(CachedObject.class).f("counter").lte(i + 10).asList(); //fill cache
            m2.createQueryFor(CachedObject.class).f("counter").lte(i + 10).asList(); //fill cache
        }
        //1 always sends to 2....
        log.info("done.");


        CachedObject o = m1.createQueryFor(CachedObject.class).f("counter").eq(155).get();
        cs2.addSyncListener(CachedObject.class, new CacheSyncListener() {
            @Override
            public void preClear(Class cls, Msg m) throws CacheSyncVetoException {
                log.info("Should clear cache");
                preClear = true;
            }

            @Override
            public void postClear(Class cls, Msg m) {
                log.info("did clear cache");
                postclear = true;
            }

            @Override
            public void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException {
                log.info("will send clear message");
                preSendClear = true;
            }

            @Override
            public void postSendClearMsg(Class cls, Msg m) {
                log.info("just sent clear message");
                postSendClear = true;
            }
        });
        msg2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                log.info("Got message " + m.getName());
                return null;
            }
        });
        log.info("resetting...");
        preSendClear = false;
        preClear = false;
        postclear = false;
        postSendClear = false;
        o.setValue("changed it");
        log.info("Storing..");
        m1.store(o);
        log.info("done.");

        Thread.sleep(3000);
        log.info("sleep finished " + postclear);
        assert (!preSendClear);
        assert (!postSendClear);
        assert (postclear);
        assert (preClear);
        log.info("Waiting a minute for msg to be cleared... ");
        Thread.sleep(60000);

        long l = m1.createQueryFor(Msg.class).countAll();
        assert (l <= 1) : "too many messages? " + l;

//        createCachedObjects(50);


//        Thread.sleep(90000); //wait for messages to be cleared
//        assert(m1.createQueryFor(Msg.class).countAll()==0);
        cs1.detach();
        cs2.detach();
        msg1.setRunning(false);
        msg2.setRunning(false);
        m2.close();

    }

}
