/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author stephan
 */
public class MassCacheTest extends MongoTest {

    public static final int NO_OBJECTS = 200;
    public static final int WRITING_THREADS = 5;
    public static final int READING_THREADS = 5;
    private static final Logger log = new Logger(MassCacheTest.class);

    @Test
    public void massiveParallelWritingTest() throws InterruptedException {

        log.info("\nMassive parallel writing - single creating objects");
        long start = System.currentTimeMillis();
        ArrayList<Thread> thr = new ArrayList<Thread>();
        for (int i = 0; i < WRITING_THREADS; i++) {
            Thread t = new Thread() {

                public void run() {
                    for (int j = 0; j < NO_OBJECTS; j++) {
                        CachedObject o = new CachedObject();
                        o.setCounter(j + 1);
                        o.setValue(getName() + " " + j);
                        log.info("Storing...");
                        MorphiumSingleton.get().store(o);
                        log.info("Stored object..." + getId() + " / " + getName());
                    }
                }
            };
            t.setName("Writing thread " + i);
            t.start();
            thr.add(t);

        }
        long dur = System.currentTimeMillis() - start;
        log.info("Starting threads. Took " + dur + " ms");

        //Waiting for threads
        for (Thread t : thr) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        waitForWrites();

        dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms");
        Thread.sleep(2500);
        log.info("Checking consistency");
        start = System.currentTimeMillis();
        for (int i = 0; i < WRITING_THREADS; i++) {
            for (int j = 0; j < NO_OBJECTS; j++) {
//                CachedObject o = new CachedObject();
//                o.setCounter(j + 1);
//                o.setValue("Writing thread " + i + " " + j);
                Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
                q.f("counter").eq(j + 1).f("value").eq("Writing thread " + i + " " + j);
                List<CachedObject> lst = MorphiumSingleton.get().find(q);

                assert (lst != null && lst.size() > 0) : "List is null - Thread " + i + " Element " + (j + 1) + " not found";

            }
            log.info(i + "" + "/" + WRITING_THREADS);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading all objects took: " + dur + " ms");
        printStats();


    }

    private void printStats() {
        final Map<String, Double> statistics = MorphiumSingleton.get().getStatistics();
        for (String k : statistics.keySet()) {
            log.info(k + ": " + statistics.get(k));
        }
    }

    @Test
    public void massiveParallelAccessTest() {
        log.info("\nMassive parallel reading & writing - single creating objects");
        long start = System.currentTimeMillis();
        ArrayList<Thread> thr = new ArrayList<Thread>();
        for (int i = 0; i < WRITING_THREADS; i++) {
            Thread t = new Thread() {

                public void run() {
                    for (int j = 0; j < NO_OBJECTS; j++) {
                        CachedObject o = new CachedObject();
                        o.setCounter(j + 1);
                        o.setValue(getName() + " " + j);
                        MorphiumSingleton.get().store(o);
                    }
                }
            };
            t.setName("Writing thread " + i);
            t.start();
            thr.add(t);
        }
        log.info("Waiting a bit...");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            new Logger(MassCacheTest.class.getName()).fatal(ex);
        }

        log.info("Creating reader threads (random read)...");
        for (int i = 0; i < READING_THREADS; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int j = 0; j < NO_OBJECTS * 2; j++) {
                        int rnd = (int) (Math.random() * NO_OBJECTS);
                        int trnd = (int) (Math.random() * WRITING_THREADS);
                        CachedObject o = new CachedObject();
                        o.setCounter(rnd + 1);
                        o.setValue("Writing thread " + trnd + " " + rnd);
                        Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
                        q.f("counter").eq(rnd + 1).f("value").eq("Writing thread " + trnd + " " + rnd);
                        List<CachedObject> lst = MorphiumSingleton.get().find(q);
                        if (lst == null || lst.size() == 0) {
                            log.info("Not written yet: " + (rnd + 1) + " Thread: " + trnd);
                        } else {
                            o = lst.get(0);
                            o.setValue(o.getValue() + " altered by Thread " + getName());
                            MorphiumSingleton.get().store(o);
                        }
                    }
                }
            };
            t.setName("alter thread " + i);
            t.start();
            thr.add(t);
        }
        //Waiting for threads
        for (Thread t : thr) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms\n");

        printStats();

        log.info("Test finished!");
    }


    @Test
    public void disableCacheTest() {
        MorphiumSingleton.get().getConfig().disableReadCache();
        MorphiumSingleton.get().getConfig().disableBufferedWrites();
        MorphiumSingleton.get().resetStatistics();
        log.info("Preparing test data...");
        for (int j = 0; j < NO_OBJECTS; j++) {
            CachedObject o = new CachedObject();
            o.setCounter(j + 1);
            o.setValue("Test " + j);
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        log.info("Done.");

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (lst.size() > 0) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
        }
        printStats();

        Map<String, Double> statistics = MorphiumSingleton.get().getStatistics();
        assert (statistics.get("CACHE_ENTRIES") == 0);
        assert (statistics.get("WRITES_CACHED") == 0);
        MorphiumSingleton.get().getConfig().enableReadCache();
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (lst.size() > 0) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
        }
        printStats();
        statistics = MorphiumSingleton.get().getStatistics();
        assert (statistics.get("CACHE_ENTRIES") != 0);
        assert (statistics.get("CHITS") != 0);
        MorphiumSingleton.get().getConfig().enableReadCache();
        MorphiumSingleton.get().getConfig().enableBufferedWrites();
    }

    @Test
    public void cacheTest() {
        log.info("Preparing test data...");
        for (int j = 0; j < NO_OBJECTS; j++) {
            CachedObject o = new CachedObject();
            o.setCounter(j + 1);
            o.setValue("Test " + j);
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        log.info("Done.");

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (lst.size() > 0) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
        }
        printStats();
        Map<String, Double> stats = MorphiumSingleton.get().getStatistics();
        assert (stats.get("CACHE_ENTRIES") != 0);
        assert (stats.get("CHITS") != 0);
    }


}
