package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.cache.CacheListener;
import de.caluga.morphium.cache.CacheObject;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 11:00
 * To change this template use File | Settings | File Templates.
 */
public class CacheListenerTest extends MongoTest {
    boolean wouldAdd = false;
    boolean wouldClear = false;
    boolean wouldRemove = false;

    @Test
    public void callbackTest() throws Exception {
        CacheListener cl = new CacheListener() {
            @Override
            public <T> CacheObject<T> wouldAddToCache(CacheObject<T> toCache) {
                wouldAdd = true;
                return toCache;
            }

            @Override
            public <T> boolean wouldClearCache(Class<T> affectedEntityType) {
                wouldClear = true;
                return true;
            }

            @Override
            public <T> boolean wouldRemoveEntryFromCache(Class cls, Object id, Object entity) {
                wouldRemove = true;
                return true;
            }
        };
        MorphiumSingleton.get().getCache().addCacheListener(cl);
        assert (MorphiumSingleton.get().getCache().isListenerRegistered(cl));


        super.createCachedObjects(100);

        for (int i = 0; i < 10; i++) {
            MorphiumSingleton.get().createQueryFor(CachedObject.class).f("counter").lte(i).asList();
        }
        waitForWrites();
        assert (wouldAdd);
        Thread.sleep(100);

        super.createCachedObjects(10);
        assert (wouldClear);

        MorphiumSingleton.get().getCache().removeCacheListener(cl);


    }

}
