/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * @author stephan
 *         These listeners will be informed about Storing _any_ object in morphium!
 */
public interface MorphiumStorageListener<T> {
    public enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, DEC, MUL, MIN, MAX, RENAME, POP, INSERT,
    }

    public void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    public void preStore(Morphium m, Map<T, Boolean> isNew) throws MorphiumAccessVetoException;


    public void postStore(Morphium m, T r, boolean isNew);

    public void postStore(Morphium m, Map<T, Boolean> isNew);

    public void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    public void preRemove(Morphium m, T r) throws MorphiumAccessVetoException;

    public void postRemove(Morphium m, T r);

    public void postRemove(Morphium m, List<T> lst);

    public void postDrop(Morphium m, Class<? extends T> cls);

    public void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    public void postRemove(Morphium m, Query<T> q);

    public void postLoad(Morphium m, T o);

    public void postLoad(Morphium m, List<T> o);

    public void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

    public void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

}
