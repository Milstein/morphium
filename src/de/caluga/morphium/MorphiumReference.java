package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;

/**
 * User: Stephan Bösebeck
 * Date: 20.05.15
 * Time: 22:03
 * <p/>
 * TODO: Add documentation here
 */
@Embedded
public class MorphiumReference {
    private Object refToId;
    private String referencedCollection;

    public MorphiumReference(Object refToId, String referencedCollection) {
        this.refToId = refToId;
        this.referencedCollection = referencedCollection;
    }

    public Object getRefToId() {
        return refToId;
    }

    public void setRefToId(Object refToId) {
        this.refToId = refToId;
    }

    public String getReferencedCollection() {
        return referencedCollection;
    }

    public void setReferencedCollection(String referencedCollection) {
        this.referencedCollection = referencedCollection;
    }
}
