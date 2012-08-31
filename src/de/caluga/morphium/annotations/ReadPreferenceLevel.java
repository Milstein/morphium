package de.caluga.morphium.annotations;

import com.mongodb.ReadPreference;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 13:54
 * <p/>
 * TODO: Add documentation here
 */
public enum ReadPreferenceLevel {
    PRIMARY(ReadPreference.primary()), PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
    SECONDARY(ReadPreference.secondary()), SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
    NEAREST(ReadPreference.nearest());
    private ReadPreference pref;

    private ReadPreferenceLevel(ReadPreference pref) {
        this.pref = pref;
    }

    public ReadPreference getPref() {
        return pref;
    }

    public void setPref(ReadPreference pref) {
        this.pref = pref;
    }
}