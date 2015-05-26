package de.caluga.morphium.async;

import de.caluga.morphium.query.Query;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 13.02.13
 * Time: 15:39
 * <p/>
 * Callback interface for async calls
 */
public interface AsyncOperationCallback<T> {
    /**
     * @param type     - type of operation performed
     * @param q        - the query for the operation (might be null)
     * @param duration - duration of the whole thing
     * @param entityType
     * @param result   - list of all results, might be null (on update or remove)
     * @param param    - the parameter (e.g. the object to store, or the list of objects) - might be null
     */
    public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, Class<? extends T> entityType, String collectionName, List<T> result, T entity, Object... param);

    /**
     * callback for insuccesfull operations
     *
     * @param type     - type of operation performed
     * @param q        - the query (might be null)
     * @param duration - the duration
     * @param error    - error message
     * @param t        - the exception (if any)
     */
    public void onOperationError(AsyncOperationType type, Query<T> q, long duration, Class<? extends T> entityType, String collectionName, String error, Throwable t, T entity, Object... param);
}
