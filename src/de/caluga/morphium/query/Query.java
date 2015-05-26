package de.caluga.morphium.query;

import com.mongodb.ServerAddress;
import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:29
 * <p/>
 * usage:
 * <code>
 * Query<UncachedObject> q= Morphium.get().createQueryFor(UncachedObject.class);
 * q=q.f("counter").lt(15).f("counter").gt(10);
 * </code>
 * Or
 * <code>
 * q.or(q.q().f("counter").eq(15),q.q().f("counter").eq(22));
 * </code>
 * AND is the default!
 */
@SuppressWarnings("UnusedDeclaration")
public interface Query<T> extends Cloneable {
    /**
     * set the where string for this query - where-String needs to be valid java script! Errors will only be shown in MongoD-Log!
     *
     * @param wh where string
     * @return where wuery
     */
    public Query<T> where(String wh);

    /**
     * Get a field. F may be the name as it is in mongo db or the variable name in java...
     *
     * @param f field
     * @return the field implementation
     */
    public MongoField<T> f(String f);

    /**
     * returns the serveraddress the query was executed on
     *
     * @return the serveraddress the query was executed on, null if not executed yet
     */
    public ServerAddress getServer();

    /**
     * same as f(field.name())
     *
     * @param field field
     * @return mongo field
     */
    public MongoField<T> f(Enum field);

    /**
     * concatenate those queries with or
     *
     * @param q query
     */
    public Query<T> or(Query<T>... q);

    /**
     * concatenate those queries with or
     *
     * @param q query
     */
    public Query<T> or(List<Query<T>> q);

    /**
     * not or
     *
     * @param q query
     */
    public Query<T> nor(Query<T>... q);

    /**
     * limit the number of entries in result
     *
     * @param i - limit
     * @return the query
     */
    public Query<T> limit(int i);

    /**
     * skip the first entries in result
     *
     * @param i skip
     * @return the query
     */
    public Query<T> skip(int i);

    /**
     * set an order - Key: FieldName (java or Mongo-Name), Value: Integer: -1 reverse, 1 standard
     *
     * @param n - sort
     * @return the query
     */
    public Query<T> sort(Map<String, Object> n);

    /**
     * set order by prefixing field names with - for reverse ordering (+ or nothing default)
     *
     * @param prefixedString sort
     * @return the query
     */
    public Query<T> sort(String... prefixedString);

    public Query<T> sort(Enum... naturalOrder);

    /**
     * count all results in query - does not take limit or skip into account
     *
     * @return number
     */
    public long countAll();  //not taking limit and skip into account!

    public void countAll(AsyncOperationCallback<T> callback);

    /**
     * needed for creation of the query representation tree
     *
     * @param e expression
     */
    public void addChild(FilterExpression e);

    /**
     * create a db object from this query and all of it's child nodes
     *
     * @return query object
     */
    public Document toQueryObject();

    /**
     * what type this query is for
     *
     * @return class
     */
    public Class<? extends T> getType();

    /**
     * the result as list
     *
     * @return list
     */
    public List<T> asList();

    public void asList(AsyncOperationCallback<T> callback);

    /**
     * create an iterator / iterable for this query, default windowSize (10), prefetch windows 1
     */
    public MorphiumIterator<T> asIterable();


    /**
     * create an iterator / iterable for this query, sets window size (how many objects should be read from DB)
     * prefetch number is 1 in this case
     */
    public MorphiumIterator<T> asIterable(int windowSize);

    /**
     * create an iterator / iterable for this query, sets window size (how many entities are read en block) and how many windows of this size will be prefechted...
     *
     * @param windowSize
     * @param prefixWindows
     * @return
     */

    public MorphiumIterator<T> asIterable(int windowSize, int prefixWindows);


    /**
     * get only 1 result (first one in result list)
     *
     * @return entity
     */
    public T get();

    public void get(AsyncOperationCallback<T> callback);


    /**
     * only return the IDs of objects (useful if objects are really large)
     *
     * @return list of Ids, type R
     */
    public <R> List<R> idList();

    public void idList(AsyncOperationCallback<T> callback);

    /**
     * what type to use
     *
     * @param type type
     */
    public void setType(Class<? extends T> type);

    /**
     * create a new empty query for the same type using the same mapper as this
     *
     * @return query
     */
    public Query<T> q();

    public List<T> complexQuery(Document query);

    /**
     * just sends the given query to the MongoDBDriver and masrhalls objects as listed
     * ignores all other query settings!!!!!
     *
     * @param query - query to be sent
     * @param skip  - amount to skip
     * @param limit - maximium number of results
     * @return list of objects matching query
     */
    public List<T> complexQuery(Document query, Map<String, Object> sort, int skip, int limit);

    public List<T> complexQuery(Document query, String sort, int skip, int limit);

    /**
     * same as copmplexQuery(query,0,1).get(0);
     *
     * @param query - query
     * @return type
     */
    public T complexQueryOne(Document query);

    public T complexQueryOne(Document query, Map<String, Object> sort, int skip);

    public T complexQueryOne(Document query, Map<String, Object> sort);

    public int getLimit();

    public int getSkip();

    public Map<String, Object> getSort();

    public Query<T> clone() throws CloneNotSupportedException;

    public ReadPreferenceLevel getReadPreferenceLevel();

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel);

    public String getWhere();

    public Morphium getMorphium();

    public void setMorphium(Morphium m);

    public void setExecutor(ThreadPoolExecutor executor);


    public void getById(Object id, AsyncOperationCallback<T> callback);

    public T getById(Object id);

    public int getNumberOfPendingRequests();

    /**
     * use a different collection name for the query
     *
     * @param n
     */
    public void setCollectionName(String n);

    public String getCollectionName();

    @Deprecated
    public List<T> textSearch(String... texts);

    @Deprecated
    public List<T> textSearch(TextSearchLanguages lang, String... texts);

    public MongoField<T> f(Enum... f);

    public MongoField<T> f(String... f);

    public void delete();

    public void setAutoValuesEnabled(boolean autoValues);

    public boolean isAutoValuesEnabled();

    public void disableAutoValues();

    public void enableAutoValues();


    public Query<T> text(String... text);

    public Query<T> text(TextSearchLanguages lang, String... text);

    public Query<T> text(String metaScoreField, TextSearchLanguages lang, String... text);

    public void setReturnedFields(String... fl);

    public void addReturnedField(String f);

    public void setReturnedFields(Enum... fl);

    public void addReturnedField(Enum f);


    List<T> complexQuery(Document query, Map<String, Object> sort, int skip, int limit, AsyncOperationCallback<T> cb);

    public List distinct(String field, AsyncOperationCallback<T> callback);

    public enum TextSearchLanguages {
        danish,
        dutch,
        english,
        finnish,
        french,
        german,
        hungarian,
        italian,
        norwegian,
        portuguese,
        romanian,
        russian,
        spanish,
        swedish,
        turkish,
        mongo_default,
        none,

    }
}
