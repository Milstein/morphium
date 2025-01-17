package de.caluga.morphium;


import com.mongodb.DB;
import com.mongodb.ServerAddress;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorFactory;
import de.caluga.morphium.aggregation.AggregatorFactoryImpl;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheImpl;
import de.caluga.morphium.query.*;
import de.caluga.morphium.writer.AsyncWriterImpl;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import org.json.simple.parser.ParseException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Stores the configuration for the MongoDBLayer.
 *
 * @author stephan
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded
public class MorphiumConfig {
    @AdditionalData(readOnly = false)
    private Map<String, String> restoreData;
    //    private MongoDbMode mode;
    private int maxConnections, housekeepingTimeout;
    private int globalCacheValidTime = 5000;
    private int writeCacheTimeout = 5000;
    private String database;
    @Transient
    private DB db = null;
    @Transient
    private MorphiumWriter writer;
    @Transient
    private MorphiumWriter bufferedWriter;
    @Transient
    private MorphiumWriter asyncWriter;
    private int connectionTimeout = 0;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = true;
    private boolean safeMode = false;
    private boolean globalFsync = false;
    private boolean globalJ = false;
    private boolean checkForNew = false;
    private int writeTimeout = 0;

    private int globalLogLevel = 0;
    private boolean globalLogSynced = false;
    private String globalLogFile = null;

    //maximum number of tries to queue a write operation
    private int maximumRetriesBufferedWriter = 10;
    private int maximumRetriesWriter = 10;
    private int maximumRetriesAsyncWriter = 10;
    //wait bewteen tries
    private int retryWaitTimeBufferedWriter = 200;
    private int retryWaitTimeWriter = 200;
    private int retryWaitTimeAsyncWriter = 200;
    private int globalW = 1; //number of writes
    private int maxWaitTime = 120000;
    //default time for write buffer to be filled
    private int writeBufferTime = 1000;
    //ms for the pause of the main thread
    private int writeBufferTimeGranularity = 100;
    private boolean autoreconnect = true;
    private int maxAutoReconnectTime = 0;
    private int blockingThreadsMultiplier = 5;
    @Transient
    private Class<? extends Query> queryClass;
    @Transient
    private Class<? extends Aggregator> aggregatorClass;
    @Transient
    private QueryFactory queryFact;
    @Transient
    private AggregatorFactory aggregatorFactory;
    @Transient
    private MorphiumCache cache;
    private int replicaSetMonitoringTimeout = 5000;
    private int retriesOnNetworkError = 1;
    private int sleepBetweenNetworkErrorRetries = 1000;
    /**
     * login credentials for MongoDB - if necessary. If null, don't authenticate
     */
    private String mongoLogin = null, mongoPassword = null;

    private boolean autoValues = true;
    boolean readCacheEnabled = true;
    boolean asyncWritesEnabled = true;
    boolean bufferedWritesEnabled = true;
    boolean camelCaseConversionEnabled = true;

    @Transient
    private List<ServerAddress> adr = new ArrayList<>();
    //securitysettings
//    private Class<? extends Object> userClass, roleClass, aclClass;
    private String mongoAdminUser, mongoAdminPwd; //THE superuser!
    @Transient
    private Class<? extends ObjectMapper> omClass = ObjectMapperImpl.class;
    @Transient
    private Class<? extends MongoField> fieldImplClass = MongoFieldImpl.class;
    @Transient
    private ReadPreferenceLevel defaultReadPreference;
    private int acceptableLatencyDifference = 15;
    private int threadPoolMessagingCoreSize = 0;
    private int threadPoolMessagingMaxSize = 100;
    private long threadPoolMessagingKeepAliveTime = 2000;
    private int threadPoolAsyncOpCoreSize = 1;
    private int threadPoolAsyncOpMaxSize = 1000;
    private long threadPoolAsyncOpKeepAliveTime = 1000;
    private boolean objectSerializationEnabled = true;
    private boolean cursorFinalizerEnabled = false;
    private boolean alwaysUseMBeans = false;
    private int heartbeatConnectTimeout = 0;
    private int heartbeatFrequency = 1000;
    private int heartbeatSocketTimeout = 1000;
    private int minConnectionsPerHost = 1;
    private int minHearbeatFrequency = 2000;
    private int localThreashold = 0;
    private int maxConnectionIdleTime = 1000;
    private int maxConnectionLifeTime = 5000;
    private String requiredReplicaSetName = null;

    public MorphiumConfig(Properties prop) {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true); //settings always convert camel case
        List<Field> flds = an.getAllFields(MorphiumConfig.class);
        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) continue;
            f.setAccessible(true);
            if (prop.getProperty(f.getName()) != null) {
                try {
                    if (f.getType().equals(int.class) || f.getType().equals(Integer.class)) {
                        f.set(this, Integer.parseInt((String) prop.get(f.getName())));
                    } else if (f.getType().equals(String.class)) {
                        f.set(this, prop.get(f.getName()));
                    } else if (f.getType().equals(boolean.class) || f.getType().equals(Boolean.class)) {
                        f.set(this, prop.get(f.getName()).equals("true"));
                    } else if (f.getType().equals(long.class) || f.getType().equals(Long.class)) {
                        f.set(this, Long.parseLong((String) prop.get(f.getName())));
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        try {
            parseClassSettings(this, prop);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public MorphiumConfig() {
        this("test", 10, 60000, 10000);
    }


    public MorphiumConfig(String db, int maxConnections, int globalCacheValidTime, int housekeepingTimeout) {
        database = db;
        adr = new ArrayList<>();

        this.maxConnections = maxConnections;
        this.globalCacheValidTime = globalCacheValidTime;
        this.housekeepingTimeout = housekeepingTimeout;

    }

    public boolean isCheckForNew() {
        return checkForNew;
    }

    public void setCheckForNew(boolean checkForNew) {
        this.checkForNew = checkForNew;
    }


    public static MorphiumConfig createFromJson(String json) throws ParseException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException, InstantiationException, UnknownHostException, NoSuchMethodException, InvocationTargetException {
        MorphiumConfig cfg = new ObjectMapperImpl().unmarshall(MorphiumConfig.class, json);
        parseClassSettings(cfg, cfg.restoreData);
        return cfg;
    }

    private static void parseClassSettings(MorphiumConfig cfg, Map settings) throws UnknownHostException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException {
        for (Object ko : settings.keySet()) {
            String k = (String) ko;
            String value = (String) settings.get(k);
            if (k.equals("hosts")) {
                String lst = value;
                for (String adr : lst.split(",")) {
                    String a[] = adr.split(":");
                    cfg.addHost(a[0].trim(), Integer.parseInt(a[1].trim()));
                }

            } else {
                if (!k.endsWith("ClassName")) continue;
                String n[] = k.split("_");
                if (n.length != 3) continue;
                Class cls = Class.forName(value);
                Field f = MorphiumConfig.class.getDeclaredField(n[0]);
                f.setAccessible(true);
                if (n[1].equals("C")) {
                    f.set(cfg, cls);
                } else if (n[1].equals("I")) {
                    f.set(cfg, cls.newInstance());
                }
            }
        }

        cfg.getAggregatorFactory().setAggregatorClass(cfg.getAggregatorClass());
        cfg.getQueryFact().setQueryImpl(cfg.getQueryClass());
    }

    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    public void setRetriesOnNetworkError(int retriesOnNetworkError) {
        if (retriesOnNetworkError == 0) {
            new Logger(MorphiumConfig.class).warn("Cannot set retries on network error to 0 - minimum is 1");
            retriesOnNetworkError = 1;
        }
        this.retriesOnNetworkError = retriesOnNetworkError;
    }

    public int getSleepBetweenNetworkErrorRetries() {
        return sleepBetweenNetworkErrorRetries;
    }

    public void setSleepBetweenNetworkErrorRetries(int sleepBetweenNetworkErrorRetries) {
        this.sleepBetweenNetworkErrorRetries = sleepBetweenNetworkErrorRetries;
    }

    public int getReplicaSetMonitoringTimeout() {
        return replicaSetMonitoringTimeout;
    }

    public void setReplicaSetMonitoringTimeout(int replicaSetMonitoringTimeout) {
        this.replicaSetMonitoringTimeout = replicaSetMonitoringTimeout;
    }

    public int getWriteBufferTimeGranularity() {
        return writeBufferTimeGranularity;
    }

    public void setWriteBufferTimeGranularity(int writeBufferTimeGranularity) {
        this.writeBufferTimeGranularity = writeBufferTimeGranularity;
    }

    public MorphiumCache getCache() {
        if (cache == null) {
            cache = new MorphiumCacheImpl();
        }
        return cache;
    }

    public void setCache(MorphiumCache cache) {
        this.cache = cache;
    }

    public int getWriteBufferTime() {
        return writeBufferTime;
    }

    public void setWriteBufferTime(int writeBufferTime) {
        this.writeBufferTime = writeBufferTime;
    }

    public Class<? extends ObjectMapper> getOmClass() {
        return omClass;
    }

    public void setOmClass(Class<? extends ObjectMapper> omClass) {
        this.omClass = omClass;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public int getGlobalW() {
        return globalW;
    }

    public void setGlobalW(int globalW) {
        this.globalW = globalW;
    }

    public boolean isGlobalJ() {
        return globalJ;
    }

    public void setGlobalJ(boolean globalJ) {
        this.globalJ = globalJ;
    }

    public Class<? extends Query> getQueryClass() {
        if (queryClass == null) {
            queryClass = QueryImpl.class;
        }
        return queryClass;
    }

    public void setQueryClass(Class<Query> queryClass) {
        this.queryClass = queryClass;
    }

    public QueryFactory getQueryFact() {
        if (queryFact == null) {
            queryFact = new QueryFactoryImpl(getQueryClass());
        }
        return queryFact;
    }

    public void setQueryFact(QueryFactory queryFact) {
        this.queryFact = queryFact;
    }

    public AggregatorFactory getAggregatorFactory() {
        if (aggregatorFactory == null) {
            aggregatorFactory = new AggregatorFactoryImpl(getAggregatorClass());
        }
        return aggregatorFactory;
    }

    public void setAggregatorFactory(AggregatorFactory aggregatorFactory) {
        this.aggregatorFactory = aggregatorFactory;
    }

    public Class<? extends Aggregator> getAggregatorClass() {
        if (aggregatorClass == null) {
            aggregatorClass = AggregatorImpl.class;
        }
        return aggregatorClass;
    }

    public void setAggregatorClass(Class<? extends Aggregator> aggregatorClass) {
        this.aggregatorClass = aggregatorClass;
    }

    public boolean isGlobalFsync() {
        return globalFsync;
    }

    public void setGlobalFsync(boolean globalFsync) {
        this.globalFsync = globalFsync;
    }

    public boolean isSafeMode() {
        return safeMode;
    }

    public void setSafeMode(boolean safeMode) {
        this.safeMode = safeMode;
    }

    public int getBlockingThreadsMultiplier() {
        return blockingThreadsMultiplier;
    }

    public void setBlockingThreadsMultiplier(int blockingThreadsMultiplier) {
        this.blockingThreadsMultiplier = blockingThreadsMultiplier;
    }

    public MorphiumWriter getBufferedWriter() {
        if (bufferedWriter == null) {
            bufferedWriter = new BufferedMorphiumWriterImpl();
        }
        return bufferedWriter;

    }

    public void setBufferedWriter(MorphiumWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
    }

    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public MorphiumWriter getWriter() {
        if (writer == null) {
            writer = new MorphiumWriterImpl();
        }
        return writer;
    }

    public void setWriter(MorphiumWriter writer) {
        this.writer = writer;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Class<? extends MongoField> getFieldImplClass() {
        return fieldImplClass;
    }

    public void setFieldImplClass(Class<? extends MongoField> fieldImplClass) {
        this.fieldImplClass = fieldImplClass;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public boolean isAutoreconnect() {
        return autoreconnect;
    }

    public void setAutoreconnect(boolean autoreconnect) {
        this.autoreconnect = autoreconnect;
    }

    public int getMaxAutoReconnectTime() {
        return maxAutoReconnectTime;
    }

    public void setMaxAutoReconnectTime(int maxAutoReconnectTime) {
        this.maxAutoReconnectTime = maxAutoReconnectTime;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public String getMongoLogin() {
        return mongoLogin;
    }

    public void setMongoLogin(String mongoLogin) {
        this.mongoLogin = mongoLogin;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    public void setMongoPassword(String mongoPassword) {
        this.mongoPassword = mongoPassword;
    }

    public ReadPreferenceLevel getDefaultReadPreference() {
        return defaultReadPreference;
    }

    public void setDefaultReadPreference(ReadPreferenceLevel defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
    }

    public String getMongoAdminUser() {
        return mongoAdminUser;
    }

    public void setMongoAdminUser(String mongoAdminUser) {
        this.mongoAdminUser = mongoAdminUser;
    }

    public String getMongoAdminPwd() {
        return mongoAdminPwd;
    }

    public void setMongoAdminPwd(String mongoAdminPwd) {
        this.mongoAdminPwd = mongoAdminPwd;
    }

    public int getWriteCacheTimeout() {
        return writeCacheTimeout;
    }

    public void setWriteCacheTimeout(int writeCacheTimeout) {
        this.writeCacheTimeout = writeCacheTimeout;
    }

    public List<ServerAddress> getAdr() {
        return adr;
    }

    /**
     * add addresses to your servers here. Depending on isREplicaSet() and isPaired() one ore more server addresses are needed
     */
    public void setAdr(List<ServerAddress> adr) {
        this.adr = adr;
    }

    /**
     * setting hosts as Host:Port
     *
     * @param str list of hosts, with or without port
     */
    public void setHosts(List<String> str) throws UnknownHostException {
        adr.clear();

        for (String s : str) {
            s = s.replaceAll(" ", "");
            String[] h = s.split(":");
            if (h.length == 1) {
                addHost(h[0], 27017);
            } else {
                addHost(h[0], Integer.parseInt(h[1]));
            }
        }
    }

    public void setHosts(List<String> str, List<Integer> ports) throws UnknownHostException {
        adr.clear();
        for (int i = 0; i < str.size(); i++) {
            String host = str.get(i).replaceAll(" ", "");
            if (ports.size() < i) {
                addHost(host, 27017);
            } else {
                addHost(host, ports.get(i));
            }
        }
    }

    public void setHosts(String hostPorts) throws UnknownHostException {
        adr.clear();
        String h[] = hostPorts.split(",");
        for (String host : h) {
            addHost(host);
        }
    }

    public void setHosts(String hosts, String ports) throws UnknownHostException {
        adr.clear();
        hosts = hosts.replaceAll(" ", "");
        ports = ports.replaceAll(" ", "");
        String h[] = hosts.split(",");
        String p[] = ports.split(",");
        for (int i = 0; i < h.length; i++) {
            if (p.length < i) {
                addHost(h[i], 27017);
            } else {
                addHost(h[i], Integer.parseInt(p[i]));
            }
        }

    }

    /**
     * add addresses to your servers here. Depending on isREplicaSet() and isPaired() one ore more server addresses are needed
     * use addHost instead
     */
    @Deprecated
    public void addAddress(String host, int port) throws UnknownHostException {
        addHost(host, port);
    }

    public void addHost(String host, int port) throws UnknownHostException {
        host = host.replaceAll(" ", "");
        ServerAddress sa = new ServerAddress(host, port);
        adr.add(sa);
    }

    /**
     * use addhost instead
     *
     * @param host
     * @throws UnknownHostException
     */
    @Deprecated
    public void addAddress(String host) throws UnknownHostException {
        addHost(host);
    }

    public void addHost(String host) throws UnknownHostException {
        host = host.replaceAll(" ", "");
        if (host.contains(":")) {
            String[] h = host.split(":");
            addHost(h[0], Integer.parseInt(h[1]));
        } else {
            addHost(host, 27017);
        }
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * for future use - set Global Caching time
     *
     * @return the global cache valid time
     */
    public int getGlobalCacheValidTime() {
        return globalCacheValidTime;
    }

    public void setGlobalCacheValidTime(int globalCacheValidTime) {
        this.globalCacheValidTime = globalCacheValidTime;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getHousekeepingTimeout() {
        return housekeepingTimeout;
    }

    public void setHousekeepingTimeout(int housekeepingTimeout) {
        this.housekeepingTimeout = housekeepingTimeout;
    }

    public long getValidTime() {
        return globalCacheValidTime;
    }

    public void setValidTime(int tm) {
        globalCacheValidTime = tm;
    }

    /**
     * returns json representation of this object containing all values
     *
     * @return json string
     */
    @Override
    public String toString() {
        updateAdditionals();
        try {
            return getOmClass().newInstance().marshall(this).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateAdditionals() {
        restoreData = new HashMap<String, String>();
        addClassSettingsTo(restoreData);


    }

    private void addClassSettingsTo(Map p) {
        MorphiumConfig defaults = new MorphiumConfig();
        getWriter();
        getBufferedWriter();
        getAsyncWriter();

        if (!defaults.getWriter().getClass().equals(getWriter().getClass())) {
            p.put("writer_I_ClassName", getWriter().getClass().getName());
        }
        if (!defaults.getBufferedWriter().getClass().equals(getBufferedWriter().getClass())) {
            p.put("bufferedWriter_I_ClassName", getBufferedWriter().getClass().getName());
        }
        if (!defaults.getAsyncWriter().getClass().equals(getAsyncWriter().getClass())) {
            p.put("asyncWriter_I_ClassName", getAsyncWriter().getClass().getName());
        }
        if (!defaults.getCache().getClass().equals(getCache().getClass())) {
            p.put("cache_I_ClassName", getCache().getClass().getName());
        }
        if (!defaults.getAggregatorClass().equals(getAggregatorClass())) {
            p.put("aggregatorClass_C_ClassName", getAggregatorClass().getName());
        }
        if (!defaults.getAggregatorFactory().getClass().equals(getAggregatorFactory().getClass())) {
            p.put("aggregatorFactory_I_ClassName", getAggregatorFactory().getClass().getName());
        }
        if (!defaults.getOmClass().equals(getOmClass())) {
            p.put("omClass_C_ClassName", getOmClass().getName());
        }
        if (!defaults.getQueryClass().equals(getQueryClass())) {
            p.put("queryClass_C_ClassName", getQueryClass().getName());
        }
        if (!defaults.getQueryFact().getClass().equals(getQueryFact().getClass())) {
            p.put("queryFact_I_ClassName", getQueryFact().getClass().getName());
        }
        StringBuilder b = new StringBuilder();
        String del = "";
        for (ServerAddress a : getAdr()) {
            b.append(del);
            b.append(a.getHost() + ":" + a.getPort());
            del = ", ";
        }
        p.put("hosts", b.toString());
    }

    public MorphiumWriter getAsyncWriter() {
        if (asyncWriter == null) {
            asyncWriter = new AsyncWriterImpl();
        }
        return asyncWriter;
    }

    public void setAsyncWriter(MorphiumWriter asyncWriter) {
        this.asyncWriter = asyncWriter;
    }

    public int getMaximumRetriesBufferedWriter() {
        return maximumRetriesBufferedWriter;
    }

    public void setMaximumRetriesBufferedWriter(int maximumRetriesBufferedWriter) {
        this.maximumRetriesBufferedWriter = maximumRetriesBufferedWriter;
    }

    public int getMaximumRetriesWriter() {
        return maximumRetriesWriter;
    }

    public void setMaximumRetriesWriter(int maximumRetriesWriter) {
        this.maximumRetriesWriter = maximumRetriesWriter;
    }

    public int getMaximumRetriesAsyncWriter() {
        return maximumRetriesAsyncWriter;
    }

    public void setMaximumRetriesAsyncWriter(int maximumRetriesAsyncWriter) {
        this.maximumRetriesAsyncWriter = maximumRetriesAsyncWriter;
    }

    public int getRetryWaitTimeBufferedWriter() {
        return retryWaitTimeBufferedWriter;
    }

    public void setRetryWaitTimeBufferedWriter(int retryWaitTimeBufferedWriter) {
        this.retryWaitTimeBufferedWriter = retryWaitTimeBufferedWriter;
    }

    public int getRetryWaitTimeWriter() {
        return retryWaitTimeWriter;
    }

    public void setRetryWaitTimeWriter(int retryWaitTimeWriter) {
        this.retryWaitTimeWriter = retryWaitTimeWriter;
    }

    public int getRetryWaitTimeAsyncWriter() {
        return retryWaitTimeAsyncWriter;
    }

    public void setRetryWaitTimeAsyncWriter(int retryWaitTimeAsyncWriter) {
        this.retryWaitTimeAsyncWriter = retryWaitTimeAsyncWriter;
    }


    /**
     * returns a property set only containing non-default values set
     *
     * @return
     */
    public Properties asProperties() {
        MorphiumConfig defaults = new MorphiumConfig();
        Properties p = new Properties();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        List<Field> flds = an.getAllFields(MorphiumConfig.class);
        for (Field f : flds) {
            if (f.isAnnotationPresent(Transient.class)) continue;
            f.setAccessible(true);
            try {
                if (f.get(this) != null && !f.get(this).equals(f.get(defaults))) {
                    p.put(f.getName(), f.get(this).toString());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        addClassSettingsTo(p);

        return p;
    }

    public static MorphiumConfig fromProperties(Properties p) throws ClassNotFoundException, NoSuchFieldException, InstantiationException, IllegalAccessException, UnknownHostException {
        return new MorphiumConfig(p);
    }

    public boolean isReadCacheEnabled() {
        return readCacheEnabled;
    }

    public void setReadCacheEnabled(boolean readCacheEnabled) {
        this.readCacheEnabled = readCacheEnabled;
    }

    public void disableReadCache() {
        this.readCacheEnabled = false;
    }

    public void enableReadCache() {
        this.readCacheEnabled = true;
    }

    public boolean isAsyncWritesEnabled() {
        return asyncWritesEnabled;
    }

    public void setAsyncWritesEnabled(boolean asyncWritesEnabled) {
        this.asyncWritesEnabled = asyncWritesEnabled;
    }

    public void disableAsyncWrites() {
        asyncWritesEnabled = false;
    }

    public void enableAsyncWrites() {
        asyncWritesEnabled = true;
    }

    public boolean isBufferedWritesEnabled() {
        return bufferedWritesEnabled;
    }

    public void setBufferedWritesEnabled(boolean bufferedWritesEnabled) {
        this.bufferedWritesEnabled = bufferedWritesEnabled;
    }

    public void disableBufferedWrites() {
        bufferedWritesEnabled = false;
    }

    public void enableBufferedWrites() {
        bufferedWritesEnabled = true;
    }

    public void setAutoValuesEnabled(boolean enabled) {
        autoValues = enabled;
    }

    public boolean isAutoValuesEnabled() {
        return autoValues;
    }

    public void enableAutoValues() {
        autoValues = true;
    }

    public void disableAutoValues() {
        autoValues = false;
    }

    public int getAcceptableLatencyDifference() {
        return acceptableLatencyDifference;
    }

    public void setAcceptableLatencyDifference(int acceptableLatencyDifference) {
        this.acceptableLatencyDifference = acceptableLatencyDifference;
    }

    public boolean isCamelCaseConversionEnabled() {
        return camelCaseConversionEnabled;
    }

    public void setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        this.camelCaseConversionEnabled = camelCaseConversionEnabled;
    }

    public int getThreadPoolMessagingCoreSize() {
        return threadPoolMessagingCoreSize;
    }

    public void setThreadPoolMessagingCoreSize(int threadPoolMessagingCoreSize) {
        this.threadPoolMessagingCoreSize = threadPoolMessagingCoreSize;
    }

    public int getThreadPoolMessagingMaxSize() {
        return threadPoolMessagingMaxSize;
    }

    public void setThreadPoolMessagingMaxSize(int threadPoolMessagingMaxSize) {
        this.threadPoolMessagingMaxSize = threadPoolMessagingMaxSize;
    }

    public long getThreadPoolMessagingKeepAliveTime() {
        return threadPoolMessagingKeepAliveTime;
    }

    public void setThreadPoolMessagingKeepAliveTime(long threadPoolMessagingKeepAliveTime) {
        this.threadPoolMessagingKeepAliveTime = threadPoolMessagingKeepAliveTime;
    }

    public int getThreadPoolAsyncOpCoreSize() {
        return threadPoolAsyncOpCoreSize;
    }

    public void setThreadPoolAsyncOpCoreSize(int threadPoolAsyncOpCoreSize) {
        this.threadPoolAsyncOpCoreSize = threadPoolAsyncOpCoreSize;
    }

    public int getThreadPoolAsyncOpMaxSize() {
        return threadPoolAsyncOpMaxSize;
    }

    public void setThreadPoolAsyncOpMaxSize(int threadPoolAsyncOpMaxSize) {
        this.threadPoolAsyncOpMaxSize = threadPoolAsyncOpMaxSize;
    }

    public long getThreadPoolAsyncOpKeepAliveTime() {
        return threadPoolAsyncOpKeepAliveTime;
    }

    public void setThreadPoolAsyncOpKeepAliveTime(long threadPoolAsyncOpKeepAliveTime) {
        this.threadPoolAsyncOpKeepAliveTime = threadPoolAsyncOpKeepAliveTime;
    }

    public boolean isObjectSerializationEnabled() {
        return objectSerializationEnabled;
    }

    public void setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        this.objectSerializationEnabled = objectSerializationEnabled;
    }

    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    public void setCursorFinalizerEnabled(boolean cursorFinalizerEnabled) {
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;
    }

    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    public void setAlwaysUseMBeans(boolean alwaysUseMBeans) {
        this.alwaysUseMBeans = alwaysUseMBeans;
    }

    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
    }

    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {
        this.heartbeatSocketTimeout = heartbeatSocketTimeout;
    }

    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    public void setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
    }

    public int getMinHearbeatFrequency() {
        return minHearbeatFrequency;
    }

    public void setMinHearbeatFrequency(int minHearbeatFrequency) {
        this.minHearbeatFrequency = minHearbeatFrequency;
    }

    public int getLocalThreashold() {
        return localThreashold;
    }

    public void setLocalThreashold(int localThreashold) {
        this.localThreashold = localThreashold;
    }

    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    public void setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    public void setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
    }

    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    public void setRequiredReplicaSetName(String requiredReplicaSetName) {
        this.requiredReplicaSetName = requiredReplicaSetName;
    }

    public int getGlobalLogLevel() {
        return globalLogLevel;
    }

    public void setGlobalLogLevel(int globalLogLevel) {
        this.globalLogLevel = globalLogLevel;
        System.getProperties().put("morphium.log.level", "" + globalLogLevel);
    }

    public boolean isGlobalLogSynced() {
        return globalLogSynced;
    }

    public void setGlobalLogSynced(boolean globalLogSynced) {
        this.globalLogSynced = globalLogSynced;
        System.getProperties().put("morphium.log.synced", "" + globalLogSynced);

    }

    public String getGlobalLogFile() {
        return globalLogFile;
    }

    public void setGlobalLogFile(String globalLogFile) {
        this.globalLogFile = globalLogFile;
        System.getProperties().put("morphium.log.file", globalLogFile);
    }

    public void setLogFileForClass(Class cls, String file) {
        setLogFileForPrefix(cls.getName(), file);
    }

    public void setLogFileForPrefix(String prf, String file) {
        System.getProperties().put("morphium.log.file." + prf, file);
    }

    public void setLogLevelForClass(Class cls, int level) {
        setLogLevelForPrefix(cls.getName(), level);
    }

    public void setLogLevelForPrefix(String cls, int level) {
        System.getProperties().put("morphium.log.level." + cls, level);
    }

    public void setLogSyncedForClass(Class cls, boolean synced) {
        setLogSyncedForPrefix(cls.getName(), synced);
    }

    public void setLogSyncedForPrefix(String cls, boolean synced) {
        System.getProperties().put("morphium.log.synced." + cls, synced);
    }
}