package de.caluga.morphium.messaging;

import de.caluga.morphium.Logger;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PreStore;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 15:45
 * <p/>
 * Message class - used by Morphium's own messaging system<br>
 * </br>
 * Reads from any node, as this produces lots of reads! All Writes will block until <b>all nodes</b> have confirmed the
 * write!
 */
@Entity
@NoCache
//timeout <0 - setting relative to replication lag
//timeout == 0 - wait forever
@WriteSafety(level = SafetyLevel.NORMAL, timeout = 0, waitForJournalCommit = false, waitForSync = false)
@DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
@Lifecycle
@Index({"sender,locked_by,processed_by,recipient,-timestamp", "locked_by,processed_by,recipient,timestamp"})
public class Msg {


    public static enum Fields {
        processedBy,
        lockedBy,
        msgId,
        locked,
        type,
        inAnswerTo,
        msg,
        additional,
        value,
        timestamp,
        sender,
        ttl,
        recipient
    }

    @Index
    private List<String> processedBy;
    @Id
    private ObjectId msgId;
    @Index
    private String lockedBy;
    @Index
    private long locked;
    private MsgType type;
    private long ttl;
    private String sender;
    private String senderHost;
    private String recipient;
    @Transient
    private List<String> to;
    private Object inAnswerTo;
    //payload goes here
    private String name;
    private String msg;
    private List<Object> additional;
    private Map<String, Object> mapValue;
    private String value;
    @Index
    private long timestamp;

    @Index(options = "expireAfterSeconds:0")
    private Date deleteAt;
    @Transient
    private Boolean exclusive = null;

    public Msg() {
        // msgId = UUID.randomUUID().toString();
        lockedBy = "ALL";
        exclusive = false;
    }

    public Msg(String name, String msg, String value) {
        this(name, MsgType.MULTI, msg, value, 30000);
    }

    public Msg(String name, MsgType t, String msg, String value, long ttl) {
        this();
        this.name = name;
        this.msg = msg;
        this.value = value;
        this.type = t;
        this.ttl = ttl;
    }

    public boolean isExclusive() {
        if (exclusive == null) {
            return getLockedBy() != null && !getLockedBy().equals("ALL");
        }
        return exclusive;
    }

    /**
     * if true (default) message can only be processed by one system at a time
     *
     * @param exclusive
     */
    public void setExclusive(boolean exclusive) {
        if (!exclusive) lockedBy = "ALL";
        else lockedBy = null;
        this.exclusive = exclusive;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getSenderHost() {
        return senderHost;
    }

    public void setSenderHost(String senderHost) {
        this.senderHost = senderHost;
    }

    public Date getDeleteAt() {
        return deleteAt;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public void addRecipient(String id) {
        if (to == null) {
            to = new ArrayList<String>();

        }
        if (!to.contains(id)) {
            to.add(id);
        }
    }

    public void removeRecipient(String id) {
        if (to == null) {
            return;

        } else {
            to.remove(id);
        }
    }

    public void addValue(String key, Object value) {
        if (mapValue == null) {
            mapValue = new HashMap<String, Object>();
        }
        mapValue.put(key, value);
    }

    public void removeValue(String key) {
        if (mapValue == null) return;
        mapValue.remove(key);
    }

    public Map<String, Object> getMapValue() {
        return mapValue;
    }

    public void setMapValue(Map<String, Object> mapValue) {
        this.mapValue = mapValue;
    }

    public List<String> getTo() {
        return to;
    }

    public void setTo(List<String> to) {
        this.to = to;
    }

    public Object getInAnswerTo() {
        return inAnswerTo;
    }

    public void setInAnswerTo(Object inAnswerTo) {
        this.inAnswerTo = inAnswerTo;
    }

    public ObjectId getMsgId() {
        return msgId;
    }

    public void setMsgId(ObjectId msgId) {
        this.msgId = msgId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getProcessedBy() {
        return processedBy;
    }

    public void setProcessedBy(List<String> processedBy) {
        this.processedBy = processedBy;
    }

    public void addProcessedId(String id) {
        if (processedBy == null) {
            processedBy = new ArrayList<String>();
        }
        processedBy.add(id);
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public long getLocked() {
        return locked;
    }

    public void setLocked(long locked) {
        this.locked = locked;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public MsgType getType() {
        return type;
    }

    public void setType(MsgType type) {
        this.type = type;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<Object> getAdditional() {
        return additional;
    }

    public void setAdditional(List<Object> additional) {
        this.additional = additional;
    }

    public void addAdditional(String value) {
        if (additional == null) {
            additional = new ArrayList<Object>();
        }
        additional.add(value);
    }

    public void removeAdditional(String value) {
        if (additional == null) return;
        additional.remove(value);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Msg{" +
                " msgId='" + msgId + '\'' +
                ", inAnswerTo='" + inAnswerTo + '\'' +
                ", lockedBy='" + lockedBy + '\'' +
                ", locked=" + locked +
                ", type=" + type +
                ", ttl=" + ttl +
                ", sender='" + sender + '\'' +
                ", name='" + name + '\'' +
                ", msg='" + msg + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", additional='" + additional + '\'' +
                ", mapValue='" + mapValue + '\'' +
                ", recipient='" + recipient + '\'' +
                ", to_list='" + to + '\'' +

                ", processedBy=" + processedBy +
                '}';
    }

    @PreStore
    public void preStore() {
        if (sender == null) {
            throw new RuntimeException("Cannot send msg anonymously - set Sender first!");
        }
        if (type == null) {
            new Logger(Msg.class).warn("Messagetype not set - using SINGLE");
            type = MsgType.SINGLE;
        }
        if (name == null) {
            throw new RuntimeException("Cannot send a message without name!");
        }
        if (ttl == 0) {
            new Logger(Msg.class).warn("Defaulting msg ttl to 30sec");
            ttl = 30000;
        }
        if (!exclusive) {
            locked = System.currentTimeMillis();
            lockedBy = "ALL";
        }
        timestamp = System.currentTimeMillis();
    }


    public Msg createAnswerMsg() {
        Msg ret = new Msg(name, type, msg, value, ttl);
        ret.setInAnswerTo(msgId);
        ret.addRecipient(sender);
        return ret;
    }

    public void sendAnswer(Messaging messaging, Msg m) {
        m.setInAnswerTo(this.msgId);
        m.addRecipient(this.getSender());
        m.setDeleteAt(new Date(System.currentTimeMillis() + m.getTtl()));
        messaging.queueMessage(m);
    }

    public Msg getCopy() {
        Msg ret = new Msg();
        ret.setAdditional(additional);
        ret.setExclusive(exclusive);
        ret.setInAnswerTo(inAnswerTo);
        ret.setLocked(locked);
        ret.setLockedBy(lockedBy);
        ret.setName(name);
        ret.setProcessedBy(processedBy);
        ret.setSender(sender);
        ret.setRecipient(recipient);
        ret.setTimestamp(timestamp);
        ret.setTtl(ttl);
        ret.setType(type);
        ret.setValue(value);
        ret.setMsg(msg);
//        ret.setMsgId();
        ret.setMapValue(mapValue);
        ret.setTo(to);

        return ret;  //To change body of created methods use File | Settings | File Templates.
    }

    public void setDeleteAt(Date deleteAt) {
        this.deleteAt = deleteAt;
    }
}
