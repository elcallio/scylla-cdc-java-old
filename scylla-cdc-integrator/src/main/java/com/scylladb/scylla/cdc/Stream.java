package com.scylladb.scylla.cdc;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * A single CDC stream
 * 
 * @author calle
 *
 */
public class Stream {
    private final Date timestamp;
    private final ByteBuffer id;
    private Date expired;

    Stream(Date timestamp, ByteBuffer id) {
        this.timestamp = timestamp;
        this.id = id;
    }

    public Date getExpired() {
        return expired;
    }

    public boolean isExpired() {
        return expired != null;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getId() {
        return id;
    }

    void expire(Date expired) {
        if (this.expired == null) {
            this.expired = expired;
            // notify
        }
        assert this.expired.equals(expired);
    }
}
