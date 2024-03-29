package com.scylladb.scylla.cdc;

import static java.time.Instant.ofEpochMilli;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Represents a position in a CDC stream.
 * 
 * This is designated by the timestamp of the cdc row(s) that make up an
 * {@link Event}.
 * 
 * @author calle
 *
 */
public final class StreamPosition implements Serializable, Comparable<StreamPosition> {
    private static final long serialVersionUID = 6791841858615998893L;
    private final UUID position;

    StreamPosition(UUID position) {
        this.position = position;
    }

    StreamPosition(Event event) {
        this(event.getTimeUUID());
    }

    StreamPosition(Instant when) {
        this(UUIDs.startOf(when.toEpochMilli()));
    }

    public static final StreamPosition INITIAL = new StreamPosition(ofEpochMilli(0));

    UUID getPosition() {
        return position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StreamPosition)) {
            return false;
        }
        StreamPosition other = (StreamPosition) obj;
        return Objects.equals(position, other.position);
    }

    @Override
    public int compareTo(StreamPosition o) {
        return Long.compare(position.timestamp(), o.position.timestamp());
    }

}
