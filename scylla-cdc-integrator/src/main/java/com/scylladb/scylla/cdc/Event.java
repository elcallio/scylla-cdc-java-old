package com.scylladb.scylla.cdc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;

/**
 * A CDC event. Typically a set of change rows that represent one or more
 * changes made to the original table in a single query.
 * 
 * Groups {@link RowImage} objects by timestamp
 * 
 * @author calle
 *
 */
public class Event {
    private final UUID timeUUID;
    private List<RowImage> rows = new ArrayList<>();

    Event(UUID timestamp) {
        this.timeUUID = timestamp;
    }

    public RowImage getPreImage() {
        return rows.stream().filter((r) -> r.getType() == RowImage.Type.PRE_IMAGE).findFirst().orElse(null);
    }

    public RowImage getPostImage() {
        return rows.stream().filter((r) -> r.getType() == RowImage.Type.POST_IMAGE).findFirst().orElse(null);
    }

    public List<RowImage> getRows() {
        return rows;
    }

    public UUID getTimeUUID() {
        return timeUUID;
    }

    public Instant getTimestamp() {
        return Instant.ofEpochMilli(UUIDs.unixTimestamp(timeUUID));
    }

    public StreamPosition position() {
        return new StreamPosition(getTimeUUID());
    }

    void addRow(LogSession session, Row r) {
        int op = r.getByte(session.getOperationColumnIndex());
        int batchSeq = r.getInt(session.getBatchSequenceColumnIndex());
        long ttl = r.getLong(session.getTtlColumnIndex());
        rows.add(new RowImage(session, r, timeUUID, RowImage.Type.values()[op], batchSeq, ttl,
                session.getProtocolVersion()));
    };
}
