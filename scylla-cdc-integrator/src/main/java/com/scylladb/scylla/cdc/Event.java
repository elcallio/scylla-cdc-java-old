package com.scylladb.scylla.cdc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;

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

    void addRow(LogSession session, Row r) {
        int op = r.getByte(session.getOperationColumnIndex());
        int batchSeq = r.getInt(session.getBatchSequenceColumnIndex());
        long ttl = r.getLong(session.getTtlColumnIndex());
        rows.add(new RowImage(session, r, timeUUID, RowImage.Type.values()[op], batchSeq, ttl,
                session.getProtocolVersion()));
    };
}
