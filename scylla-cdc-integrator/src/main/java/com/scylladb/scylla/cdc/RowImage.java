package com.scylladb.scylla.cdc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.driver.core.AbstractGettableData;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.google.common.reflect.TypeToken;

public class RowImage extends AbstractGettableData implements GettableData {
    public static enum Type {
        PRE_IMAGE, UPDATE, INSERT, ROW_DELETE, PARTITION_DELETE, RANGE_DELETE_START_INCLUSIVE, RANGE_DELETE_START_EXCLUSIVE, RANGE_DELETE_END_INCLUSIVE, RANGE_DELETE_END_EXCLUSIVE, POST_IMAGE,
    }

    private final LogSession session;
    private final Row row;

    private final UUID timeUUID;
    private final Type type;
    private final int batchSequence;
    private final long ttl;

    RowImage(LogSession session, Row row, UUID timeuuid, Type type, int batchSequence, long ttl,
            ProtocolVersion protocolVersion) {
        super(protocolVersion);
        this.session = session;
        this.row = row;
        this.timeUUID = timeuuid;
        this.type = type;
        this.batchSequence = batchSequence;
        this.ttl = ttl;
    }

    private int toRow(int i) {
        return session.imageToQuery(i);
    }

    private int fromRow(int i) {
        return session.queryToImage(i);
    }

    public List<ColumnMetadata> getColumns() {
        return session.getImageColumns();
    }

    public List<ColumnMetadata> getPartitionKey() {
        return getColumns().subList(0, session.getBaseTable().getPartitionKey().size());
    }

    public List<ColumnMetadata> getPrimaryKey() {
        return getColumns().subList(0, session.getBaseTable().getPrimaryKey().size());
    }

    public List<ColumnMetadata> getClusteringColumns() {
        return getColumns().subList(0, session.getBaseTable().getClusteringColumns().size());
    }

    public List<ColumnMetadata> getSourceColumns() {
        return session.getBaseTable().getColumns();
    }

    public UUID getTimeUUID() {
        return timeUUID;
    }

    public int getBatchSequence() {
        return batchSequence;
    }

    public long getTTL() {
        return ttl;
    }

    @Override
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        if (!session.getDeltaMeta(i).isAtomic()) {
            return getListData(i, elementsType).values().stream().collect(Collectors.toList());
        }
        return super.getList(i, elementsType);
    }

    public <T> Map<UUID, T> getListData(int i, TypeToken<T> elementsType) {
        assert !session.getDeltaMeta(i).isAtomic();
        // the map returned from scylla is always ordered by uuid.
        return getMap(i, TypeToken.of(UUID.class), elementsType);
    }

    public <T> Map<UUID, T> getListData(int i, Class<T> elementsType) {
        return getListData(i, TypeToken.of(elementsType));
    }

    public <T> Map<UUID, T> getListData(String name, TypeToken<T> elementsType) {
        return getListData(getIndexOf(name), elementsType);
    }

    public <T> Map<UUID, T> getListData(String name, Class<T> elementsType) {
        return getListData(getIndexOf(name), TypeToken.of(elementsType));
    }

    @Override
    protected int getIndexOf(String name) {
        return fromRow(row.getColumnDefinitions().getIndexOf(name));
    }

    @Override
    protected DataType getType(int i) {
        return row.getColumnDefinitions().getType(toRow(i));
    }

    @Override
    protected String getName(int i) {
        return row.getColumnDefinitions().getName(toRow(i));
    }

    @Override
    protected ByteBuffer getValue(int i) {
        return row.getBytesUnsafe(toRow(i));
    }

    @Override
    protected CodecRegistry getCodecRegistry() {
        return session.getCodecRegistry();
    }

    public Type getType() {
        return type;
    }

    public boolean isDeleted(int columnIndex) {
        DeltaMetadata m = session.getDeltaMeta(columnIndex);
        if (!m.hasDeleteColumn()) {
            return false;
        }
        return row.getBool(m.getDeletedColumn());
    }

    public boolean isDeleted(String columnName) {
        return isDeleted(getIndexOf(columnName));
    }

    public boolean hasDeletedElements(int columnIndex) {
        DeltaMetadata m = session.getDeltaMeta(columnIndex);
        if (m.isAtomic()) {
            return false;
        }
        return !row.isNull(m.getDeletedElementsColumn());
    }

    public boolean hasDeletedElements(String name) {
        return hasDeletedElements(getIndexOf(name));
    }

    public Set<? extends UUID> getDeletedListElements(int columnIndex) {
        return getDeletedSetElements(columnIndex, UUID.class);
    }

    public <T> Set<T> getDeletedSetElements(int columnIndex, Class<T> type) {
        return getDeletedSetElements(columnIndex, TypeToken.of(type));
    }

    public <T> Set<T> getDeletedSetElements(int columnIndex, TypeToken<T> type) {
        DeltaMetadata m = session.getDeltaMeta(columnIndex);
        if (m.isAtomic() || row.isNull(m.getDeletedElementsColumn())) {
            return null;
        }
        return row.getSet(m.getDeletedElementsColumn(), type);
    }

    public <T> Set<T> getDeletedMapKeys(int columnIndex, Class<T> type) {
        return getDeletedMapKeys(columnIndex, TypeToken.of(type));
    }

    public <T> Set<T> getDeletedMapKeys(int columnIndex, TypeToken<T> keyType) {
        return getDeletedSetElements(columnIndex, keyType);
    }

    public Collection<? extends UUID> getDeletedListElements(String name) {
        return getDeletedListElements(getIndexOf(name));
    }

    public <T> Set<T> getDeletedSetElements(String name, Class<T> type) {
        return getDeletedSetElements(getIndexOf(name), type);
    }

    public <T> Set<T> getDeletedSetElements(String name, TypeToken<T> type) {
        return getDeletedSetElements(getIndexOf(name), type);
    }

    public <T> Set<T> getDeletedMapKeys(String name, Class<T> type) {
        return getDeletedMapKeys(getIndexOf(name), type);
    }

    public <T> Set<T> getDeletedMapKeys(String name, TypeToken<T> type) {
        return getDeletedMapKeys(getIndexOf(name), type);
    }

    public Set<Short> getDeletedUDTFields(int columnIndex) {
        return getDeletedSetElements(columnIndex, short.class);
    }

    public Set<Short> getDeletedUDTFields(String name) {
        return getDeletedSetElements(getIndexOf(name), short.class);
    }
}
