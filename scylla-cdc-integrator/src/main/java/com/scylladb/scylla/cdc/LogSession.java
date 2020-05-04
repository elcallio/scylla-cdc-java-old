package com.scylladb.scylla.cdc;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.unmodifiableList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * CDC config + factory for a single source table.
 * 
 * Provides metadata and factory methods to access cdc log.
 * 
 * @author calle
 *
 */
public class LogSession {
    private static final String TABLE_NAME_SUFFIX = "_scylla_cdc_log";

    private static String META_COLUMN_PREFIX = "cdc$";
    private static final String DELETED_COLUMN_PREFIX = META_COLUMN_PREFIX + "deleted_";
    private static final String DELETED_ELEMENTS_PREFIX = META_COLUMN_PREFIX + "deleted_elements_";

    private final Session session;
    private final TableMetadata baseTable;
    private final TableMetadata logTable;
    private final List<ColumnMetadata> logColumns, queryColumns, imageColumns;
    private final List<DeltaMetadata> deltaMetas;
    private final ColumnMetadata pkColumn, timeColumn, opColumn, batchIdColumn, ttlColumn;
    private final CodecRegistry codecRegistry;
    private final Duration streamPositionWindow;

    private final int timeColumnIndex, batchSequenceColumnIndex, operationColumnIndex, ttlColumnIndex;

    private final Map<Integer, Integer> imageToQuery, queryToImage;

    private final Streams streams;
    private final int maxStreamsInQuery;

    private ProtocolVersion protocolVersion;

    private static String logName(String baseName) {
        return baseName + TABLE_NAME_SUFFIX;
    }

    private static String metaName(String baseName) {
        return META_COLUMN_PREFIX + baseName;
    }

    private static final Map<String, BiConsumer<DeltaMetadata, Integer>> metas = new LinkedHashMap<>();

    {
        metas.put(DELETED_ELEMENTS_PREFIX, DeltaMetadata::setDeletedElementsColumn);
        metas.put(DELETED_COLUMN_PREFIX, DeltaMetadata::setDeletedColumn);
    }

    LogSession(Session session, TableMetadata baseTable, Duration d, int maxStreamsInQuery) {
        this(session, baseTable, baseTable.getKeyspace().getTable(logName(baseTable.getName())), d, maxStreamsInQuery);
    }

    LogSession(Session session, TableMetadata baseTable, TableMetadata logTable, Duration d, int maxStreamsInQuery) {
        this.session = session;
        this.baseTable = baseTable;
        this.logTable = logTable;
        this.streamPositionWindow = d;
        this.maxStreamsInQuery = maxStreamsInQuery;

        logColumns = unmodifiableList(logTable.getColumns());
        List<ColumnMetadata> imageColumns = new ArrayList<>();

        imageToQuery = new HashMap<>();
        queryToImage = new HashMap<>();

        assert logTable.getPartitionKey().size() == 1;

        pkColumn = logTable.getColumn(metaName("stream_id"));
        timeColumn = logTable.getColumn(metaName("time"));
        batchIdColumn = logTable.getColumn(metaName("batch_seq_no"));
        opColumn = logTable.getColumn(metaName("operation"));
        ttlColumn = logTable.getColumn(metaName("ttl"));

        this.queryColumns = unmodifiableList(logTable.getColumns());

        timeColumnIndex = queryColumns.indexOf(timeColumn);
        batchSequenceColumnIndex = queryColumns.indexOf(batchIdColumn);
        operationColumnIndex = queryColumns.indexOf(opColumn);
        ttlColumnIndex = queryColumns.indexOf(ttlColumn);

        assert timeColumn.getType() == DataType.timeuuid();

        Map<String, DeltaMetadata> deltaMetas = new LinkedHashMap<>();

        int qi = 0;

        for (ColumnMetadata c : queryColumns) {
            String name = c.getName();
            if (name.startsWith(META_COLUMN_PREFIX)) {
                for (Map.Entry<String, BiConsumer<DeltaMetadata, Integer>> e : metas.entrySet()) {
                    if (name.startsWith(e.getKey())) {
                        DeltaMetadata m = deltaMetas.computeIfAbsent(name.substring(e.getKey().length()),
                                (s) -> new DeltaMetadata(baseTable.getColumn(quoteIfNecessary(s))));
                        e.getValue().accept(m, qi);
                        break;
                    }
                }
            } else {
                imageToQuery.put(imageColumns.size(), qi);
                queryToImage.put(qi, imageColumns.size());
                imageColumns.add(c);
            }
            ++qi;
        }

        this.imageColumns = unmodifiableList(imageColumns);
        this.codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();
        this.deltaMetas = imageColumns.stream()
                .map((c) -> deltaMetas.computeIfAbsent(c.getName(), (s) -> new DeltaMetadata(baseTable.getColumn(s))))
                .collect(Collectors.toList());

        this.streams = new Streams(session);
    }

    int imageToQuery(int i) {
        return this.imageToQuery.getOrDefault(i, -1);
    }

    int queryToImage(int i) {
        return this.queryToImage.getOrDefault(i, -1);
    }

    String getLogKeyspace() {
        return logTable.getKeyspace().getName();
    }

    String getLogTablename() {
        return logTable.getName();
    }

    public List<ColumnMetadata> getImageColumns() {
        return imageColumns;
    }

    public List<ColumnMetadata> getImagePartitionKey() {
        return imageColumns.subList(0, baseTable.getPartitionKey().size());
    }

    public List<ColumnMetadata> getImagePrimaryKey() {
        return imageColumns.subList(0, baseTable.getPrimaryKey().size());
    }

    public List<ColumnMetadata> getImageClusteringColumns() {
        return imageColumns.subList(0, baseTable.getClusteringColumns().size());
    }

    List<ColumnMetadata> getLogColumns() {
        return logColumns;
    }

    List<ColumnMetadata> getQueryColumns() {
        return queryColumns;
    }

    ColumnMetadata getPkColumn() {
        return pkColumn;
    }

    ColumnMetadata getTimeColumn() {
        return timeColumn;
    }

    CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    public TableMetadata getBaseTable() {
        return baseTable;
    }

    public TableMetadata getLogTable() {
        return logTable;
    }

    Duration getStreamPositionWindow() {
        return streamPositionWindow;
    }

    int getFetchSize() {
        return 300;
    }

    int getRefetchSize() {
        return 100;
    }

    int getTimeColumnIndex() {
        return timeColumnIndex;
    }

    int getBatchSequenceColumnIndex() {
        return batchSequenceColumnIndex;
    }

    int getOperationColumnIndex() {
        return operationColumnIndex;
    }

    int getTtlColumnIndex() {
        return ttlColumnIndex;
    }

    DeltaMetadata getDeltaMeta(int i) {
        return deltaMetas.get(i);
    }

    ProtocolVersion getProtocolVersion() {
        if (protocolVersion == null) {
            protocolVersion = session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        }
        return protocolVersion;
    }

    int getMaxStreamsInQuery() {
        return maxStreamsInQuery;
    }

    public Streams getStreams() {
        return streams;
    }

    public Reader createReader() {
        return new Reader(session, this);
    }

    public static Builder builder(Session session) {
        return new Builder(session);
    }

    /**
     * Configures and builds a {@link LogSession}
     * 
     * @author calle
     *
     */
    public static class Builder {
        private final Session session;

        private KeyspaceMetadata keyspace;
        private TableMetadata table;
        private Duration streamPositionWindow = Duration.of(30, SECONDS);
        private int maxStreamsInQuery = 10;

        private Builder(Session session) {
            this.session = session;
        }

        /**
         * Sets source keyspace
         */
        public Builder withKeyspace(KeyspaceMetadata keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        /**
         * @see #withKeyspace(KeyspaceMetadata)
         */
        public Builder withKeyspace(String keyspace) {
            return withKeyspace(session.getCluster().getMetadata().getKeyspace(keyspace));
        }

        /**
         * Sets source table
         */
        public Builder withTable(TableMetadata table) {
            this.table = table;
            return this;
        }

        /** @see #withTable(TableMetadata) */
        public Builder withTable(String table) {
            return withTable(this.keyspace.getTable(table));
        }

        /**
         * Sets confidence window for CDC log queries. TODO: explain
         */
        public Builder withStreamPositionWindow(Duration d) {
            this.streamPositionWindow = d;
            return this;
        }

        /**
         * Sets max streams to query in a single select. I.e. "IN" limit in
         * cluster.
         */
        public Builder withMaxStreamsInQuery(int m) {
            maxStreamsInQuery = m;
            return this;
        }

        public LogSession build() {
            return new LogSession(session, table, streamPositionWindow, maxStreamsInQuery);
        }

    }
}
