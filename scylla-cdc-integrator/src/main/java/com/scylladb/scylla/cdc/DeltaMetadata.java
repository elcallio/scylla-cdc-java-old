package com.scylladb.scylla.cdc;

import com.datastax.driver.core.ColumnMetadata;

class DeltaMetadata {
    private final ColumnMetadata sourceColumn;

    private int deletedColumn = -1;
    private int deletedElementsColumn = -1;

    public DeltaMetadata(ColumnMetadata column) {
        if (column == null) {
            throw new IllegalArgumentException("null column");
        }
        this.sourceColumn = column;
    }

    public int getDeletedColumn() {
        return deletedColumn;
    }

    public void setDeletedColumn(int deletedColumn) {
        this.deletedColumn = deletedColumn;
    }

    public int getDeletedElementsColumn() {
        return deletedElementsColumn;
    }

    public void setDeletedElementsColumn(int deletedElementsColumn) {
        this.deletedElementsColumn = deletedElementsColumn;
    }

    public boolean isAtomic() {
        return deletedElementsColumn == -1;
    }

    public boolean hasDeleteColumn() {
        return deletedColumn != -1;
    }

    public ColumnMetadata getSourceColumn() {
        return sourceColumn;
    }
}