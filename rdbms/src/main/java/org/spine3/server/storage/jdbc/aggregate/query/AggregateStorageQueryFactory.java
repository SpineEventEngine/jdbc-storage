/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.spine3.server.storage.jdbc.aggregate.query;

import org.slf4j.Logger;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.entity.visibility.query.CreateVisibilityTableQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertAndMarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.SelectVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.UpdateVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable;
import org.spine3.server.storage.jdbc.query.AbstractQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.aggregate.query.Table.EventCount.EVENT_COUNT_TABLE_NAME_SUFFIX;

/**
 * This class creates queries for interaction with {@link Table.AggregateRecord} and {@link Table.EventCount}.
 *
 * @param <I> the type of IDs used in the storage
 * @author Andrey Lavrov
 */
public class AggregateStorageQueryFactory<I> extends AbstractQueryFactory {

    private final IdColumn<I> idColumn;
    private final String mainTableName;
    private final String eventCountTableName;
    private final DataSourceWrapper dataSource;

    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource     instance of {@link DataSourceWrapper}
     * @param aggregateClass aggregate class of corresponding {@link AggregateStorage} instance
     */
    @SuppressWarnings("unchecked")
    // The aux visibility handling query factory has a hardcoded type param
    public AggregateStorageQueryFactory(DataSourceWrapper dataSource,
                                        Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        super();
        this.idColumn = IdColumn.newInstance(aggregateClass);
        this.mainTableName = DbTableNameFactory.newTableName(aggregateClass);
        this.eventCountTableName = mainTableName + EVENT_COUNT_TABLE_NAME_SUFFIX;
        this.dataSource = dataSource;
    }

    public CreateVisibilityTableQuery newCreateVisibilityTableQuery() {
        final CreateVisibilityTableQuery.Builder builder =
                CreateVisibilityTableQuery.newBuilder()
                                          .setDataSource(dataSource)
                                          .setLogger(getLogger())
                                          .setTableName(VisibilityTable.TABLE_NAME);
        return builder.build();
    }

    public InsertVisibilityQuery newInsertVisibilityQuery(I id, Visibility visibility) {
        final InsertVisibilityQuery.Builder builder =
                InsertVisibilityQuery.newBuilder()
                                     .setDataSource(dataSource)
                                     .setLogger(getLogger())
                                     .setId(id)
                                     .setVisibility(visibility);
        return builder.build();
    }

    public SelectVisibilityQuery newSelectVisibilityQuery(I id) {
        final SelectVisibilityQuery.Builder builder =
                SelectVisibilityQuery.newBuilder()
                                     .setDataSource(dataSource)
                                     .setLogger(getLogger())
                                     .setId(id);
        return builder.build();
    }

    public UpdateVisibilityQuery newUpdateVisibilityQuery(I id, Visibility visibility) {
        final UpdateVisibilityQuery.Builder builder =
                UpdateVisibilityQuery.<I>newBuilder()
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setId(id)
                        .setVisibility(visibility);
        return builder.build();
    }

    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, archived);
    }

    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, deleted);
    }

    public InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, archived);
    }

    public InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, deleted);
    }

    private InsertAndMarkEntityQuery<I> newInsertAndMarkEntityQuery(I id, VisibilityField column) {
        final InsertAndMarkEntityQuery<I> query = InsertAndMarkEntityQuery.<I>newInsertBuilder()
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setTableName(VisibilityTable.TABLE_NAME)
                .setColumn(column)
                .setIdColumn(idColumn)
                .setId(id)
                .build();
        return query;
    }

    private MarkEntityQuery<I> newMarkQuery(I id, VisibilityField column) {
        final MarkEntityQuery<I> query = MarkEntityQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setTableName(VisibilityTable.TABLE_NAME)
                .setColumn(column)
                .setIdColumn(idColumn)
                .setId(id)
                .build();
        return query;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = checkNotNull(logger);
    }

    public Logger getLogger() {
        return logger;
    }

    /** Returns a query that creates a new {@link Table.AggregateRecord} if it does not exist. */
    public CreateMainTableQuery newCreateMainTableQuery() {
        final CreateMainTableQuery.Builder<I> builder = CreateMainTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setTableName(mainTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /** Returns a query that creates a new {@link Table.EventCount} if it does not exist. */
    public CreateEventCountTableQuery newCreateEventCountTableQuery() {
        final CreateEventCountTableQuery.Builder<I> builder = CreateEventCountTableQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setTableName(eventCountTableName)
                .setIdColumn(idColumn);
        return builder.build();
    }

    /**
     * Returns a  query that inserts a new aggregate event count after the last snapshot
     * to the {@link Table.EventCount}.
     *
     * @param id    corresponding aggregate id
     * @param count event count
     */
    public InsertEventCountQuery newInsertEventCountQuery(I id, int count) {
        final InsertEventCountQuery.Builder<I> builder =
                InsertEventCountQuery.<I>newBuilder(eventCountTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id)
                        .setCount(count);
        return builder.build();
    }

    /**
     * Returns a query that updates aggregate event count in the {@link Table.EventCount}.
     *
     * @param id    corresponding aggregate id
     * @param count new event count
     */
    public UpdateEventCountQuery newUpdateEventCountQuery(I id, int count) {
        final UpdateEventCountQuery.Builder<I> builder =
                UpdateEventCountQuery.<I>newBuilder(eventCountTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id)
                        .setCount(count);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link AggregateEventRecord} to the {@link Table.AggregateRecord}.
     *
     * @param id     aggregate id
     * @param record new aggregate record
     */
    public InsertAggregateRecordQuery newInsertRecordQuery(I id, AggregateEventRecord record) {
        final InsertAggregateRecordQuery.Builder<I> builder =
                InsertAggregateRecordQuery.<I>newBuilder(mainTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id)
                        .setRecord(record);
        return builder.build();
    }

    /** Returns a query that selects event count by corresponding aggregate ID. */
    public SelectEventCountByIdQuery<I> newSelectEventCountByIdQuery(I id) {
        final SelectEventCountByIdQuery.Builder<I> builder =
                SelectEventCountByIdQuery.<I>newBuilder(eventCountTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id);
        return builder.build();
    }

    /** Returns a query that selects aggregate records by ID sorted by time descending. */
    @SuppressWarnings("InstanceMethodNamingConvention")
    public SelectByIdSortedByTimeDescQuery<I> newSelectByIdSortedByTimeDescQuery(I id) {
        final SelectByIdSortedByTimeDescQuery.Builder<I> builder =
                SelectByIdSortedByTimeDescQuery.<I>newBuilder(mainTableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id);
        return builder.build();
    }
}
