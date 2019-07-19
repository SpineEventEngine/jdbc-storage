/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.spine.annotation.Internal;
import io.spine.logging.Logging;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.type.TypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static io.spine.server.storage.jdbc.Sql.Query.DEFAULT;
import static io.spine.server.storage.jdbc.Sql.Query.NOT;
import static io.spine.server.storage.jdbc.Sql.Query.NULL;
import static io.spine.server.storage.jdbc.Sql.Query.PRIMARY_KEY;

/**
 * A representation of an SQL table.
 *
 * <p>This class is responsible for storing all the information about a table including:
 * <ul>
 *     <li>table name;
 *     <li>columns;
 *     <li>identifier and {@code PRIMARY KEY};
 *     <li>queries to the table.
 * </ul>
 *
 * <p>A table provides a sufficient API for performing the database interaction. However, it never
 * performs any validation or data transformation, but only invokes the appropriate queries.
 *
 * @param <I>
 *         a type of ID of the records stored in the table
 * @param <R>
 *         a result type of a read operation by a single ID
 * @param <W>
 *         a type of stored records
 * @see TableColumn
 */
@Internal
public abstract class AbstractTable<I, R, W> implements Logging {

    private final String name;
    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final TypeMapping typeMapping;

    /**
     * The memoized value of the table columns.
     *
     * <p>This field is effectively final but is initialized lazily.
     *
     * @see #columns() for the initialization
     */
    private ImmutableList<? extends TableColumn> columns;

    protected AbstractTable(String name, IdColumn<I> idColumn,
                            DataSourceWrapper dataSource, TypeMapping typeMapping) {
        this.name = checkNotNull(name);
        this.idColumn = checkNotNull(idColumn);
        this.dataSource = checkNotNull(dataSource);
        this.typeMapping = checkNotNull(typeMapping);
    }

    protected abstract List<? extends TableColumn> tableColumns();

    /**
     * Creates current table in the database if it does not exist yet.
     *
     * <p>Equivalent to an SQL expression:
     * <p>{@code CREATE TABLE IF NOT EXISTS $TableName ( $Columns );}
     */
    public void create() {
        QueryExecutor queryExecutor = new QueryExecutor(dataSource, log());
        String createTableSql = composeCreateTableSql();
        queryExecutor.execute(createTableSql);
    }

    /**
     * Checks if the table contains a record with given ID.
     *
     * @param id
     *         an ID to check
     * @return {@code true} if there is a record with such ID in the table, {@code false} otherwise
     */
    protected boolean containsRecord(I id) {
        ContainsQuery.Builder<I> builder = ContainsQuery.newBuilder();
        ContainsQuery<I> query = builder.setIdColumn(idColumn)
                                        .setId(id)
                                        .setTableName(name)
                                        .setDataSource(dataSource)
                                        .build();
        boolean result = query.execute();
        return result;
    }

    /**
     * Reads a record with the given ID from the table.
     *
     * @param id
     *         an ID to search by
     * @return table record or {@code null} if there is no record with given ID
     */
    public @Nullable R read(I id) {
        SelectQuery<R> query = composeSelectQuery(id);
        R result = query.execute();
        return result;
    }

    /**
     * Performs a write operation on the table.
     *
     * <p>If the table {@linkplain #containsRecord(Object) contains} a record with the given ID,
     * the operation is treated as an {@code UPDATE}, otherwise - as an {@code INSERT}.
     *
     * @param id
     *         an ID to write the record under
     * @param record
     *         the record to write
     */
    public void write(I id, W record) {
        if (containsRecord(id)) {
            update(id, record);
        } else {
            insert(id, record);
        }
    }

    /**
     * Retrieves the table index.
     *
     * @return an {@code Iterator} over the table IDs
     * @see io.spine.server.storage.Storage#index()
     */
    public Iterator<I> index() {
        StorageIndexQuery.Builder<I> builder = StorageIndexQuery.newBuilder();
        StorageIndexQuery<I> query = builder.setDataSource(dataSource)
                                            .setTableName(name)
                                            .setIdColumn(idColumn)
                                            .build();
        Iterator<I> result = query.execute();
        return result;
    }

    /**
     * Inserts the record into the table using the specified ID.
     *
     * @param id
     *         an ID for the record
     * @param record
     *         a record to insert
     */
    public void insert(I id, W record) {
        WriteQuery query = composeInsertQuery(id, record);
        query.execute();
    }

    /**
     * Updates the record with the specified ID for the table.
     *
     * @param id
     *         an ID of the record
     * @param record
     *         a new state of the record
     */
    public void update(I id, W record) {
        WriteQuery query = composeUpdateQuery(id, record);
        query.execute();
    }

    /**
     * Retrieves the columns of this table.
     *
     * <p>When called for the first time, this method initializes the list of the columns.
     *
     * @return the initialized {@link List} of the table columns
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField") // Returns immutable collection
    final ImmutableList<? extends TableColumn> columns() {
        if (columns == null) {
            List<? extends TableColumn> tableColumnsType = tableColumns();
            columns = ImmutableList.copyOf(tableColumnsType);
        }
        return columns;
    }

    /**
     * Obtains the map of column defaults for this table.
     */
    protected ImmutableMap<String, Object> columnDefaults() {
        return ImmutableMap.of();
    }

    protected String name() {
        return name;
    }

    protected IdColumn<I> idColumn() {
        return idColumn;
    }

    protected DataSourceWrapper dataSource() {
        return dataSource;
    }

    protected abstract WriteQuery composeInsertQuery(I id, W record);

    protected abstract WriteQuery composeUpdateQuery(I id, W record);

    protected abstract SelectQuery<R> composeSelectQuery(I id);

    private String composeCreateTableSql() {
        Iterable<? extends TableColumn> columns = columns();
        StringBuilder sql = new StringBuilder();
        sql.append(CREATE_IF_MISSING)
           .append(name())
           .append(BRACKET_OPEN);
        Set<String> primaryKey = new HashSet<>();
        for (Iterator<? extends TableColumn> iterator = columns.iterator(); iterator.hasNext(); ) {
            TableColumn column = iterator.next();
            String name = column.name();
            Type type = typeOf(column);
            TypeName typeName = typeMapping.typeNameFor(type);
            sql.append(name)
               .append(' ')
               .append(typeName);
            if (columnDefaults().containsKey(name)) {
                Object defaultValue = columnDefaults().get(name);
                sql.append(DEFAULT)
                   .append(defaultValue);
            }
            if (!column.isNullable()) {
                sql.append(NOT)
                   .append(NULL);
            }
            if (column.isPrimaryKey()) {
                primaryKey.add(name);
            }
            if (iterator.hasNext() || !primaryKey.isEmpty()) {
                sql.append(COMMA);
            }
        }
        if (!primaryKey.isEmpty()) {
            String columnNames = Joiner.on(COMMA.toString())
                                       .join(primaryKey);
            sql.append(PRIMARY_KEY)
               .append(BRACKET_OPEN)
               .append(columnNames)
               .append(BRACKET_CLOSE);
        }
        sql.append(BRACKET_CLOSE)
           .append(SEMICOLON);
        String result = sql.toString();
        return result;
    }

    /**
     * Obtains the type of the specified column.
     *
     * <p>If the column is an ID column, the correct type is obtained from its
     * {@linkplain IdColumn wrapper}, otherwise the column type is returned as-is.
     *
     * <p>It's also assumed that non-ID columns always have their SQL type set.
     */
    private Type typeOf(TableColumn column) {
        boolean isIdColumn = column.equals(idColumn.column());
        if (isIdColumn) {
            return idColumn.sqlType();
        }
        return checkNotNull(column.type());
    }

    /**
     * Deletes a row in the table corresponding to the given ID.
     *
     * @param id
     *         an ID to search by
     * @return {@code true} if the row was deleted successfully,
     *         {@code false} if the row was not found
     */
    public boolean delete(I id) {
        DeleteRecordQuery.Builder<I> builder = DeleteRecordQuery.newBuilder();
        DeleteRecordQuery<I> query = builder.setTableName(name)
                                            .setIdColumn(idColumn())
                                            .setId(id)
                                            .setDataSource(dataSource)
                                            .build();
        long rowsAffected = query.execute();
        return rowsAffected != 0;
    }

    /**
     * Deletes all the records from the table.
     */
    public void deleteAll() {
        DeleteAllQuery query = DeleteAllQuery.newBuilder()
                                             .setTableName(name)
                                             .setDataSource(dataSource)
                                             .build();
        query.execute();
    }
}
