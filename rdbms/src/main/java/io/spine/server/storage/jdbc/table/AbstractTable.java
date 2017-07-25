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

package io.spine.server.storage.jdbc.table;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.entity.query.DeleteAllQuery;
import io.spine.server.storage.jdbc.query.ContainsQuery;
import io.spine.server.storage.jdbc.query.DeleteRecordQuery;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.SimpleQuery;
import io.spine.server.storage.jdbc.query.StorageIndexQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.EntityField.version;
import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
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
 * @param <I> type of ID of the records stored in the table
 * @param <R> type of the record stored in the table; must be a {@linkplain Message proto message}
 * @author Dmytro Dashenkov
 * @see TableColumn
 */
public abstract class AbstractTable<I, R extends Message, W> {

    /**
     * A map of the Spine common Entity Columns to their default values.
     *
     * <p>Some write operations may not include these columns. Though, they are required for
     * the framework to work properly. Hence, the tables which include them should make these values
     * {@code DEFAULT} for these columns.
     *
     * <p>The map stores the names of the Entity Columns as a string keys for simplicity and
     * the default values of the Columns as the map values.
     */
    private static final ImmutableMap<String, Object> COLUMN_DEFAULTS =
            ImmutableMap.<String, Object>of(archived.name(), false,
                                            deleted.name(), false,
                                            version.name(), 0);
    private final String name;

    private final IdColumn<I> idColumn;

    private final DataSourceWrapper dataSource;

    private ImmutableList<? extends TableColumn> columns;

    protected AbstractTable(String name,
                            IdColumn<I> idColumn,
                            DataSourceWrapper dataSource) {
        super();
        this.name = checkNotNull(name);
        this.idColumn = checkNotNull(idColumn);
        this.dataSource = checkNotNull(dataSource);
    }

    /**
     * Retrieves the enum object representing the table ID column.
     *
     * <p>Example:
     * <pre>
     *     {@code
     *     \@Override
     *      public Column getIdColumnDeclaration() {
     *          return Column.id;
     *      }
     *     }
     * </pre>
     */
    public abstract TableColumn getIdColumnDeclaration();

    /**
     * @return an instance of {@link ReadQueryFactory} which produces queries to the given table
     */
    protected abstract ReadQueryFactory<I, R> getReadQueryFactory();

    /**
     * @return an instance of {@link WriteQueryFactory} which produces queries to the given table
     */
    protected abstract WriteQueryFactory<I, W> getWriteQueryFactory();

    protected abstract List<? extends TableColumn> getTableColumns();

    /**
     * Creates current table in the database if it does not exist yet.
     *
     * <p>Equivalent to an SQL expression:
     * <p>{@code CREATE TABLE IF NOT EXISTS $TableName ( $Columns );}
     */
    public void createIfNotExists() {
        final String rawSql = composeCreateTableSql();
        final SimpleQuery query = SimpleQuery.newBuilder()
                                             .setDataSource(dataSource)
                                             .setLogger(log())
                                             .setQuery(rawSql)
                                             .build();
        query.execute();
    }

    /**
     * Checks if the table contains a record with given ID.
     *
     * @param id ID to check
     * @return {@code true} if there is a record with such ID in the table, {@code false} otherwise
     */
    public boolean containsRecord(I id) {
        final ContainsQuery<I> query = ContainsQuery.<I>newBuilder()
                                                    .setIdColumn(getIdColumn())
                                                    .setId(id)
                                                    .setTableName(getName())
                                                    .setKeyColumn(getIdColumnDeclaration())
                                                    .setDataSource(dataSource)
                                                    .setLogger(log())
                                                    .build();
        final boolean result = query.execute();
        return result;
    }

    /**
     * Reads a record with the given ID from the table.
     *
     * @param id ID to search by
     * @return table record or {@code null} if there is no record with given ID
     */
    @Nullable
    public R read(I id) {
        final SelectByIdQuery<I, R> query = composeSelectQuery(id);
        final R result = query.execute();
        return result;
    }

    /**
     * Performs a write operation on the table.
     *
     * <p>If the table {@linkplain #containsRecord(Object) contains} a record with the given ID,
     * the operation is treated as an {@code UPDATE}, otherwise - as an {@code INSERT}.
     *
     * @param id     ID to write the record under
     * @param record the record to write
     */
    public void write(I id, W record) {
        if (containsRecord(id)) {
            update(id, record);
        } else {
            insert(id, record);
        }
    }

    public Iterator<I> index() {
        final StorageIndexQuery<I> query = getReadQueryFactory().newIndexQuery();
        final Iterator<I> result = query.execute();
        return result;
    }

    protected void insert(I id, W record) {
        final WriteQuery query = composeInsertQuery(id, record);
        query.execute();
    }

    protected void update(I id, W record) {
        final WriteQuery query = composeUpdateQuery(id, record);
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
    protected final ImmutableList<? extends TableColumn> getColumns() {
        if (columns == null) {
            final List<? extends TableColumn> tableColumnsType = getTableColumns();
            columns = ImmutableList.copyOf(tableColumnsType);
        }
        return columns;
    }

    public String getName() {
        return name;
    }

    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    public DataSourceWrapper getDataSource() {
        return dataSource;
    }

    protected WriteQuery composeInsertQuery(I id, W record) {
        final WriteQuery query = getWriteQueryFactory().newInsertQuery(id, record);
        return query;
    }

    protected WriteQuery composeUpdateQuery(I id, W record) {
        final WriteQuery query = getWriteQueryFactory().newUpdateQuery(id, record);
        return query;
    }

    protected SelectByIdQuery<I, R> composeSelectQuery(I id) {
        final SelectByIdQuery<I, R> query = getReadQueryFactory().newSelectByIdQuery(id);
        return query;
    }

    private String composeCreateTableSql() {
        final Iterable<? extends TableColumn> columns = getColumns();
        final StringBuilder sql = new StringBuilder();
        sql.append(CREATE_IF_MISSING)
           .append(getName())
           .append(BRACKET_OPEN);
        final Set<String> primaryKey = new HashSet<>();
        for (TableColumn column : columns) {
            final String name = column.name();
            sql.append(name)
               .append(' ')
               .append(ensureType(column));
            if (COLUMN_DEFAULTS.containsKey(name)) {
                final Object defaultValue = COLUMN_DEFAULTS.get(name);
                sql.append(DEFAULT).append(defaultValue);
            }
            if (!column.isNullable()) {
                sql.append(NOT).append(NULL);
            }
            sql.append(COMMA);
            if (column.isPrimaryKey()) {
                primaryKey.add(name);
            }
        }
        if (!primaryKey.isEmpty()) {
            final String columnNames = Joiner.on(COMMA.toString())
                                             .join(primaryKey);
            sql.append(PRIMARY_KEY)
               .append(BRACKET_OPEN)
               .append(columnNames)
               .append(BRACKET_CLOSE);
        }
        sql.append(BRACKET_CLOSE)
           .append(SEMICOLON);
        final String result = sql.toString();
        return result;
    }

    private Sql.Type getIdType() {
        final Sql.Type idType = getIdColumn().getSqlType();
        return idType;
    }

    /**
     * For the ID column checks the {@link Sql.Type} and returns the type of the ID column if
     * the ID type is {@linkplain Sql.Type#ID}.
     *
     * <p>If the column does not represent an ID of the table, this method throws
     * an {@link IllegalStateException}.
     *
     * @param column the column the type of which will be checked
     * @return the SQL type of the column
     */
    private Sql.Type ensureType(TableColumn column) throws IllegalStateException {
        Sql.Type type = column.type();
        if (type == Sql.Type.ID) {
            if (column.equals(getIdColumnDeclaration())) {
                type = getIdType();
            } else {
                throw new IllegalStateException("ID type of a non-ID column " + column.name());
            }
        }
        return type;
    }

    /**
     * Deletes a row in the table corresponding to the given ID.
     *
     * @param id ID to search by
     * @return {@code true} if the row was deleted successfully, {@code false} if the row was
     * not found
     */
    public boolean delete(I id) {
        final DeleteRecordQuery<I> query =
                DeleteRecordQuery.<I>newBuilder()
                        .setTableName(getName())
                        .setIdColumn(getIdColumn())
                        .setIdValue(id)
                        .setLogger(log())
                        .setDataSource(dataSource)
                        .build();
        return query.execute();
    }

    /**
     * Deletes all the records from the table.
     */
    public void deleteAll() {
        final DeleteAllQuery query = DeleteAllQuery.newBuilder(getName())
                                                   .setDataSource(dataSource)
                                                   .setLogger(log())
                                                   .build();
        query.execute();
    }

    protected static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(AbstractTable.class);
    }
}
