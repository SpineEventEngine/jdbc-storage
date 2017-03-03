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

package org.spine3.server.storage.jdbc.table;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.ContainsQuery;
import org.spine3.server.storage.jdbc.query.VoidQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Query.PRIMARY_KEY;

/**
 * @author Dmytro Dashenkov.
 */
public abstract class AbstractTable<I, C extends Enum<C> & TableColumn> {

    private static final int DEFAULT_SQL_QUERY_LENGTH = 128;

    private final String name;

    private final IdColumn<I> idColumn;

    private final DataSourceWrapper dataSource;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ImmutableList<C> columns;

    protected AbstractTable(String name,
                            IdColumn<I> idColumn,
                            DataSourceWrapper dataSource) {
        super();
        this.name = name;
        this.idColumn = idColumn;
        this.dataSource = dataSource;
    }

    public abstract C getIdColumnDeclaration();

    protected abstract Class<C> getTableColumnType();

    public void createIfNotExists() {
        final String sql = composeCreateTableSql();
        final VoidQuery query = VoidQuery.newBuilder()
                                         .setDataSource(dataSource)
                                         .setLogger(log())
                                         .setQuery(sql)
                                         .build();
        query.execute();
    }

    protected boolean containsRecord(I id) {
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

    @SuppressWarnings("ReturnOfCollectionOrArrayField") // Returns immutable collection
    public ImmutableList<C> getColumns() {
        if (columns == null) {
            final Class<C> tableColumnsType = getTableColumnType();
            final C[] columnsArray = tableColumnsType.getEnumConstants();
            columns = ImmutableList.copyOf(columnsArray);
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

    protected Logger log() {
        return logger;
    }

    private String composeCreateTableSql() {
        final String idColumnName = getIdColumnDeclaration().name();
        @SuppressWarnings("StringBufferReplaceableByString")
        final StringBuilder sql = new StringBuilder(DEFAULT_SQL_QUERY_LENGTH);
        sql.append(CREATE_IF_MISSING)
           .append(getName())
           .append(BRACKET_OPEN);
        for (C column : getColumns()) {
            final String name = column.name();
            final Sql.Type type = ensureType(column);
            sql.append(name)
               .append(type)
               .append(COMMA);
            // Comma after the last column declaration is required since we add PRIMARY KEY after
        }
        sql.append(PRIMARY_KEY)
           .append(BRACKET_OPEN)
           .append(idColumnName)
           .append(BRACKET_CLOSE)
           .append(BRACKET_CLOSE)
           .append(SEMICOLON);
        final String result = sql.toString();
        return result;
    }

    private void fillDerectOrderParams(PreparedStatement sqlStatement, Object... queryParams)
            throws SQLException {
        final int paramsCount = queryParams.length;
        for (int i = 0; i < getColumns().size(); i++) {
            final C column = columns.get(i);
            final Object parameter = i < paramsCount
                                     ? queryParams[i]
                                     : null;
            setParameter(sqlStatement, column, parameter, i);
        }
    }

    private void fillParamsWithIdAtTheEnd(PreparedStatement sqlStatement, Object... queryParams)
            throws SQLException {
        final int paramsCount = queryParams.length;
        final String idColumnName = getIdColumnDeclaration().name();
        int position = 1;
        for (final C column : getColumns()) {
            if (column.name()
                      .equals(idColumnName)) {
                continue;
            }
            final Object parameter = position < paramsCount
                                     ? queryParams[position]
                                     : null;
            setParameter(sqlStatement, column, parameter, position);
            ++position;
        }

        @SuppressWarnings("unchecked")
        final I id = (I) queryParams[queryParams.length - 1];
        idColumn.setId(position, id, sqlStatement);
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // UNKNOWN case handled separately
    private void setParameter(PreparedStatement sqlStatement,
                              C column,
                              @Nullable Object value,
                              int position) throws SQLException {
        final Sql.Type type = ensureType(column);
        if (value == null) {
            final int sqlTypeIndex = type.getSqlType();
            sqlStatement.setNull(position, sqlTypeIndex);
            return;
        }
        switch (type) {
            case BLOB:
                final byte[] bytes = (byte[]) value;
                sqlStatement.setBytes(position, bytes);
                break;
            case INT:
                final int number = (int) value;
                sqlStatement.setInt(position, number);
                break;
            case BIGINT:
                final long longNumber = (long) value;
                sqlStatement.setLong(position, longNumber);
                break;
            case VARCHAR_512: // All VARCHAR types are Java Strings
            case VARCHAR_999:
                final String stringValue = (String) value;
                sqlStatement.setString(position, stringValue);
                break;
            case BOOLEAN:
                final boolean logicalValue = (boolean) value;
                sqlStatement.setBoolean(position, logicalValue);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unhandled SQL type \"" + type.toString() + '\"');
        }
    }

    private Sql.Type getIdType() {
        final Sql.Type idType = getIdColumn().getColumnDataType();
        return idType;
    }

    private Sql.Type ensureType(C column) {
        Sql.Type type = column.type();
        if (type == Sql.Type.UNKNOWN) {
            if (column == getIdColumnDeclaration()) {
                type = getIdType();
            } else {
                throw new IllegalStateException("UNKNOWN type of a non-ID column " + column.name());
            }
        }
        return type;
    }
}
