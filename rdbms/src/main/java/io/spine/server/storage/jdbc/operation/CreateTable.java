/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.operation;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.client.ArchivedColumn;
import io.spine.client.DeletedColumn;
import io.spine.client.VersionColumn;
import io.spine.logging.WithLogging;
import io.spine.query.Column;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.QueryExecutor;
import io.spine.server.storage.jdbc.record.RecordTable;
import org.checkerframework.checker.nullness.qual.NonNull;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static io.spine.server.storage.jdbc.Sql.Query.DEFAULT;
import static io.spine.server.storage.jdbc.Sql.Query.NOT;
import static io.spine.server.storage.jdbc.Sql.Query.NULL;
import static io.spine.server.storage.jdbc.Sql.Query.PRIMARY_KEY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Creates a table in the database.
 *
 * @param <I>
 *         the type of the identifiers of the stored records
 * @param <R>
 *         the type of the stored records
 */
public class CreateTable<I, R extends Message> extends Operation<I, R> implements WithLogging {

    /**
     * A map of the Spine common Entity Columns to their default values.
     *
     * <p>Some write operations may not include these columns. Though, they are required for
     * the framework to work properly. Hence, the tables which include them should make these
     * values {@code DEFAULT} for these columns.
     *
     * <p>The map stores the names of the Entity Columns as a string keys for simplicity and
     * the default values of the Columns as the map values.
     */
    private static final ImmutableMap<String, Object> COLUMN_DEFAULTS =
            ImmutableMap.of(archivedColumnName(), false,
                            deletedColumnName(), false,
                            versionColumnName(), 0);

    private final TypeMapping typeMapping;

    /**
     * Creates a new operation.
     *
     * @param table
     *         a description of the table to create
     * @param ds
     *         a data source to use for connectivity with the database instance
     * @param mapping
     *         the mapping of generic SQL types to the types used in a particular storage engine
     */
    @SuppressWarnings("WeakerAccess" /* Available to SPI users. */)
    public CreateTable(RecordTable<I, R> table, DataSourceWrapper ds, TypeMapping mapping) {
        super(table, ds);
        this.typeMapping = mapping;
    }

    /**
     * Executes the operation.
     */
    public void execute() {
        var queryExecutor = new QueryExecutor(dataSource(), logger());
        var createTableSql = sqlStatement();
        queryExecutor.execute(createTableSql);
    }

    /**
     * Composes an SQL statement for this operation.
     */
    @Internal
    public String sqlStatement() {
        var sql = beginStatement();

        var primaryKeyColumnName = addId(sql);
        var columns = table().spec().dataColumns();
        for (var column : columns) {
            addColumn(sql, column);
        }
        declarePrimaryKey(sql, primaryKeyColumnName);

        closeStatement(sql);
        var result = sql.toString();
        return result;
    }

    private static void closeStatement(StringBuilder sql) {
        sql.append(BRACKET_CLOSE)
           .append(SEMICOLON);
    }

    @NonNull
    private StringBuilder beginStatement() {
        var tableName = tableName();
        var capacity = CREATE_IF_MISSING.toString().length() +
                tableName.length() +
                BRACKET_OPEN.toString().length();

        var sql = new StringBuilder(capacity);
        sql.append(CREATE_IF_MISSING)
           .append(tableName)
           .append(BRACKET_OPEN);
        return sql;
    }

    private static void declarePrimaryKey(StringBuilder sql, String primaryKeyColumnName) {
        sql.append(PRIMARY_KEY)
           .append(BRACKET_OPEN)
           .append(primaryKeyColumnName)
           .append(BRACKET_CLOSE);
    }

    private void addColumn(StringBuilder sql, TableColumn column) {
        var name = column.name();
        var type = column.type();
        requireNonNull(type,
                       () -> format("The name of `%s` column is required at the table creation.",
                                    name));
        var typeName = typeMapping.typeNameFor(type);
        sql.append(name)
           .append(' ')
           .append(typeName);
        if (COLUMN_DEFAULTS.containsKey(name)) {
            var defaultValue = COLUMN_DEFAULTS.get(name);
            sql.append(DEFAULT)
               .append(defaultValue)
               .append(NOT)
               .append(NULL);
        }
        sql.append(COMMA);
    }

    private String addId(StringBuilder sql) {
        var idColumn = table().idColumn();

        var name = idColumn.columnName();
        var sqlType = idColumn.sqlType();
        var typeName = typeMapping.typeNameFor(sqlType);
        sql.append(name)
           .append(' ')
           .append(typeName)
           .append(NOT)
           .append(NULL);
        sql.append(COMMA);

        return name;
    }

    private static String versionColumnName() {
        return rawNameOf(VersionColumn.instance());
    }

    private static String deletedColumnName() {
        return rawNameOf(DeletedColumn.instance());
    }

    private static String archivedColumnName() {
        return rawNameOf(ArchivedColumn.instance());
    }

    private static String rawNameOf(Column<?, ?> instance) {
        return instance
                .name()
                .value();
    }
}
