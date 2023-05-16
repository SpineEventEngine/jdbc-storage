/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.ImmutableList;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.aggregate.LifecycleFlagsTable.Column.ID;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A table for storing the {@link LifecycleFlags} of an {@link Aggregate}.
 */
final class LifecycleFlagsTable<I> extends EntityTable<I, LifecycleFlags, LifecycleFlags> {

    private static final String TABLE_NAME_POSTFIX = "_visibility";

    LifecycleFlagsTable(Class<? extends Aggregate<I, ?, ?>> aggregateClass,
                        DataSourceWrapper dataSource,
                        TypeMapping typeMapping) {
        super(TABLE_NAME_POSTFIX, aggregateClass, ID, dataSource, typeMapping);
    }

    @Override
    protected List<? extends TableColumn> tableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    public void write(I id, LifecycleFlags record) {
        if (containsRecord(id)) {
            update(id, record);
        } else {
            insert(id, record);
        }
    }

    @Override
    protected SelectQuery<LifecycleFlags> composeSelectQuery(I id) {
        SelectLifecycleFlagsQuery.Builder<I> builder = SelectLifecycleFlagsQuery.newBuilder();
        SelectQuery<LifecycleFlags> query = builder.setTableName(name())
                                                   .setDataSource(dataSource())
                                                   .setIdColumn(idColumn())
                                                   .setId(id)
                                                   .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, LifecycleFlags record) {
        InsertLifecycleFlagsQuery.Builder<I> builder = InsertLifecycleFlagsQuery.newBuilder();
        WriteQuery query = builder.setTableName(name())
                                  .setId(id)
                                  .setLifecycleFlags(record)
                                  .setDataSource(dataSource())
                                  .setIdColumn(idColumn())
                                  .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, LifecycleFlags record) {
        UpdateLifecycleFlagsQuery.Builder<I> builder = UpdateLifecycleFlagsQuery.newBuilder();
        WriteQuery query = builder.setTableName(name())
                                  .setDataSource(dataSource())
                                  .setId(id)
                                  .setLifecycleFlags(record)
                                  .setIdColumn(idColumn())
                                  .build();
        return query;
    }

    /**
     * Always throws an {@code IllegalStateException}, as this table does not provide
     * such a behaviour.
     *
     * @throws IllegalStateException
     *         always
     */
    @Override
    protected WriteQuery composeInsertOrUpdateQuery(I id, LifecycleFlags record) {
        String errMsg = "`LifecycleFlagsTable` does not provide insert-or-update behaviour." +
                " Use `if(contains(..) { update(...); } else { insert(...);}` instead.";
        throw newIllegalStateException(errMsg);
    }

    /**
     * The enumeration of the columns of a {@link LifecycleFlagsTable}.
     */
    enum Column implements TableColumn {

        ID,
        ARCHIVED(BOOLEAN),
        DELETED(BOOLEAN);

        private final Type type;

        Column(Type type) {
            this.type = type;
        }

        /**
         * Creates a column, {@linkplain #type() type} of which is unknown at the compile time.
         */
        Column() {
            this.type = null;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return this == ID;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
