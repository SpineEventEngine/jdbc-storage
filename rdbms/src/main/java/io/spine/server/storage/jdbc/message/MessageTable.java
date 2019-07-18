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

package io.spine.server.storage.jdbc.message;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.AbstractTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.function.Function;

public abstract class MessageTable<I, M extends Message> extends AbstractTable<I, M, M> {

    protected MessageTable(String name,
                           IdColumn<I> idColumn,
                           DataSourceWrapper dataSource,
                           TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
    }

    @Override
    protected InsertMessageQuery<I, M> composeInsertQuery(I id, M record) {
        InsertMessageQuery.Builder<I, M> builder = InsertMessageQuery.newBuilder();
        InsertMessageQuery<I, M> query = builder.setTableName(name())
                                                .setDataSource(dataSource())
                                                .setIdColumn(idColumn())
                                                .setId(id)
                                                .setMessage(record)
                                                .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, M record) {
        return null;
    }

    @Override
    protected SelectQuery<M> composeSelectQuery(I id) {
        return null;
    }

    public interface Column<M extends Message> extends TableColumn {

        Getter<M> getter();

        @FunctionalInterface
        interface Getter<M extends Message> extends Function<M, Object> {
        }
    }
}
