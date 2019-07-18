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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.collect.ImmutableList;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.AbstractTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;
import static io.spine.server.storage.jdbc.delivery.InboxMessageTable.Column.ID;

/**
 * A table in the DB responsible for storing the {@link io.spine.server.delivery.Inbox Inbox} data.
 */
final class InboxMessageTable extends AbstractTable<InboxMessageId, InboxMessage, InboxMessage> {

    InboxMessageTable(String name,
                      IdColumn<InboxMessageId> idColumn,
                      DataSourceWrapper dataSource,
                      TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
    }

    @Override
    protected TableColumn idColumnDeclaration() {
        return ID;
    }

    @Override
    protected List<? extends TableColumn> tableColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    @Override
    protected WriteQuery composeInsertQuery(InboxMessageId id, InboxMessage record) {
        return null;
    }

    @Override
    protected WriteQuery composeUpdateQuery(InboxMessageId id, InboxMessage record) {
        return null;
    }

    @Override
    protected SelectQuery<InboxMessage> composeSelectQuery(InboxMessageId id) {
        return null;
    }

    SelectQuery<InboxMessage> composeSelectByShardIndexQuery(ShardIndex index) {
        return null;
    }

    enum Column implements TableColumn {

        ID(STRING_255),
        SIGNAL_ID(STRING_255),
        INBOX_ID(STRING_255),
        SHARD_INDEX(LONG),
        OF_TOTAL_SHARDS(LONG),
        IS_EVENT(BOOLEAN),
        IS_COMMAND(BOOLEAN),
        LABEL(STRING),
        STATUS(STRING),
        WHEN_RECEIVED(LONG),
        WHEN_RECEIVED_NANOS(INT);

        private final Type type;

        Column(Type type) {
            this.type = type;
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
