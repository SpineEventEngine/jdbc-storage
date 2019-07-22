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
import com.google.protobuf.Descriptors.Descriptor;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.IdColumn;

import javax.annotation.Nullable;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING_255;

final class ShardedWorkRegistryTable extends MessageTable<ShardIndex, ShardSessionRecord> {

    private static final String NAME = "shard_session_registry";

    ShardedWorkRegistryTable(DataSourceWrapper dataSource,
                             TypeMapping typeMapping) {
        super(NAME, IdColumn.of(Column.ID, ShardIndex.class), dataSource, typeMapping);
    }

    @Override
    protected Descriptor messageDescriptor() {
        return ShardSessionRecord.getDescriptor();
    }

    @Override
    protected Iterable<Column> messageSpecificColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    DbIterator<ShardSessionRecord> readByIndex(long shardIndex) {
        SelectShardSessionsByShardIndex query = composeSelectByShardIndexQuery(shardIndex);
        DbIterator<ShardSessionRecord> iterator = query.execute();
        return iterator;
    }

    private SelectShardSessionsByShardIndex composeSelectByShardIndexQuery(long shardIndex) {
        SelectShardSessionsByShardIndex.Builder builder =
                SelectShardSessionsByShardIndex.newBuilder();
        SelectShardSessionsByShardIndex query = builder.setTableName(name())
                                                       .setDataSource(dataSource())
                                                       .setShardIndex(shardIndex)
                                                       .build();
        return query;
    }

    enum Column implements MessageTable.Column<ShardSessionRecord> {

        ID(ShardSessionRecord::getIndex),

        SHARD_INDEX(LONG, m -> m.getIndex()
                                .getIndex()),

        OF_TOTAL_SHARDS(LONG, m -> m.getIndex()
                                    .getOfTotal()),

        NODE_ID(STRING_255, m -> m.getPickedBy()
                                  .getValue()),

        WHEN_LAST_PICKED(LONG, m -> m.getWhenLastPicked()
                                     .getSeconds()),

        WHEN_LAST_PICKED_NANOS(INT, m -> m.getWhenLastPicked()
                                          .getNanos());

        @Nullable
        private final Type type;
        private final Getter<ShardSessionRecord> getter;

        Column(Getter<ShardSessionRecord> getter) {
            this.type = null;
            this.getter = getter;
        }

        Column(Type type, Getter<ShardSessionRecord> getter) {
            this.type = type;
            this.getter = getter;
        }

        @Nullable
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

        @Override
        public Getter<ShardSessionRecord> getter() {
            return getter;
        }
    }
}
