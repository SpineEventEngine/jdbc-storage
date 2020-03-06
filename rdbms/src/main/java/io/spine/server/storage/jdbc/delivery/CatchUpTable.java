/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import com.google.protobuf.Descriptors;
import io.spine.server.delivery.CatchUp;
import io.spine.server.delivery.CatchUpId;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * A table in the DB responsible for storing the statuses of the ongoing
 * {@link io.spine.server.delivery.CatchUp catch-up} processes.
 */
public class CatchUpTable extends MessageTable<CatchUpId, CatchUp> {

    private static final String NAME = "catchup";

    /**
     * Creates a new instance of the table.
     *
     * @param dataSource
     *         wrapper over the RDBMS data source
     * @param typeMapping
     *         mapping of types, specific to the RDBMS for the current application
     */
    CatchUpTable(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(NAME, IdColumn.of(CatchUpTable.Column.ID, CatchUpId.class), dataSource, typeMapping);
    }

    @Override
    protected Descriptors.Descriptor messageDescriptor() {
        return CatchUp.getDescriptor();
    }

    @Override
    protected Iterable<? extends MessageTable.Column<CatchUp>> messageSpecificColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    /**
     * Reads all {@code CatchUp} statuses by the type URL of the catching-up projection.
     */
    Iterable<CatchUp> readByType(TypeUrl type) {
        SelectCatchUpByTypeQuery query = SelectCatchUpByTypeQuery
                .newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .setProjectionType(type)
                .build();
        Iterator<CatchUp> iterator = query.execute();
        return ImmutableList.copyOf(iterator);
    }

    /**
     * Reads all {@code CatchUp} statuses.
     *
     * @implNote Realistically, there should not be lots of the catch-up processes in the
     *         system. Therefore, no pagination is implemented at this point.
     */
    public Iterable<CatchUp> readAll() {
        SelectAllCatchUpsQuery query = SelectAllCatchUpsQuery
                .newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .build();
        Iterator<CatchUp> iterator = query.execute();
        return ImmutableList.copyOf(iterator);
    }

    /**
     * The columns of {@link InboxMessage} DB representation.
     */
    enum Column implements MessageTable.Column<CatchUp> {

        /**
         * Identifier of the {@code CatchUp}.
         */
        ID(CatchUp::getId),

        /**
         * The type URL of the catching-up projection, stored as a {@code String}.
         */
        PROJECTION_TYPE(STRING_255, (m) -> m.getId()
                                            .getProjectionType());

        private final @Nullable Type type;
        private final Getter<CatchUp> getter;

        Column(Type type, Getter<CatchUp> getter) {
            this.type = type;
            this.getter = getter;
        }

        Column(Getter<CatchUp> getter) {
            this.type = null;
            this.getter = getter;
        }

        @Override
        public @Nullable Type type() {
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
        public Getter<CatchUp> getter() {
            return getter;
        }
    }
}
