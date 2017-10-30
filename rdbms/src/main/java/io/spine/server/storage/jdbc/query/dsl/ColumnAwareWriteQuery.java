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
package io.spine.server.storage.jdbc.query.dsl;

import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;

/**
 * A storage write query aware of Entity Columns.
 *
 * @author Alexander Aleksandrov
 */
abstract class ColumnAwareWriteQuery extends AbstractStoreQuery {

    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    ColumnAwareWriteQuery(Builder<? extends Builder, ? extends ColumnAwareWriteQuery> builder) {
        super(builder);
        this.columnTypeRegistry = builder.getColumnTypeRegistry();
    }

    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
        return columnTypeRegistry;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q>, Q extends ColumnAwareWriteQuery>
            extends AbstractStoreQuery.Builder<B, Q> {

        private ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry
                = JdbcTypeRegistryFactory.defaultInstance();

        ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
            return columnTypeRegistry;
        }

        B setColumnTypeRegistry(ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return getThis();
        }
    }
}
