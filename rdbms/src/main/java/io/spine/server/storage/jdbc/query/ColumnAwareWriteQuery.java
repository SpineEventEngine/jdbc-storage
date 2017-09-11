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
package io.spine.server.storage.jdbc.query;

import com.google.common.base.Function;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Functions.forMap;

/**
 * A storage write query aware of Entity Columns.
 *
 * @author Alexander Aleksandrov
 */
abstract class ColumnAwareWriteQuery extends WriteQuery {

    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    ColumnAwareWriteQuery(Builder<? extends Builder, ? extends ColumnAwareWriteQuery> builder) {
        super(builder);
        this.columnTypeRegistry = builder.getColumnTypeRegistry();
    }

    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
        return columnTypeRegistry;
    }

    /**
     * Obtains the name-to-index transformer for the specified record.
     *
     * @param record           the record to extract column names
     * @param firstColumnIndex the index of the first entity column in a query
     * @return a {@code Function} transforming a column name to the its index
     * @see io.spine.server.entity.storage.ColumnRecords#feedColumnsTo
     */
    Function<String, Integer> getTransformer(EntityRecordWithColumns record,
                                             int firstColumnIndex) {
        final Map<String, Integer> result = new HashMap<>();

        int index = firstColumnIndex;
        for (String entry : record.getColumnNames()) {
            result.put(entry, index);
            index++;
        }
        final Function<String, Integer> function = forMap(result);
        return function;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q>, Q extends ColumnAwareWriteQuery>
            extends WriteQuery.Builder<B, Q> {

        private ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry
                = JdbcTypeRegistryFactory.defaultInstance();

        ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
            return columnTypeRegistry;
        }

        B setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return getThis();
        }
    }
}
