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
import com.google.common.collect.ImmutableMap;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;

import java.util.List;

import static com.google.common.base.Functions.forMap;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Collections.sort;

/**
 * A storage write query aware of Entity Columns.
 *
 * @author Alexander Aleksandrov
 */
abstract class ColumnAwareWriteQuery extends AbstractWriteQuery {

    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    ColumnAwareWriteQuery(Builder<? extends Builder, ? extends ColumnAwareWriteQuery> builder) {
        super(builder);
        this.columnTypeRegistry = builder.getColumnTypeRegistry();
    }

    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> getColumnTypeRegistry() {
        return columnTypeRegistry;
    }

    /**
     * Obtains a {@code Function} identifying entity columns of a record in a {@code SQL} query
     * by the index for each column name.
     *
     * <p>Such an index may be used to insert column values to a {@code PreparedStatement} via
     * the {@linkplain io.spine.server.entity.storage.ColumnRecords#feedColumnsTo
     * feedColumnsTo() utility}.
     *
     * @param record           the record to extract column names
     * @param firstColumnIndex the index of the first entity column in a query
     * @return a {@code Function} transforming a column name to its index
     * @see #formatAndMergeColumns(EntityRecordWithColumns, String)
     */
    Function<String, String> getEntityColumnIdentifier(EntityRecordWithColumns record,
                                                        int firstColumnIndex) {
        final ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        int index = firstColumnIndex;
        for (String entry : getSortedColumnNames(record)) {
            result.put(entry, String.valueOf(index));
            index++;
        }
        final Function<String, String> function = forMap(result.build());
        return function;
    }

    /**
     * Formats column names and merges the formatted values into {@code String}.
     *
     * <p>Use {@link #getEntityColumnIdentifier(EntityRecordWithColumns, int)}
     * to identify a column in the result. These methods use the same ordering of column names.
     *
     * @param record       the record to obtain column names
     * @param columnFormat the format of a column name
     * @return the formatted column names merged into {@code String}
     */
    static String formatAndMergeColumns(EntityRecordWithColumns record, String columnFormat) {
        final StringBuilder builder = new StringBuilder();
        for (String columnName : getSortedColumnNames(record)) {
            final String item = format(columnFormat, columnName);
            builder.append(item);
        }
        return builder.toString();
    }

    /**
     * Obtains the sorted entity column names.
     *
     * <p>Use this method to order the entity columns in a SQL query.
     *
     * @param record the record to obtain column names
     * @return a {@code Collection} of sorted column names
     */
    private static Iterable<String> getSortedColumnNames(EntityRecordWithColumns record) {
        final List<String> list = newArrayList(record.getColumnNames());
        sort(list);
        return list;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q>, Q extends ColumnAwareWriteQuery>
            extends AbstractWriteQuery.Builder<B, Q> {

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
