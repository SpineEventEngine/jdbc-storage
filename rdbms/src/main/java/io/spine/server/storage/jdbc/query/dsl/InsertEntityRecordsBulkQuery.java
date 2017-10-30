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

import com.google.common.base.Functions;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.Serializer;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;

/**
 * A query for {@code INSERT}-ing multiple {@linkplain EntityRecord entity records} as a bulk.
 *
 * @author Dmytro Dashenkov
 */
class InsertEntityRecordsBulkQuery<I> extends ColumnAwareWriteQuery {

    private final Map<I, EntityRecordWithColumns> records;
    private final IdColumn<I> idColumn;

    InsertEntityRecordsBulkQuery(Builder<I> builder) {
        super(builder);
        this.records = builder.records;
        this.idColumn = builder.idColumn;
    }

    @Override
    public long execute() {
        final SQLInsertClause clause = createClause();
        for (I id : records.keySet()) {
            final EntityRecordWithColumns record = records.get(id);
            final Parameters batchParameters = getBatchParameters(id, record);
            addBatch(clause, batchParameters);
        }
        return clause.execute();
    }

    @Override
    Parameters getParameters() {
        throw new UnsupportedOperationException("Bulk operations doesn't allow this.");
    }

    private void addBatch(SQLInsertClause clause, Parameters parameters) {
        final Set<String> identifiers = parameters.getIdentifiers();
        for (String identifier : identifiers) {
            final Object parameterValue = parameters.getParameter(identifier)
                                                    .getValue();
            clause.set(pathOf(identifier), parameterValue);
        }
        clause.addBatch();
    }

    private Parameters getBatchParameters(I recordId, EntityRecordWithColumns record) {
        final Parameters.Builder parameters = Parameters.newBuilder();
        addRecordParams(parameters, recordId, record);
        if (record.hasColumns()) {
            ColumnRecords.feedColumnsTo(parameters,
                                        record,
                                        getColumnTypeRegistry(),
                                        Functions.<String>identity());
        }
        return parameters.build();
    }

    @Override
    SQLInsertClause createClause() {
        return factory().insert(table());
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Adds an {@link EntityRecordWithColumns} to the query parameters.
     *
     * <p>This includes following items:
     * <ul>
     *     <li>ID;
     *     <li>serialized record by itself;
     * </ul>
     *
     * @param parametersBuilder the builder to add the query parameters to
     * @param id                the ID of the record
     * @param record            the record to store
     */
    private void addRecordParams(Parameters.Builder parametersBuilder,
                                 I id,
                                 EntityRecordWithColumns record) {
        idColumn.setId(idColumn.getColumnName(), id, parametersBuilder);
        final byte[] serializedRecord = Serializer.serialize(record.getRecord());
        final Parameter recordParameter = Parameter.of(serializedRecord, Sql.Type.BLOB);
        parametersBuilder.addParameter(entity.name(), recordParameter);
    }

    static class Builder<I> extends ColumnAwareWriteQuery.Builder<Builder<I>,
                                                                  InsertEntityRecordsBulkQuery> {

        private final Map<I, EntityRecordWithColumns> records = newHashMap();
        private IdColumn<I> idColumn;

        Builder<I> addRecords(Map<I, EntityRecordWithColumns> records) {
            checkNotNull(records);
            this.records.putAll(records);
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return getThis();
        }

        @Override
        InsertEntityRecordsBulkQuery<I> build() {
            if (records.isEmpty()) {
                throw new IllegalStateException("Records are not set.");
            }
            return new InsertEntityRecordsBulkQuery<>(this);
        }

        @Override
        Builder<I> getThis() {
            return this;
        }
    }
}
