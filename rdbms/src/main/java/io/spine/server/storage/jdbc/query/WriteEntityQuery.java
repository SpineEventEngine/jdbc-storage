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

import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.RecordTable;

/**
 * The write query to the {@link RecordTable RecordTable}.
 *
 * @author Dmytro Dashenkov
 */
abstract class WriteEntityQuery<I> extends WriteRecordQuery<I, EntityRecordWithColumns> {

    WriteEntityQuery(
            Builder<? extends Builder, ? extends WriteRecordQuery, I, EntityRecordWithColumns> builder) {
        super(builder);
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameters superParameters = super.getQueryParameters();
        final Parameters.Builder builder =
                Parameters.newBuilder()
                          .addParameters(superParameters);
        if (getRecord().hasColumns()) {
            ColumnRecords.feedColumnsTo(builder,
                                        getRecord(),
                                        getColumnTypeRegistry(),
                                        getEntityColumnIdentifier(getRecord(),
                                                                  getFirstColumnIndex()));
        }
        return builder.build();
    }

    /**
     * Obtains the index of the first entity column to be inserted in the query.
     *
     * @return the index of the first column
     */
    protected abstract int getFirstColumnIndex();
}
