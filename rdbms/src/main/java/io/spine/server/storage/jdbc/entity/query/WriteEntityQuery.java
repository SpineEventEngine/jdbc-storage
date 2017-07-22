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

package io.spine.server.storage.jdbc.entity.query;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.WriteRecordQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable.StandardColumn;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.spine.server.entity.storage.EntityColumns.sorted;

/**
 * The write query to the {@link io.spine.server.storage.jdbc.table.entity.RecordTable RecordTable}.
 *
 * @author Dmytro Dashenkov
 */
public class WriteEntityQuery<I> extends WriteRecordQuery<I, EntityRecordWithColumns> {

    protected WriteEntityQuery(
            Builder<? extends Builder, ? extends WriteRecordQuery, I, EntityRecordWithColumns> builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        if(getRecord().hasColumns()) {
            ColumnRecords.feedColumnsTo(statement,
                                        getRecord(),
                                        getColumnTypeRegistry(),
                                        getTransformer());
        }
        return statement;
    }

    /**
     * Retrieves a {@link Function} transforming the Entity Column names into the indexes of
     * the {@link PreparedStatement} parameters.
     */
    private Function<String, Integer> getTransformer() {
        final Function<String, Integer> function;
        final Map<String, Column> columns = getRecord().getColumns();
        final Collection<String> columnNames = sorted(columns.keySet());
        final Map<String, Integer> result = new HashMap<>();

        int index = StandardColumn.values().length + 1;
        for (String entry : columnNames) {
            result.put(entry, index);
            index++;
        }

        function = Functions.forMap(result);
        return function;
    }
}
