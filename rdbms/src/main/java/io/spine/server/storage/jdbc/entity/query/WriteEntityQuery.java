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
import io.spine.option.EntityOption;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.WriteRecordQuery;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
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

        try {
            statement.setBoolean(QueryParameter.ARCHIVED.index, false);
            statement.setBoolean(QueryParameter.DELETED.index, false);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return statement;
    }

    protected Function<String, Integer> getTransformer() {
        final Function<String, Integer> function;
        final Map<String, Column> columns = getRecord().getColumns();
        final Map<String, Integer> result = new HashMap<>();

        Integer index = 3;

        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            result.put(entry.getKey(), index);
            index++;
        }

        function = Functions.forMap(result);
        return function;
    }

    protected enum QueryParameter {

        RECORD(1),
        ARCHIVED(2),
        DELETED(3),
        ID(4);

        public final int index;

        QueryParameter(int index) {
            this.index = index;
        }
    }
}
