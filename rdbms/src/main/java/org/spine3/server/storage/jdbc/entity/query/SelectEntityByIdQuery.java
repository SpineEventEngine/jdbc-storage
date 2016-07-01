/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.Internal;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.entity.query.Constants.*;

@Internal
public class SelectEntityByIdQuery<I> extends SelectByIdQuery<I, EntityStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_BY_ID = "SELECT " + ENTITY_COL + " FROM %s WHERE " + ID_COL + " = ?;";

    public SelectEntityByIdQuery(String tableName, DataSourceWrapper dataSource, IdColumn<I> idColumn, I id) {
        super(format(SELECT_BY_ID, tableName), dataSource, idColumn, id);
        setMessageColumnName(ENTITY_COL);
        setMessageDescriptor(RECORD_DESCRIPTOR);
    }
}