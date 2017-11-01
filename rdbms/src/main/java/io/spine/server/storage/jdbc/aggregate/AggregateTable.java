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

package io.spine.server.storage.jdbc.aggregate;

import com.google.protobuf.Message;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.AbstractTable;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.EntityTable;

/**
 * A common superclass for the
 * {@linkplain AbstractTable tables} working with
 * the {@linkplain JdbcAggregateStorage aggregate storage}.
 *
 * @author Dmytro Dashenkov
 */
abstract class AggregateTable<I, T extends Message> extends EntityTable<I, T, T> {

    AggregateTable(Class<? extends Entity<I, ?>> entityClass,
                             String idColumnName,
                             DataSourceWrapper dataSource) {
        super(entityClass, idColumnName, dataSource);
    }

    AggregateTable(String tableName,
                             Class<? extends Entity<I, ?>> entityClass,
                             String idColumnName,
                             DataSourceWrapper dataSource) {
        super(tableName, entityClass, idColumnName, dataSource);
    }
}