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
package io.spine.server.storage.jdbc.type;

import io.spine.annotation.SPI;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

/**
 * The implementation base for the JDBC-storage
 * {@linkplain io.spine.server.entity.storage.ColumnType ColumnTypes}.
 *
 * @author Alexander Aleksandrov
 */
@SPI
public abstract class AbstractJdbcColumnType<J, C> implements JdbcColumnType<J, C> {

    @Override
    public void setColumnValue(Parameters.Builder storageRecord, C value, String columnIdentifier) {
        final Parameter parameter = Parameter.of(value, getSqlType());
        storageRecord.addParameter(columnIdentifier, parameter);
    }

    @Override
    public void setNull(Parameters.Builder storageRecord, String columnIdentifier) {
        final Parameter nullParameter = Parameter.of(null, getSqlType());
        storageRecord.addParameter(columnIdentifier, nullParameter);
    }
}
