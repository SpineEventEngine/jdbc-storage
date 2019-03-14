/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation base for the {@linkplain JdbcColumnType JdbcColumnTypes} which store the given
 * value "as is", i.e. with no preceding conversion.
 *
 * <p>More formally, a {@code Simple} column type is a column type whose
 * {@link io.spine.server.entity.storage.ColumnType#convertColumnValue convertColumnValue()} method
 * is an identity function.
 *
 * @implNote The {@link io.spine.server.entity.storage.ColumnType#convertColumnValue
 *         convertColumnValue()} method throws a {@link NullPointerException} if the value
 *         is equal to {@code null}.
 */
@SPI
public abstract class SimpleJdbcColumnType<T> extends AbstractJdbcColumnType<T, T> {

    @Override
    public final T convertColumnValue(T fieldValue) {
        checkNotNull(fieldValue);
        return fieldValue;
    }
}
