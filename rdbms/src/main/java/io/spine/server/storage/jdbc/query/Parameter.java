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

import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.TableColumn;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A parameter of an SQL query.
 *
 * <p>The class is a DTO containing a parameter value and its {@linkplain Sql.Type SQL type}.
 *
 * @author Dmytro Grankin
 */
public final class Parameter {

    private final Object value;
    private final Sql.Type type;

    private Parameter(@Nullable Object value, Sql.Type type) {
        this.value = value;
        this.type = checkNotNull(type);
    }

    /**
     * Creates a parameter using the specified parameters.
     *
     * @param value the parameter value
     * @param type  the SQL type of the parameter
     * @return a new {@code Parameter} instance
     */
    public static Parameter of(@Nullable Object value, Sql.Type type) {
        return new Parameter(value, type);
    }

    /**
     * Creates a parameter using the specified parameters.
     *
     * @param value  the parameter value
     * @param column the {@link TableColumn} describing this parameter
     * @return a new {@code Parameter} instance
     */
    public static Parameter of(@Nullable Object value, TableColumn column) {
        return new Parameter(value, column.type());
    }

    /**
     * Obtains a raw value of the parameter.
     *
     * @return the parameter value
     */
    @Nullable
    public Object getValue() {
        return value;
    }

    /**
     * Obtains an SQL type of the parameter.
     *
     * <p>The type should be used to set a {@linkplain #getValue() parameter value} to an SQL query.
     *
     * @return the type of the parameter
     */
    public Sql.Type getType() {
        return type;
    }
}
