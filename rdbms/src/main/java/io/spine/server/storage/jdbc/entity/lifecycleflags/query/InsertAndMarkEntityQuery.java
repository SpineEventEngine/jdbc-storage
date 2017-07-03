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

package io.spine.server.storage.jdbc.entity.lifecycleflags.query;

import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.table.entity.aggregate.LifecycleFlagsTable;

import static java.lang.String.format;

/**
 * The query for creating a new record in the table storing
 * the {@linkplain io.spine.server.entity.LifecycleFlags entity lifecycle flags} with one
 * of the columns set to {@code true}.
 *
 * @author Dmytro Dashenkov
 */
public class InsertAndMarkEntityQuery<I> extends MarkEntityQuery<I> {

    private static final String FORMAT_PLACEHOLDER = "%s";

    private static final String SQL_TEMPLATE =
            Sql.Query.INSERT_INTO + FORMAT_PLACEHOLDER +
            Sql.BuildingBlock.BRACKET_OPEN +
            LifecycleFlagsTable.Column.id + Sql.BuildingBlock.COMMA + FORMAT_PLACEHOLDER +
            Sql.BuildingBlock.BRACKET_CLOSE +
            Sql.Query.VALUES + Sql.BuildingBlock.BRACKET_OPEN +
            Sql.Query.PLACEHOLDER + Sql.BuildingBlock.COMMA + Sql.Query.TRUE +
            Sql.BuildingBlock.BRACKET_CLOSE + Sql.BuildingBlock.SEMICOLON;

    protected InsertAndMarkEntityQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newInsertBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends AbstractMarkQueryBuilder<I,
            Builder<I>,
            InsertAndMarkEntityQuery<I>> {

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        protected InsertAndMarkEntityQuery<I> newQuery() {
            return new InsertAndMarkEntityQuery<>(this);
        }

        @Override
        protected String buildSql() {
            return format(SQL_TEMPLATE, getTableName(), getColumn());
        }
    }
}