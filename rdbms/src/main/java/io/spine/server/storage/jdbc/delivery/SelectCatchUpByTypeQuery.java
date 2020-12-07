/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.delivery;

import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.CatchUp;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.type.TypeUrl;

import java.sql.ResultSet;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.delivery.CatchUpTable.Column.PROJECTION_TYPE;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

/**
 * Selects the {@link CatchUp} statuses by the type of the catching-up projection.
 */
public class SelectCatchUpByTypeQuery extends AbstractQuery
        implements SelectQuery<Iterator<CatchUp>> {

    private final TypeUrl projectionType;

    private SelectCatchUpByTypeQuery(Builder builder) {
        super(builder);
        this.projectionType = builder.projectionType;
    }

    @Override
    public Iterator<CatchUp> execute() {
        AbstractSQLQuery<Object, ?> query =
                factory().select(pathOf(bytesColumn()))
                         .from(table())
                         .where(pathOf(PROJECTION_TYPE).eq(projectionType.value()));
        ResultSet resultSet = query.getResults();
        DbIterator<CatchUp> iterator =
                DbIterator.over(resultSet,
                                messageReader(bytesColumn().name(), CatchUp.getDescriptor()));
        return iterator;
    }

    /**
     * Creates a new {@code Builder} for this query.
     */
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder of {@code SelectCatchUpByTypeQuery} query.
     */
    static class Builder extends AbstractQuery.Builder<Builder, SelectCatchUpByTypeQuery> {

        private TypeUrl projectionType;

        /**
         * Prevents the instantiation of this builder from outside of this class.
         */
        private Builder() {
            super();
        }

        Builder setProjectionType(TypeUrl type) {
            this.projectionType = type;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectCatchUpByTypeQuery doBuild() {
            checkNotNull(projectionType, "The type of the catching-up projection must be set.");
            return new SelectCatchUpByTypeQuery(this);
        }
    }
}