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

package org.spine3.server.storage.jdbc.builder;

import org.spine3.server.storage.Storage;
import org.spine3.server.storage.jdbc.query.AbstractQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An abstract Builder for the JDBC-based {@link Storage} implementations.
 *
 * <p>Aggregates all the common fields of the JDBC storages.
 *
 * <p>Each setter method returns an instance of the builder itself ({@code this}).
 *
 * @param <B> type of the builder itself; used to construct call chains
 * @param <S> type of the built storage
 * @param <F> type of the {@linkplain AbstractQueryFactory} used by the storage
 * @author Dmytro Dashenkov.
 */
public abstract class StorageBuilder<B extends StorageBuilder<B, S, F>,
                                     S extends Storage,
                                     F extends AbstractQueryFactory> {

    private boolean multitenant;

    private DataSourceWrapper dataSource;

    private F queryFactory;

    protected StorageBuilder() {
        // Prevent accidental direct initialization
    }

    public boolean isMultitenant() {
        return multitenant;
    }

    /**
     * @param multitenant determines if the storage is multitenant or not
     * @see Storage#isMultitenant()
     */
    public B setMultitenant(boolean multitenant) {
        this.multitenant = multitenant;
        return getThis();
    }

    public DataSourceWrapper getDataSource() {
        return dataSource;
    }

    /**
     * @param dataSource the {@linkplain DataSourceWrapper} used by the storage
     */
    public B setDataSource(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        return getThis();
    }

    public F getQueryFactory() {
        return queryFactory;
    }

    /**
     * @param queryFactory an implementation of the {@linkplain AbstractQueryFactory} used by the storage
     *                     to generate SQL queries
     */
    public B setQueryFactory(F queryFactory) {
        this.queryFactory = queryFactory;
        return getThis();
    }

    /**
     * Creates a new instance of the {@link Storage} with respect to the preconditions.
     *
     * @return a new non-null instance of the {@link Storage}
     * @see #checkPreconditions()
     */
    public S build() {
        checkPreconditions();
        final S result = doBuild();
        checkNotNull(result, "The build storage must not be null.");
        return result;
    }

    /**
     * Checks the preconditions of the storage construction.
     *
     * <p>Default implementation checks that fields {@link #dataSource} and {@link #queryFactory}
     * were set to a non-null values. Override this method to modify these preconditions.
     *
     * @throws IllegalStateException upon a precondition violation
     */
    protected void checkPreconditions() throws IllegalStateException {
        checkState(dataSource != null, "Data source must not be null");
       // checkState(queryFactory != null, "Query factory must not be null");
    }

    /**
     * Returns current instance of {@linkplain StorageBuilder}.
     *
     * <p>Used in setters to avoid extra unchecked casts.
     *
     * @return {@code StorageBuilder.this} reference
     */
    protected abstract B getThis();

    /**
     * Builds a new instance of the {@link Storage}.
     *
     * <p>The construction preconditions are checked before calling this method.
     *
     * @return new non-null instance of the {@link Storage}.
     */
    protected abstract S doBuild();
}
