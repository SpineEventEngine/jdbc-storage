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

package io.spine.server.storage.jdbc;

import io.spine.annotation.Internal;
import io.spine.server.storage.Storage;

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
 * @author Dmytro Dashenkov
 */
@Internal
public abstract class StorageBuilder<B extends StorageBuilder<B, S>, S extends Storage> {

    private boolean multitenant;
    private DataSourceWrapper dataSource;
    private TypeMapping typeMapping;

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

    public TypeMapping getTypeMapping() {
        return typeMapping;
    }

    /**
     * @param typeMapping the type mapping for the usage in queries
     */
    public B setTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = checkNotNull(typeMapping);
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
        S result = doBuild();
        checkNotNull(result, "The build storage must not be null.");
        return result;
    }

    /**
     * Checks the preconditions of the storage construction.
     *
     * <p>Default implementation checks that the field {@link #dataSource}
     * was set to a non-null value.
     *
     * <p>Override this method to modify these preconditions.
     *
     * @throws IllegalStateException upon a precondition violation
     */
    protected void checkPreconditions() throws IllegalStateException {
        checkState(dataSource != null, "Data source must not be null");
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
