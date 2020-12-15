/*
 * Copyright 2020, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import io.spine.server.delivery.CatchUp;
import io.spine.server.delivery.CatchUpId;
import io.spine.server.delivery.CatchUpReadRequest;
import io.spine.server.delivery.CatchUpStorage;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;
import io.spine.type.TypeUrl;

/**
 * A JDBC-based implementation of the {@link CatchUpStorage}.
 *
 * <p>All statuses of the catch-up processes reside in a separate {@linkplain CatchUpTable table}.
 */
public class JdbcCatchUpStorage
        extends JdbcMessageStorage<CatchUpId, CatchUp, CatchUpReadRequest, CatchUpTable>
        implements CatchUpStorage {

    private final DataSourceWrapper dataSource;

    private JdbcCatchUpStorage(Builder builder) {
        super(builder.isMultitenant(), new CatchUpTable(builder.dataSource(),
                                                        builder.typeMapping()));
        this.dataSource = builder.dataSource();
    }

    @Override
    public Iterable<CatchUp> readAll() {
        checkNotClosed();
        return table().readAll();
    }

    @Override
    public Iterable<CatchUp> readByType(TypeUrl projectionType) {
        return table().readByType(projectionType);
    }

    /**
     * Creates a new instance of {@code Builder} for this storage.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Closes the underlying data source.
     */
    @Override
    public void close() {
        super.close();
        dataSource.close();
    }

    /**
     * A builder for {@link JdbcCatchUpStorage}.
     */
    public static class Builder extends StorageBuilder<Builder, JdbcCatchUpStorage> {

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected JdbcCatchUpStorage doBuild() {
            return new JdbcCatchUpStorage(this);
        }
    }
}
