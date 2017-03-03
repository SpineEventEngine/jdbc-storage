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

package org.spine3.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.query.AbstractQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

/**
 * This class creates queries for interaction with {@link ProjectionTable}.
 *
 * @author Andrey Lavrov
 */
public class ProjectionStorageQueryFactory<I> extends AbstractQueryFactory {

    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource      instance of {@link DataSourceWrapper}
     */
    public ProjectionStorageQueryFactory(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a query that inserts a new {@link Timestamp} to the {@link ProjectionTable}. */
    public InsertTimestampQuery newInsertTimestampQuery(String key, Timestamp time) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder(key)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setTimestamp(time);
        return builder.build();
    }

    /** Returns a query that updates {@link Timestamp} in the {@link ProjectionTable}. */
    public UpdateTimestampQuery newUpdateTimestampQuery(String key, Timestamp time) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder(key)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setTimestamp(time);
        return builder.build();
    }

    /** Returns a query that selects timestamp from the {@link ProjectionTable}. */
    public SelectTimestampQuery newSelectTimestampQuery(String key) {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder(key)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger);
        return builder.build();
    }
}
