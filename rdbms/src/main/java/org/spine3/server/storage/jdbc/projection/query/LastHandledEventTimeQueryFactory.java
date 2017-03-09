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
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class creates queries for interaction with {@link ProjectionTable}.
 *
 * @author Andrey Lavrov
 */
public class LastHandledEventTimeQueryFactory implements QueryFactory<String, Timestamp> {

    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource      instance of {@link DataSourceWrapper}
     */
    public LastHandledEventTimeQueryFactory(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public SelectByIdQuery<String, Timestamp> newSelectByIdQuery(String id) {
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder(id)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger);
        return builder.build();
    }

    @Override
    public WriteQuery newInsertQuery(String id, Timestamp record) {
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder(id)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setTimestamp(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(String id, Timestamp record) {
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder(id)
                                                                         .setDataSource(dataSource)
                                                                         .setLogger(logger)
                                                                         .setTimestamp(record);
        return builder.build();
    }

    public void setLogger(Logger logger) {
        this.logger = checkNotNull(logger);
    }
}
