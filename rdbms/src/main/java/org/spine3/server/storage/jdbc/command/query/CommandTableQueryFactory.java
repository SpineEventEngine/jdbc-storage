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

package org.spine3.server.storage.jdbc.command.query;

import org.slf4j.Logger;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumnSetter;

/**
 * An implementation of the {@link QueryFactory} for generating queries to
 * the {@link org.spine3.server.storage.jdbc.table.CommandTable}.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
public class CommandTableQueryFactory implements QueryFactory<String, CommandRecord> {

    private final IdColumnSetter<String> idColumnSetter;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource the dataSource wrapper
     */
    public CommandTableQueryFactory(DataSourceWrapper dataSource) {
        this.idColumnSetter = new IdColumnSetter.StringIdColumnSetter();
        this.dataSource = dataSource;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Returns a query that updates {@link CommandRecord} with a new {@link Error}.
     *
     * @param id    command record id
     * @param error a technical error occurred during command handling
     */
    public SetErrorQuery newSetErrorQuery(String id, Error error) {
        final SetErrorQuery.Builder builder = SetErrorQuery.newBuilder()
                                                           .setDataSource(dataSource)
                                                           .setLogger(logger)
                                                           .setIdColumnSetter(
                                                                   idColumnSetter)
                                                           .setId(id)
                                                           .setRecord(error);
        return builder.build();
    }

    /**
     * Returns a query that updates {@link CommandRecord} with a new {@link Failure}.
     *
     * @param id      command record id
     * @param failure a business failure occurred during command handling
     */
    public SetFailureQuery newSetFailureQuery(String id, Failure failure) {
        final SetFailureQuery.Builder builder = SetFailureQuery.newBuilder()
                                                               .setDataSource(dataSource)
                                                               .setLogger(logger)
                                                               .setIdColumnSetter(
                                                                       idColumnSetter)
                                                               .setId(id)
                                                               .setRecord(failure);
        return builder.build();
    }

    /**
     * Returns a query that sets {@link CommandStatus} to OK state.
     *
     * @param id command record id
     */
    public SetOkStatusQuery newSetOkStatusQuery(String id) {
        final SetOkStatusQuery.Builder builder = SetOkStatusQuery.newBuilder()
                                                                 .setDataSource(dataSource)
                                                                 .setLogger(logger)
                                                                 .setIdColumnSetter(
                                                                         idColumnSetter)
                                                                 .setId(id);
        return builder.build();
    }

    /** Returns a query that {@link CommandRecord} selects record by {@link CommandStatus}. */
    public SelectCommandByStatusQuery newSelectCommandByStatusQuery(CommandStatus status) {
        final SelectCommandByStatusQuery.Builder builder =
                SelectCommandByStatusQuery.newBuilder()
                                          .setDataSource(dataSource)
                                          .setLogger(logger)
                                          .setStatus(status);
        return builder.build();
    }

    @Override
    public SelectByIdQuery<String, CommandRecord> newSelectByIdQuery(String id) {
        final SelectCommandByIdQuery.Builder builder =
                SelectCommandByIdQuery.newBuilder()
                                      .setDataSource(dataSource)
                                      .setLogger(logger)
                                      .setIdColumnSetter(idColumnSetter)
                                      .setId(id);
        return builder.build();
    }

    @Override
    public WriteQuery newInsertQuery(String id, CommandRecord record) {
        final InsertCommandQuery.Builder builder =
                InsertCommandQuery.newBuilder()
                                  .setDataSource(dataSource)
                                  .setLogger(logger)
                                  .setIdColumnSetter(idColumnSetter)
                                  .setId(id)
                                  .setRecord(record)
                                  .setStatus(record.getStatus().getCode());
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(String id, CommandRecord record) {
        final UpdateCommandQuery.Builder builder =
                UpdateCommandQuery.newBuilder()
                                  .setDataSource(dataSource)
                                  .setLogger(logger)
                                  .setIdColumnSetter(idColumnSetter)
                                  .setId(id)
                                  .setRecord(record)
                                  .setStatus(record.getStatus().getCode());
        return builder.build();
    }
}
