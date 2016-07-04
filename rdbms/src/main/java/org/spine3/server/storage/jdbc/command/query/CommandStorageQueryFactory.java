/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * This class creates queries for interaction with {@link CommandTable}.
 *
 * @author Andrey Lavrov
 */
@SuppressWarnings("TypeMayBeWeakened")
public class CommandStorageQueryFactory {

    private final IdColumn <String> idColumn;
    private final DataSourceWrapper dataSource;
    private Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource the dataSource wrapper
     */
    public CommandStorageQueryFactory(DataSourceWrapper dataSource) {
        this.idColumn = new IdColumn.StringIdColumn();
        this.dataSource = dataSource;
    }

    /** Sets the logger for logging exceptions during queries execution. */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a query that creates a new {@link CommandTable} if it does not exist. */
    public CreateTableQuery newCreateTableQuery(){
        final CreateTableQuery.Builder builder = CreateTableQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link CommandStorageRecord} to the {@link CommandTable}.
     *
     * @param id        new command record id
     * @param record    new command record
     */
    public InsertCommandQuery newInsertCommandQuery(CommandId id, CommandStorageRecord record){
        final InsertCommandQuery.Builder builder = InsertCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()));
        return builder.build();
    }

    /**
     * Returns a query that updates {@link CommandStorageRecord} in the {@link CommandTable}.
     *
     * @param id        command id
     * @param record    updated record state
     */
    public UpdateCommandQuery newUpdateCommandQuery(CommandId id, CommandStorageRecord record){
        final UpdateCommandQuery.Builder builder = UpdateCommandQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(record)
                .setStatus(CommandStatus.forNumber(record.getStatusValue()));
        return builder.build();
    }

    /**
     * Returns a query that updates {@link CommandStorageRecord} with a new {@link Error}.
     *
     * @param id        command record id
     * @param error     a technical error occurred during command handling
     */
    public SetErrorQuery newSetErrorQuery(CommandId id, Error error){
        final SetErrorQuery.Builder builder = SetErrorQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(error);
        return builder.build();
    }

    /**
     * Returns a query that updates {@link CommandStorageRecord} with a new {@link Failure}.
     *
     * @param id        command record id
     * @param failure   a business failure occurred during command handling
     */
    public SetFailureQuery newSetFailureQuery(CommandId id, Failure failure){
        final SetFailureQuery.Builder builder = SetFailureQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid())
                .setRecord(failure);
        return builder.build();
    }

    /**
     * Returns a query that sets {@link CommandStatus} to OK state.
     *
     * @param id    command record id
     */
    public SetOkStatusQuery newSetOkStatusQuery(CommandId id){
        final SetOkStatusQuery.Builder builder = SetOkStatusQuery.newBuilder()
                .setDataSource(this.dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid());
        return builder.build();
    }

    /** Returns a query that selects {@link CommandStorageRecord} by ID. */
    public SelectCommandByIdQuery newSelectCommandByIdQuery(CommandId id){
        final SelectCommandByIdQuery.Builder builder = SelectCommandByIdQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setIdColumn(idColumn)
                .setId(id.getUuid());
        return builder.build();
    }

    /** Returns a query that {@link CommandStorageRecord} selects record by {@link CommandStatus}. */
    public SelectCommandByStatusQuery newSelectCommandByStatusQuery(CommandStatus status){
        final SelectCommandByStatusQuery.Builder builder = SelectCommandByStatusQuery.newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setStatus(status);
        return builder.build();
    }
}
