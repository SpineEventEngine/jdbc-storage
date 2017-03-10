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

package org.spine3.server.storage.jdbc.table;

import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.storage.jdbc.Sql.Type;
import org.spine3.server.storage.jdbc.command.query.CommandTableQueryFactory;
import org.spine3.server.storage.jdbc.command.query.SelectCommandByStatusQuery;
import org.spine3.server.storage.jdbc.command.query.SetErrorQuery;
import org.spine3.server.storage.jdbc.command.query.SetFailureQuery;
import org.spine3.server.storage.jdbc.command.query.SetOkStatusQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.util.Iterator;

import static org.spine3.server.storage.jdbc.Sql.Type.BLOB;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_255;

/**
 * A table for storing {@link CommandRecord commands}.
 *
 * @author Dmytro Dashenkov
 */
public class CommandTable extends AbstractTable<String, CommandRecord, CommandTable.Column> {

    public static final String TABLE_NAME = "commands";

    private final CommandTableQueryFactory queryFactory;

    public CommandTable(DataSourceWrapper dataSource) {
        super(TABLE_NAME, new IdColumn.StringIdColumn(Column.id.name()), dataSource);
        this.queryFactory = new CommandTableQueryFactory(dataSource);
        queryFactory.setLogger(log());
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    @Override
    protected QueryFactory<String, CommandRecord> getQueryFactory() {
        return queryFactory;
    }

    public Iterator<CommandRecord> readByStatus(CommandStatus status) {
        final SelectCommandByStatusQuery query = queryFactory.newSelectCommandByStatusQuery(status);
        final Iterator<CommandRecord> result = query.execute();
        return result;
    }

    public void setOkStatus(String id) {
        final SetOkStatusQuery query = queryFactory.newSetOkStatusQuery(id);
        query.execute();
    }

    public void setError(String id, Error error) {
        final SetErrorQuery query = queryFactory.newSetErrorQuery(id, error);
        query.execute();
    }

    public void setFailure(String id, Failure failure) {
        final SetFailureQuery query = queryFactory.newSetFailureQuery(id, failure);
        query.execute();
    }

    public enum Column implements TableColumn {

        id(VARCHAR_255),
        command(BLOB),
        command_status(VARCHAR_255),
        error(BLOB),
        failure(BLOB);

        private final Type type;

        Column(Type type) {
            this.type = type;
        }

        @Override
        public Type type() {
            return type;
        }
    }
}
