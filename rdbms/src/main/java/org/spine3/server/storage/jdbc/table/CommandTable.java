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
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.util.Iterator;

import static org.spine3.server.storage.jdbc.Sql.Type.BLOB;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_512;
import static org.spine3.server.storage.jdbc.Sql.Type.VARCHAR_999;

/**
 * @author Dmytro Dashenkov.
 */
public class CommandTable extends AbstractTable<String, CommandTable.Column> {

    private static final String TABLE_NAME = "commands";

    public CommandTable(DataSourceWrapper dataSource) {
        super(TABLE_NAME, new IdColumn.StringIdColumn(), dataSource);
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    public CommandRecord read(String id) {
        return null;
    }

    public Iterator<CommandRecord> readByStatus(CommandStatus status) {
        return null;
    }

    public void write(String id, CommandRecord record) {

    }

    public void setOkStatus(String id) {

    }

    public void setError(String id, Error error) {

    }

    public void setFailure(String id, Failure failure) {

    }

    enum Column implements TableColumn {

        id(VARCHAR_999),
        command(BLOB),
        command_status(VARCHAR_512),
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
