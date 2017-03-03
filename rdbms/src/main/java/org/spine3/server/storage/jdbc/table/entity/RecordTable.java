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

package org.spine3.server.storage.jdbc.table.entity;

import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.Sql.Type.BLOB;
import static org.spine3.server.storage.jdbc.Sql.Type.UNKNOWN;

/**
 * @author Dmytro Dashenkov.
 */
public class RecordTable<I> extends EntityTable<I, RecordTable.Column> {

    protected RecordTable(Class<Entity<I, ?>> entityClass,
                          IdColumn<I> idColumn,
                          DataSourceWrapper dataSource) {
        super(entityClass, idColumn, dataSource);
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    enum Column implements TableColumn, Cloneable {

        id(UNKNOWN),
        entity_record(BLOB);

        private final Sql.Type type;

        Column(Sql.Type type) {
            this.type = type;
        }

        @Override
        public Sql.Type type() {
            return type;
        }
    }
}
