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

package org.spine3.server.storage.jdbc.table.entity.aggregate;

import org.spine3.server.entity.Entity;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;

import static org.spine3.server.storage.jdbc.Sql.Type.BOOLEAN;
import static org.spine3.server.storage.jdbc.Sql.Type.UNKNOWN;

/**
 * @author Dmytro Dashenkov.
 */
public class VisibilityTable<I> extends AggregateTable<I, VisibilityTable.Column> {

    private static final String TABLE_NAME_POSTFIX = "visibility";

    public VisibilityTable(Class<? extends Entity<I, ?>> entityClass,
                              DataSourceWrapper dataSource) {
        super(DbTableNameFactory.newTableName(entityClass) + TABLE_NAME_POSTFIX,
              entityClass,
              dataSource);
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    public Visibility read(I id) {
        return null;
    }

    public void write(I id, Visibility status) {

    }

    public void markArchived(I id) {

    }

    public void markDeleted(I id) {

    }

    enum Column implements TableColumn {

        id(UNKNOWN),
        archived(BOOLEAN),
        deleted(BOOLEAN);

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
