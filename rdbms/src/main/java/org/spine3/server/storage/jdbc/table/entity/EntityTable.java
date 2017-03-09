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

import com.google.protobuf.Message;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.table.AbstractTable;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

/**
 * @author Dmytro Dashenkov.
 */
public abstract class EntityTable<I, R extends Message, C extends Enum<C> & TableColumn> extends AbstractTable<I, R, C> {

    private final Class<? extends Entity<I, ?>> entityClass;

    protected EntityTable(Class<? extends Entity<I, ?>> entityClass,
                          IdColumn<I> idColumn,
                          DataSourceWrapper dataSource) {
        this(newTableName(entityClass), entityClass, idColumn, dataSource);
    }

    protected EntityTable(String tableName,
                          Class<? extends Entity<I, ?>> entityClass,
                          IdColumn<I> idColumn,
                          DataSourceWrapper dataSource) {
        super(tableName, idColumn, dataSource);
        this.entityClass = entityClass;
    }

    public Class<? extends Entity<I, ?>> getEntityClass() {
        return entityClass;
    }
}
