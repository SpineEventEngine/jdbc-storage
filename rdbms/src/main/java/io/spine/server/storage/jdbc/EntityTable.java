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

package io.spine.server.storage.jdbc;

import com.google.protobuf.Message;
import io.spine.server.entity.Entity;

import static io.spine.server.storage.jdbc.DbTableNameFactory.newTableName;

/**
 * A common superclass for the {@linkplain AbstractTable tables} working with the storages which
 * store {@linkplain Entity entities}.
 *
 * @author Dmytro Dashenkov
 */
abstract class EntityTable<I, R extends Message, W>
        extends AbstractTable<I, R, W> {

    private final Class<? extends Entity<I, ?>> entityClass;

    /**
     * Creates a new instance of the {@code EntityTable}.
     *
     * <p>The table will have a name based on the FQN name of the given {@link Entity} class.
     *
     * @param entityClass the {@link Class} of the {@link Entity} to store
     * @param dataSource  an instance of {@link DataSourceWrapper} to use
     */
    EntityTable(Class<? extends Entity<I, ?>> entityClass,
                          String idColumnName,
                          DataSourceWrapper dataSource) {
        this(newTableName(entityClass), entityClass, idColumnName, dataSource);
    }

    /**
     * Creates a new instance of the {@code EntityTable}.
     *
     * @param tableName   the name of the table
     * @param entityClass the {@link Class} of the {@link Entity} to store
     * @param dataSource  an instance of {@link DataSourceWrapper} to use
     */
    EntityTable(String tableName,
                          Class<? extends Entity<I, ?>> entityClass,
                          String idColumnName,
                          DataSourceWrapper dataSource) {
        super(tableName, IdColumn.newInstance(entityClass, idColumnName), dataSource);
        this.entityClass = entityClass;
    }

    Class<? extends Entity<I, ?>> getEntityClass() {
        return entityClass;
    }
}
