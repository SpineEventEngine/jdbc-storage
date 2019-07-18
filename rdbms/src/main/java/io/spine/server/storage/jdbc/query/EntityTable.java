/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.google.common.collect.ImmutableMap;
import io.spine.annotation.Internal;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TypeMapping;

import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
import static io.spine.server.storage.VersionField.version;
import static io.spine.server.storage.jdbc.query.TableNames.newTableName;

/**
 * A common base for the storage tables, that are used to maintain {@linkplain Entity entities}.
 */
@Internal
public abstract class EntityTable<I, R, W> extends AbstractTable<I, R, W> {

    /**
     * A map of the Spine common Entity Columns to their default values.
     *
     * <p>Some write operations may not include these columns. Though, they are required for
     * the framework to work properly. Hence, the tables which include them should make these
     * values {@code DEFAULT} for these columns.
     *
     * <p>The map stores the names of the Entity Columns as a string keys for simplicity and
     * the default values of the Columns as the map values.
     */
    private static final ImmutableMap<String, Object> COLUMN_DEFAULTS =
            ImmutableMap.of(archived.name(), false,
                            deleted.name(), false,
                            version.name(), 0);
    private final Class<? extends Entity<I, ?>> entityClass;

    /**
     * Creates a new instance of the {@code EntityTable}.
     *
     * <p>The table will have a name based on the FQN name of the given {@link Entity} class.
     *
     * @param entityClass
     *         the {@link Class} of the {@link Entity} to store
     * @param dataSource
     *         an instance of {@link DataSourceWrapper} to use
     */
    protected EntityTable(Class<? extends Entity<I, ?>> entityClass,
                          String idColumnName,
                          DataSourceWrapper dataSource,
                          TypeMapping typeMapping) {
        this("", entityClass, idColumnName, dataSource, typeMapping);
    }

    /**
     * Creates a new instance of the {@code EntityTable}.
     *
     * <p>The table will have a name based on the FQN name of
     * the given {@link Entity} class and the given postfix.
     *
     * @param tableNamePostfix
     *         the postfix for the the table name
     * @param entityClass
     *         the {@link Class} of the {@link Entity} to store
     * @param dataSource
     *         an instance of {@link DataSourceWrapper} to use
     */
    protected EntityTable(String tableNamePostfix,
                          Class<? extends Entity<I, ?>> entityClass,
                          String idColumnName,
                          DataSourceWrapper dataSource,
                          TypeMapping typeMapping) {
        super(newTableName(entityClass) + tableNamePostfix,
              IdColumn.newInstance(entityClass, idColumnName),
              dataSource,
              typeMapping);
        this.entityClass = entityClass;
    }

    @Override
    protected ImmutableMap<String, Object> columnDefaults() {
        return COLUMN_DEFAULTS;
    }

    // TODO:2019-07-18:dmytro.kuzmin:WIP: Seems unused, remove together with the field.
    public Class<? extends Entity<I, ?>> entityClass() {
        return entityClass;
    }
}
