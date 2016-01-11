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

package org.spine3.server.storage.rdbms;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.spine3.server.Entity;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.*;

import static org.spine3.protobuf.Messages.getClassDescriptor;
import static org.spine3.util.Classes.getGenericParameterType;

/**
 * Creates storages based on HyperSQL Database.
 *
 * @author Alexander Litus
 */
public class HsqlStorageFactory implements StorageFactory {

    private static final String DATA_SOURCE_CLASS_NAME = "org.hsqldb.jdbc.JDBCDataSource";

    private static final int ENTITY_MESSAGE_TYPE_PARAMETER_INDEX = 1;

    private final HsqlDb database;

    /**
     * Creates a new instance with the specified data source configuration.
     *
     * <p>Please see required and optional config properties list and other documentation:
     * <ul>
     *     <li><a href="https://github.com/brettwooldridge/HikariCP#essentials">HikariCP config properties</a></li>
     *     <li><a href="https://github.com/brettwooldridge/HikariCP#initialization">HikariCP configuration sample</a></li>
     * </ul>
     * NOTE: {@code dataSourceClassName} config property is set automatically.
     *
     * <p>Examples of JDBC URL:
     *
     * <p>{@code jdbc:hsqldb:hsql://localhost:9001/dbname;ifexists=true}
     * <p>{@code jdbc:hsqldb:mem:inmemorydb} (for in-memory database)
     *
     * @param config the config used to create {@link HikariDataSource}.
     */
    public static HsqlStorageFactory newInstance(HikariConfig config) {
        return new HsqlStorageFactory(config);
    }

    private HsqlStorageFactory(HikariConfig config) {
        config.setDataSourceClassName(DATA_SOURCE_CLASS_NAME);
        this.database = HsqlDb.newInstance(config);
    }

    @Override
    public CommandStorage createCommandStorage() {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    @Override
    public EventStorage createEventStorage() {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    /**
     * NOTE: the parameter is not used.
     */
    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?>> unused) {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    @Override
    public <I, M extends Message> EntityStorage<I, M> createEntityStorage(Class<? extends Entity<I, M>> entityClass) {
        final Class<Message> messageClass = getGenericParameterType(entityClass, ENTITY_MESSAGE_TYPE_PARAMETER_INDEX);
        final Descriptors.Descriptor descriptor = (Descriptors.Descriptor) getClassDescriptor(messageClass);
        return HsqlEntityStorage.newInstance(database, descriptor);
    }

    @Override
    public void init() {
        // NOP
    }

    @Override
    public void close() {
        database.close();
    }
}
