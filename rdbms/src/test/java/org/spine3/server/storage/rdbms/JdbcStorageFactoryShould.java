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

import com.google.protobuf.StringValue;
import org.junit.Test;
import org.spine3.server.Entity;
import org.spine3.server.storage.EntityStorage;

import static org.junit.Assert.assertNotNull;

/**
 * @author Alexander Litus
 */
@SuppressWarnings({"InstanceMethodNamingConvention", "MagicNumber"})
public class JdbcStorageFactoryShould {

    private static final DataSourceConfig CONFIG = DataSourceConfig.newBuilder()
            .setJdbcUrl("jdbc:hsqldb:mem:factorytests")
            .setUsername("SA")
            .setPassword("pwd")
            .setMaxPoolSize(12)
            .build();

    @Test
    public void create_entity_storage() {
        final JdbcStorageFactory factory = JdbcStorageFactory.newInstance(CONFIG);
        final EntityStorage<String> storage = factory.createEntityStorage(TestEntity.class);
        assertNotNull(storage);
    }

    @Test
    public void create_entity_storage_if_intity_class_is_inner() {
        final JdbcStorageFactory factory = JdbcStorageFactory.newInstance(CONFIG);
        final EntityStorage<String> storage = factory.createEntityStorage(TestEntity.InnerTestEntity.class);
        assertNotNull(storage);
    }

    public static class TestEntity extends Entity<String, StringValue> {

        public TestEntity(String id) {
            super(id);
        }

        @Override
        protected StringValue getDefaultState() {
            return StringValue.getDefaultInstance();
        }

        public static class InnerTestEntity extends Entity<String, StringValue> {

            public InnerTestEntity(String id) {
                super(id);
            }

            @Override
            protected StringValue getDefaultState() {
                return StringValue.getDefaultInstance();
            }
        }
    }
}
