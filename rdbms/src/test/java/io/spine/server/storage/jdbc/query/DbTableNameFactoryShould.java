/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.StringValue;
import io.spine.server.entity.AbstractEntity;
import org.junit.Test;

import static io.spine.server.storage.jdbc.query.DbTableNameFactory.newTableName;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alexander Litus
 */
public class DbTableNameFactoryShould {

    private final Class<TestEntity> entityClass = TestEntity.class;

    @Test
    public void have_private_utility_constructor() {
        assertHasPrivateParameterlessCtor(DbTableNameFactory.class);
    }

    @Test
    public void pass_null_tolerance_check() {
        final NullPointerTester tester = new NullPointerTester();
        tester.testStaticMethods(DbTableNameFactory.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    public void return_table_name_which_starts_with_entity_class_name() {
        final String tableName = newTableName(entityClass);
        final String className = entityClass.getSimpleName();
        assertTrue(tableName.startsWith(className));
    }

    @Test
    public void produce_same_name_for_same_class() {
        assertEquals(newTableName(entityClass), newTableName(entityClass));
    }

    private static class TestEntity extends AbstractEntity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }
}
