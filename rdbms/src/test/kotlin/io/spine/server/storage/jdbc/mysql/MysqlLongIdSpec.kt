/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.mysql

import com.google.protobuf.StringValue
import io.kotest.matchers.optional.shouldBePresent
import io.kotest.matchers.shouldBe
import io.spine.server.storage.RecordSpec
import io.spine.server.storage.Storage
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singleTenantSpec
import io.spine.test.storage.StgProject
import io.spine.test.storage.StgProjectId
import io.spine.test.storage.stgProject
import io.spine.test.storage.stgProjectId
import io.spine.testing.SlowTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * A regression test for [issue #164](https://github.com/SpineEventEngine/jdbc-storage/issues/164).
 *
 * The ID column used to be `VARCHAR(255)`, so storing a record whose (possibly serialized)
 * identifier was longer than 255 characters failed on MySQL with a `MysqlDataTruncation`
 * ("Data too long for column") error. The ID column is now `VARCHAR(512)`, both for plain
 * `String` identifiers and for message identifiers serialized to JSON.
 *
 * These tests run against a real MySQL server, reproducing the original failure environment.
 */
@DisplayName(
    "`JdbcRecordStorage` on MySQL should store a record whose identifier exceeds 255 characters"
)
@SlowTest
@EnableConditionally
internal class MysqlLongIdSpec {

    @Test
    fun `when the identifier is a 'String'`() {
        val factory = MysqlTests.newFactory()
        val spec = RecordSpec(
            String::class.java,
            StringValue::class.java,
            StringValue::getValue
        )
        val storage: Storage<String, StringValue> =
            factory.createRecordStorage(singleTenantSpec(), spec)

        val longId = "i".repeat(LONG_ID_LENGTH)
        val record = StringValue.of(longId)

        storage.write(longId, record)

        val stored = storage.read(longId).shouldBePresent()
        stored shouldBe record
    }

    @Test
    fun `when the identifier is a message serialized to a long 'String'`() {
        val factory = MysqlTests.newFactory()
        val spec = RecordSpec(
            StgProjectId::class.java,
            StgProject::class.java,
            StgProject::getId
        )
        val storage: Storage<StgProjectId, StgProject> =
            factory.createRecordStorage(singleTenantSpec(), spec)

        val projectId = stgProjectId { id = "i".repeat(LONG_ID_LENGTH) }
        val project = stgProject {
            id = projectId
            name = "A project with a long identifier"
        }

        storage.write(projectId, project)

        val stored = storage.read(projectId).shouldBePresent()
        stored shouldBe project
    }

    companion object {

        /**
         * The length of the identifiers used in these tests.
         *
         * It is well above the former `VARCHAR(255)` limit of the ID column, yet within the
         * `VARCHAR(512)` width now used, so the identifiers are stored without truncation. The
         * serialized form of the message identifier is longer still, and so also overflows the
         * old limit.
         */
        private const val LONG_ID_LENGTH = 300
    }
}
