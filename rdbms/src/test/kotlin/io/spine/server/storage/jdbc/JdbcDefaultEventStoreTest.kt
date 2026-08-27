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

package io.spine.server.storage.jdbc

import io.spine.environment.Tests
import io.spine.server.ServerEnvironment
import io.spine.server.event.store.DefaultEventStoreTest
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.DisplayName

/**
 * Runs the [DefaultEventStoreTest] contract against [JdbcStorageFactory]
 * over an in-memory H2 database.
 *
 * The base suite builds a Bounded Context in its `@BeforeEach`, which runs
 * before any `@BeforeEach` of this class could. JUnit creates a new test
 * instance per test method, so the constructor of this class is the only
 * seam ahead of the base setup: it points the test server environment to
 * a fresh storage factory — and thus a fresh database — for every test,
 * keeping the persistent event tables from leaking between the tests.
 */
@DisplayName("JDBC-backed `EventStore` should")
internal class JdbcDefaultEventStoreTest : DefaultEventStoreTest() {

    init {
        ServerEnvironment.`when`(Tests::class.java).use(freshFactory())
    }

    companion object {

        private val factories = mutableListOf<JdbcStorageFactory>()

        /**
         * Creates a new factory over a fresh in-memory H2 database,
         * remembering it for the after-all cleanup.
         */
        private fun freshFactory(): JdbcStorageFactory =
            h2Factory().also { factories.add(it) }

        /**
         * Detaches the storage configuration from the test server environment
         * and closes the factories created for the test methods.
         */
        @AfterAll
        @JvmStatic
        fun resetEnvironment() {
            ServerEnvironment.instance().reset()
            factories.forEach(JdbcStorageFactory::close)
            factories.clear()
        }
    }
}
