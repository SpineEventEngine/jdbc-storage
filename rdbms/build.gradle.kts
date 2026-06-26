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

import io.spine.dependency.lib.Grpc
import io.spine.dependency.lib.Slf4J
import io.spine.dependency.local.CoreJvm
import io.spine.dependency.storage.H2
import io.spine.dependency.storage.Hikari
import io.spine.dependency.storage.HsqlDb
import io.spine.dependency.storage.MySql
import io.spine.dependency.storage.PostgreSql
import io.spine.dependency.storage.QueryDsl
import io.spine.dependency.test.Testcontainers

plugins {
    module
    id("io.spine.core-jvm")
}

dependencies {
    implementation(CoreJvm.server)

    api(QueryDsl.sql) {
        exclude(group = "com.google.guava")
    }
    implementation(Hikari.lib)

    testImplementation(CoreJvm.serverTestLib)

    // The test fixtures of `spine-server` provide the shared test message types
    // (e.g. `io.spine.test.storage.StgProject`) used by the storage tests.
    //
    // The capability is requested explicitly because `spine-server` publishes its
    // test fixtures under the `io.spine:server-test-fixtures` capability (derived from
    // the `server` module name), which does not match the name the `testFixtures(...)`
    // helper would expect for the `spine-`-prefixed artifact.
    testImplementation(CoreJvm.server) {
        capabilities {
            requireCapability("io.spine:server-test-fixtures")
        }
    }

    // `EntityRecordStorageTest` is a concrete contract test published in the `tests`-classified
    // JAR of `spine-server` (it is not part of the `server-test-fixtures` set, unlike the other
    // storage-contract bases). It is consumed here so that `JdbcEntityRecordStorageTest` can
    // reuse it against the JDBC-backed storage.
    testImplementation("${CoreJvm.server}:test")

    testImplementation(Grpc.stub)
    testImplementation(HsqlDb.lib)
    testImplementation(H2.lib)
    testImplementation(Testcontainers.lib)
    testImplementation(Testcontainers.junitJupiter)
    testImplementation(Testcontainers.mySql) {
        exclude(group = "org.jetbrains")
    }
    testImplementation(MySql.connector)
    testImplementation(Testcontainers.postgresql) {
        exclude(group = "org.jetbrains")
    }
    testImplementation(PostgreSql.connector)

    testRuntimeOnly(Slf4J.simple)
}

/**
 * Hardens the resource-heavy storage test suite.
 *
 * Each test class spins up a bounded context and a JDBC connection pool. Run in a single JVM,
 * the full suite accumulates enough resources (pooled connections, executor threads, contexts)
 * to crash the test worker before it finishes. We enlarge the worker heap and recycle the worker
 * every few classes to release those resources.
 */
tasks.withType<Test>().configureEach {
    maxHeapSize = "2g"
    setForkEvery(10)
}
