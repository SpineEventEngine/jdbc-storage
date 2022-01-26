/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.newrecord;

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.storage.RecordStorageDelegateTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;

@DisplayName("`JdbcRecordStorage` should")
class NewRecordStorageTest extends RecordStorageDelegateTest {

    @BeforeEach
    @Override
    protected void setUpAbstractStorageTest() {
        ServerEnvironment.when(Tests.class)
                         .useStorageFactory((env) -> newFactory());
        super.setUpAbstractStorageTest();
    }

    @AfterAll
    static void tearDownClass() {
        ServerEnvironment.instance().reset();
    }

    //TODO:2021-07-01:alex.tymchenko: adapt this one.
//    @Test
//    @DisplayName("persist entity columns beside the corresponding record")
//    @SuppressWarnings("ProtoTimestampGetSecondsGetNano") /* Compares points in time.*/
//    void testPersistColumns() {
//        StgProjectId id = newId();
//        StgProject project = newStorageRecord(id);
//        Version expectedVersion = Versions.newVersion(42, currentTime());
//        Timestamp expectedDueDate = currentTime();
//        StgProject.Status expectedStatus = StgProject.Status.STARTED;
//        project = project
//                .toBuilder()
//                .setProjectVersion(expectedVersion)
//                .setDueDate(expectedDueDate)
//                .setStatus(expectedStatus)
//                .vBuild();
//        MessageRecordSpec<StgProjectId, StgProject> spec =
//                new MessageRecordSpec<>(StgProjectId.class, StgProject.class, StgProject::getId);
//        RecordWithColumns<StgProjectId, StgProject> record =
//                RecordWithColumns.create(project, spec);
//        storage().write(id, project);
//
//        // Read Datastore Entity
//        DatastoreWrapper datastore = datastoreFactory.newDatastoreWrapper(
//                storage().isMultitenant());
//        Key key = datastore.keyFor(Kind.of(StgProject.class), RecordId.ofEntityId(id));
//        Optional<Entity> readResult = datastore.read(key);
//        assertThat(readResult).isPresent();
//        Entity datastoreEntity = readResult.get();
//
//        // Check entity record
//        TypeUrl recordType = TypeUrl.from(StgProject.getDescriptor());
//        StgProject actualProject = Entities.toMessage(datastoreEntity, recordType);
//        assertEquals(project, actualProject);
//
//        // Check custom Columns
//        assertEquals(expectedStatus.name(),
//                     datastoreEntity.getString(status.name()
//                                                     .value()));
//        assertEquals(expectedVersion.getNumber(),
//                     datastoreEntity.getLong(project_version.name()
//                                                            .value()));
//        com.google.cloud.Timestamp actualDueDate =
//                datastoreEntity.getTimestamp(due_date.name()
//                                                     .value());
//        assertEquals(expectedDueDate.getSeconds(), actualDueDate.getSeconds());
//        assertEquals(expectedDueDate.getNanos(), actualDueDate.getNanos());
//    }

//    @BeforeEach
//    void setUp() {
//        storageFactory.();
//    }

//    @AfterEach
//    void tearDown() {
//        storageFactory.tearDown();
//    }
}
