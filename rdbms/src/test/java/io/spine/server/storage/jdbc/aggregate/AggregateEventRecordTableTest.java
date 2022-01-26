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

package io.spine.server.storage.jdbc.aggregate;

//TODO:2021-07-01:alex.tymchenko: kill.
//@DisplayName("AggregateEventRecordTable should")
class AggregateEventRecordTableTest {

//    @Test
//    @DisplayName("throw ISE on attempt to update event record")
//    void throwOnEventRecordUpdate() {
//        AggregateEventRecordTable<String> table =
//                new AggregateEventRecordTable<>(AnAggregate.class,
//                                                whichIsStoredInMemory(newUuid()),
//                                                H2_1_4);
//        assertThrows(IllegalStateException.class,
//                     () -> table.update(newUuid(), AggregateEventRecord.getDefaultInstance()));
//    }
//
//    @Test
//    @DisplayName("store record kind in string representation")
//    void storeRecordKind() {
//        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
//        AggregateEventRecordTable<String> table =
//                new AggregateEventRecordTable<>(AnAggregate.class, dataSource, H2_1_4);
//        table.create();
//
//        Snapshot snapshot = Snapshot.newBuilder()
//                                    .setVersion(newVersion(5, currentTime()))
//                                    .build();
//        AggregateEventRecord record = AggregateEventRecord.newBuilder()
//                                                          .setSnapshot(snapshot)
//                                                          .build();
//        InsertAggregateRecordQuery<String> query = table.composeInsertQuery(newUuid(), record);
//        query.execute();
//
//        String expectedKind = record.getKindCase()
//                                    .toString();
//        String actualKind = query.factory()
//                                 .select(path(String.class, KIND.name()))
//                                 .from(query.table())
//                                 .fetchFirst();
//        assertEquals(expectedKind, actualKind);
//    }
}
