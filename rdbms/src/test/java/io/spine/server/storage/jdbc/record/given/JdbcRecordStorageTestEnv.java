/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record.given;

import io.spine.base.Identifier;
import io.spine.client.CompositeFilter;
import io.spine.client.Filter;
import io.spine.client.OrderBy;
import io.spine.client.TargetFilters;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.test.storage.ProjectId;

import static io.spine.base.Identifier.newUuid;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static io.spine.client.Filters.lt;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.client.OrderBy.Direction.DESCENDING;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.test.storage.Project.Status.CANCELLED;

/**
 * Test environment
 * for {@link io.spine.server.storage.jdbc.record.JdbcRecordStorageTest JdbcRecordStorageTest}.
 */
public final class JdbcRecordStorageTestEnv {

    private static final String STATUS_VALUE_COLNAME = "project_status_value";

    private JdbcRecordStorageTestEnv() {
    }

    public static ProjectId unpackProjectId(EntityRecord next) {
        return AnyPacker.unpack(next.getEntityId(), ProjectId.class);
    }

    public static ProjectId projectId() {
        return ProjectId
                .newBuilder()
                .setId(newUuid())
                .build();
    }

    public static String statusValueColumn() {
        return STATUS_VALUE_COLNAME;
    }

    public static EntityRecord asEntityRecord(ProjectId id, TestCounterEntity entity) {
        return EntityRecord
                .newBuilder()
                .setEntityId(Identifier.pack(id))
                .setState(pack(entity.state()))
                .setVersion(entity.version())
                .setLifecycleFlags(entity.lifecycleFlags())
                .build();
    }

    public static OrderBy ascendingByStatusValue() {
        return orderByStatusValue(ASCENDING);
    }

    public static OrderBy descendingByStatusValue() {
        return orderByStatusValue(DESCENDING);
    }

    private static OrderBy orderByStatusValue(OrderBy.Direction direction) {
        return OrderBy.newBuilder()
                      .setColumn(statusValueColumn())
                      .setDirection(direction)
                      .vBuild();
    }

    public static EntityQuery<ProjectId> queryAllBeforeCancelled(JdbcRecordStorage<ProjectId> storage) {
        Filter lessThan = lt(statusValueColumn(), CANCELLED.getNumber());
        CompositeFilter columnFilter = CompositeFilter
                .newBuilder()
                .addFilter(lessThan)
                .setOperator(ALL)
                .build();
        TargetFilters filters = TargetFilters
                .newBuilder()
                .addFilter(columnFilter)
                .build();
        EntityQuery<ProjectId> query = EntityQueries.from(filters, storage);
        return query;
    }
}
