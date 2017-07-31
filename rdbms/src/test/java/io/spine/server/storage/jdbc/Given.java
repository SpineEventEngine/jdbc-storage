/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc;

import com.google.protobuf.Message;
import io.spine.core.Version;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.TestTransaction;
import io.spine.server.stand.StandStorage;
import io.spine.test.commandservice.customer.Customer;
import io.spine.test.commandservice.customer.CustomerVBuilder;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.test.storage.ProjectVBuilder;

import java.util.LinkedList;
import java.util.List;

class Given {

    private Given() {
        // Prevent utility class instantiation.
    }

    static StandStorage newStorage() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                GivenDataSource.DEFAULT_TABLE_NAME);
        final StandStorage storage = JdbcStandStorage.<String>newBuilder()
                                                     .setDataSource(dataSource)
                                                     .build();
        return storage;
    }

    static class TestAggregate extends Aggregate<String, Project, ProjectVBuilder> {

        /**
         * Creates a new aggregate instance.
         *
         * @param id the ID for the new aggregate
         * @throws IllegalArgumentException if the ID is not of one of the supported types
         */
        TestAggregate(String id) {
            super(id);
        }

        private void setState(Message state) {
            TestTransaction.injectState(this, state, Version.getDefaultInstance());
        }
    }

    static class TestAggregate2 extends Aggregate<String, Customer, CustomerVBuilder> {

        /**
         * Creates a new aggregate instance.
         *
         * @param id the ID for the new aggregate
         * @throws IllegalArgumentException if the ID is not of one of the supported types
         */
        TestAggregate2(String id) {
            super(id);
        }
    }

    static List<TestAggregate> testAggregates(int amount) {
        final List<TestAggregate> aggregates = new LinkedList<>();

        for (int i = 0; i < amount; i++) {
            aggregates.add(new TestAggregate(String.valueOf(i)));
        }

        return aggregates;
    }

    static List<TestAggregate> testAggregatesWithState(int amount) {
        final List<TestAggregate> aggregates = new LinkedList<>();

        for (int i = 0; i < amount; i++) {
            final TestAggregate aggregate = new TestAggregate(
                    String.valueOf(i));
            final ProjectId projectId = ProjectId.newBuilder()
                                                 .setId(aggregate.getId())
                                                 .build();
            final Project state = Project.newBuilder()
                                         .setId(projectId)
                                         .setName("Some project")
                                         .setStatus(Project.Status.CREATED)
                                         .build();

            aggregate.setState(state);

            aggregates.add(aggregate);
        }

        return aggregates;
    }
}