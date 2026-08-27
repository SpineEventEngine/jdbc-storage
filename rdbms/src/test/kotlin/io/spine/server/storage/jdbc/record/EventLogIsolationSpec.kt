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

package io.spine.server.storage.jdbc.record

import io.kotest.matchers.collections.shouldContainExactly
import io.spine.base.Identifier
import io.spine.core.Event
import io.spine.core.EventId
import io.spine.grpc.StreamObservers.memoizingObserver
import io.spine.server.ContextSpec
import io.spine.server.event.EventStore
import io.spine.server.event.EventStreamQuery
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.record.given.HistoryStorageTestEnv.h2Factory
import io.spine.test.storage.event.StgProjectCreated
import io.spine.test.storage.stgProjectId
import io.spine.testdata.Sample
import io.spine.testing.server.TestEventFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests that the event stores of distinct Bounded Contexts served by one
 * [JdbcStorageFactory] land in distinct DB tables.
 *
 * Since core-jvm groups the storage of an event store by the Bounded Context,
 * the storages of the two contexts below arrive at the factory with
 * distinct groups, and each `read` observes only the events appended
 * to its own context.
 */
@DisplayName("`JdbcStorageFactory`, when serving distinct Bounded Contexts, should")
internal class EventLogIsolationSpec {

    private lateinit var factory: JdbcStorageFactory

    @BeforeEach
    fun setUpFactory() {
        factory = h2Factory()
    }

    @AfterEach
    fun closeFactory() {
        factory.close()
    }

    @Test
    fun `store the events of each context in its own table`() {
        val billing = factory.createEventStore(ContextSpec.singleTenant("Billing"))
        val shipping = factory.createEventStore(ContextSpec.singleTenant("Shipping"))

        val billingEvent = newEvent()
        val shippingEvent = newEvent()
        billing.append(billingEvent)
        shipping.append(shippingEvent)

        idsOf(billing) shouldContainExactly listOf(billingEvent.id)
        idsOf(shipping) shouldContainExactly listOf(shippingEvent.id)
    }

    /**
     * Reads the identifiers of all events stored in the given store.
     */
    private fun idsOf(store: EventStore): List<EventId> {
        val observer = memoizingObserver<Event>()
        store.read(EventStreamQuery.getDefaultInstance(), observer)
        return observer.responses().map { it.id }
    }

    private fun newEvent(): Event {
        val producer = stgProjectId {
            id = "event-log-isolation"
        }
        val eventFactory = TestEventFactory.newInstance(
            Identifier.pack(producer),
            EventLogIsolationSpec::class.java
        )
        return eventFactory.createEvent(Sample.messageOfType(StgProjectCreated::class.java))
    }
}
