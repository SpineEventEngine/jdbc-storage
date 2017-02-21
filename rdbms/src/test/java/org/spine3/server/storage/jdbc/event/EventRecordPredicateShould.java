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

package org.spine3.server.storage.jdbc.event;

import org.junit.Test;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.event.JdbcEventStorage.EventRecordPredicate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov.
 */
public class EventRecordPredicateShould {

    @Test
    public void be_true_if_filters_are_empty() {
        final EventRecordPredicate predicate = new EventRecordPredicate(
                Collections.<EventFilter>emptyList());
        final boolean result = predicate.apply(EventStorageRecord.getDefaultInstance());
        assertTrue(result);
    }

    @Test
    public void be_false_for_null_records() {
        final EventRecordPredicate predicate = new EventRecordPredicate(
                Collections.<EventFilter>emptyList());
        final boolean result = predicate.apply(null);
        assertFalse(result);
    }

    @Test
    public void be_true_if_all_filters_are_empty() {
        final Collection<EventFilter> filters = Arrays.asList(
                EventFilter.getDefaultInstance(),
                EventFilter.getDefaultInstance());
        final EventRecordPredicate predicate = new EventRecordPredicate(filters);
        final boolean result = predicate.apply(EventStorageRecord.getDefaultInstance());
        assertTrue(result);
    }
}
