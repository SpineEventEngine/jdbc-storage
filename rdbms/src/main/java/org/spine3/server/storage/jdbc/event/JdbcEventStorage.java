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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.Event;
import org.spine3.base.EventContext;
import org.spine3.base.EventId;
import org.spine3.base.FieldFilter;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStorage;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.builder.StorageBuilder;
import org.spine3.server.storage.jdbc.event.query.EventStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbIterator;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static org.spine3.server.storage.jdbc.util.Closeables.closeAll;

/**
 * The implementation of the event storage based on the RDBMS.
 *
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
public class JdbcEventStorage extends EventStorage {

    private final DataSourceWrapper dataSource;

    private final EventStorageQueryFactory queryFactory;

    /**
     * Iterators which are not closed yet.
     */
    private final Collection<DbIterator> iterators = newLinkedList();

    /**
     * Creates a new storage instance.
     *
     * @param dataSource   the dataSource wrapper
     * @param multitenant  defines if this storage is multitenant or not
     * @param queryFactory factory that will generate queries for interaction with event table
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    public static JdbcEventStorage newInstance(DataSourceWrapper dataSource,
                                               boolean multitenant,
                                               EventStorageQueryFactory queryFactory)
            throws DatabaseException {
        return new JdbcEventStorage(dataSource, multitenant, queryFactory);
    }

    protected JdbcEventStorage(DataSourceWrapper dataSource,
                               boolean multitenant,
                               EventStorageQueryFactory queryFactory)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);
        queryFactory.newCreateEventTableQuery()
                    .execute();
    }

    private JdbcEventStorage(Builder builder) {
        this(builder.getDataSource(), builder.isMultitenant(), builder.getQueryFactory());
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>NOTE:</b> it is required to call {@link Iterator#hasNext()} before
     * {@link Iterator#next()}.
     *
     * @return a wrapped {@link DbIterator} instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<Event> iterator(EventStreamQuery query) throws DatabaseException {
        checkNotClosed();
        checkNotNull(query);

        final Iterator<Event> iterator = queryFactory.newFilterAndSortQuery(query)
                                                     .execute();
        iterators.add((DbIterator) iterator);

        final UnmodifiableIterator<Event> filtered = filterEvents(iterator, query);
        return filtered;
    }

    /**
     * Filters the Recieved {@linkplain Event Events} by the {@code FieldFilter}s for
     * the event message and {@linkplain EventContext}.
     *
     * <p>As each {@code event} data is stored as a single serialized {@code Message}, it
     * is not possible to perform this filtering directly within an SQL query.
     */
    private static UnmodifiableIterator<Event> filterEvents(Iterator<Event> unfilteredEvents,
                                                            EventStreamQuery query) {
        final List<EventFilter> filterList = query.getFilterList();
        final UnmodifiableIterator<Event> filtered =
                Iterators.filter(unfilteredEvents, new EventRecordPredicate(filterList));
        return filtered;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void write(EventId id, Event event) throws DatabaseException {
        checkNotClosed();
        checkNotNull(id);
        checkNotNull(event);

        final String eventId = id.getUuid();
        if (containsRecord(eventId)) {
            queryFactory.newUpdateEventQuery(eventId, event)
                        .execute();
        } else {
            queryFactory.newInsertEventQuery(eventId, event)
                        .execute();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public Optional<Event> read(EventId eventId) throws DatabaseException {
        checkNotClosed();
        checkNotNull(eventId);

        final String id = eventId.getUuid();
        final Event record = queryFactory.newSelectEventByIdQuery(id)
                                         .execute();
        return Optional.fromNullable(record);
    }

    private boolean containsRecord(String id) {
        final Event record = queryFactory.newSelectEventByIdQuery(id)
                                         .execute();
        final boolean contains = record != null;
        return contains;
    }

    @Override
    public void close() throws DatabaseException {
        checkNotClosed();

        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        closeAll(iterators);
        iterators.clear();
        dataSource.close();
    }

    public static class Builder extends StorageBuilder<Builder, JdbcEventStorage, EventStorageQueryFactory> {

        private Builder() {
            super();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        public JdbcEventStorage doBuild() {
            return new JdbcEventStorage(this);
        }
    }

    /**
     * Predicate matching an {@link Event} to a number of {@link EventFilter} instances.
     */
    @VisibleForTesting
    static class EventRecordPredicate implements Predicate<Event> {

        private final Collection<EventFilter> eventFilters;

        @VisibleForTesting
        EventRecordPredicate(Collection<EventFilter> eventFilters) {
            this.eventFilters = eventFilters;
        }

        @Override
        public boolean apply(@Nullable Event eventRecord) {
            if (eventRecord == null) {
                return false;
            }

            if (eventFilters.isEmpty()) {
                return true;
            }

            boolean nonEmptyFilterPresent = false;
            for (EventFilter filter : eventFilters) {
                final EventFilterChecker predicate = new EventFilterChecker(filter);
                if (predicate.checkFilterEmpty()) {
                    continue;
                }
                nonEmptyFilterPresent = true;
                final boolean matches = predicate.apply(eventRecord);
                if (matches) {
                    return true;
                }
            }

            return !nonEmptyFilterPresent;
        }
    }

    /**
     * Predicate matching an {@link Event} stored as {@link Event} to a single {@link EventFilter}.
     */
    private static class EventFilterChecker implements Predicate<Event> {

        private final Collection<FieldFilter> eventFieldFilters;
        private final Collection<FieldFilter> contextFieldFilters;

        private static final Function<Any, Message> ANY_UNPACKER = new Function<Any, Message>() {
            @Nullable
            @Override
            public Message apply(@Nullable Any input) {
                if (input == null) {
                    return null;
                }

                return AnyPacker.unpack(input);
            }
        };

        private EventFilterChecker(EventFilter eventFilter) {
            this.eventFieldFilters = eventFilter.getEventFieldFilterList();
            this.contextFieldFilters = eventFilter.getContextFieldFilterList();
        }

        private boolean checkFilterEmpty() {
            return eventFieldFilters.isEmpty() && contextFieldFilters.isEmpty();
        }

        // Defined as nullable, parameter `event` is actually non null.
        @SuppressWarnings({"MethodWithMoreThanThreeNegations", "MethodWithMultipleLoops"})
        @Override
        public boolean apply(@Nullable Event event) {
            if (event == null) {
                return false;
            }

            if (!eventFieldFilters.isEmpty()) {
                final Any eventWrapped = event.getMessage();
                final Message eventMessage = AnyPacker.unpack(eventWrapped);

                // Check event fields
                for (FieldFilter filter : eventFieldFilters) {
                    final boolean matchesFilter = checkFields(eventMessage, filter);
                    if (!matchesFilter) {
                        return false;
                    }
                }
            }

            // Check context fields
            final EventContext context = event.getContext();
            for (FieldFilter filter : contextFieldFilters) {
                final boolean matchesFilter = checkFields(context, filter);
                if (!matchesFilter) {
                    return false;
                }
            }

            return true;
        }

        private static boolean checkFields(Message object, FieldFilter filter) {
            final String fieldPath = filter.getFieldPath();
            final String fieldName = fieldPath.substring(fieldPath.lastIndexOf('.') + 1);
            checkArgument(!Strings.isNullOrEmpty(fieldName),
                          "Field filter " + filter.toString() + " is invalid");
            final String fieldGetterName = "get" + fieldName.substring(0, 1)
                                                            .toUpperCase() + fieldName.substring(1);

            final Collection<Any> expectedAnys = filter.getValueList();
            final Collection<Message> expectedValues = Collections2.transform(expectedAnys,
                                                                              ANY_UNPACKER);
            Message actualValue;
            try {
                final Class<?> messageClass = object.getClass();
                final Method fieldGetter = messageClass.getDeclaredMethod(fieldGetterName);
                actualValue = (Message) fieldGetter.invoke(object);
                if (actualValue instanceof Any) {
                    actualValue = AnyPacker.unpack((Any) actualValue);
                }
            } catch (@SuppressWarnings("OverlyBroadCatchBlock") ReflectiveOperationException e) {
                // Catch several checked reflection exceptions that should never happen
                throw new IllegalStateException(e);
            }

            final boolean result = expectedValues.contains(actualValue);
            return result;
        }
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEventStorage.class);
    }
}
