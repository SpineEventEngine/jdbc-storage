/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;

import java.io.Serializable;
import java.util.Comparator;

import static com.google.protobuf.util.Timestamps.toMillis;
import static io.spine.json.Json.toCompactJson;
import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.util.Exceptions.newIllegalArgumentException;

public final class DefaultColumnTypeRegistry implements ColumnTypeRegistry {

    private static final ImmutableSortedMap<Class<?>, PersistenceStrategy<?>> defaultStrategies =
            defaultPolicies();

    @SuppressWarnings("unchecked") // Ensured by `defaultPolicies` declaration.
    @Override
    public <T> PersistenceStrategy<T> persistenceStrategyOf(Class<T> clazz) {
        PersistenceStrategy<?> strategy = defaultStrategies.get(clazz);
        if (strategy == null) {
            strategy = searchForSuperclassStrategy(clazz);
        }
        PersistenceStrategy<T> result = (PersistenceStrategy<T>) strategy;
        return result;
    }

    private static <T> PersistenceStrategy<?> searchForSuperclassStrategy(Class<T> clazz) {
        PersistenceStrategy<?> result =
                defaultPolicies().keySet()
                                 .stream()
                                 .filter(cls -> cls.isAssignableFrom(clazz))
                                 .map(defaultStrategies::get)
                                 .findFirst()
                                 .orElseThrow(() -> classNotFound(clazz));
        return result;
    }

    private static <T> IllegalArgumentException classNotFound(Class<T> clazz) {
        throw newIllegalArgumentException("The class %s is not found among registered types.",
                                          clazz.getCanonicalName());
    }

    private static ImmutableSortedMap<Class<?>, PersistenceStrategy<?>> defaultPolicies() {
        ImmutableSortedMap.Builder<Class<?>, PersistenceStrategy<?>> policies =
                ImmutableSortedMap.orderedBy(new SimplisticClassComparator());

        policies.put(String.class, new DefaultStringPersistenceStrategy());
        policies.put(Integer.class, new DefaultIntegerPersistenceStrategy());
        policies.put(Long.class, new DefaultLongPersistenceStrategy());
        policies.put(Boolean.class, new DefaultBooleanPersistenceStrategy());
        policies.put(ByteString.class, new DefaultByteStringPersistenceStrategy());
        policies.put(Timestamp.class, new DefaultTimestampPersistenceStrategy());
        policies.put(Version.class, new DefaultVersionPersistenceStrategy());
        policies.put(Enum.class, new DefaultEnumPersistenceStrategy());
        policies.put(Message.class, new DefaultMessagePersistenceStrategy());

        return policies.build();
    }

    @Override
    public Type typeOf(Class<?> clazz) {
        return null;
    }

    /**
     * A class comparator for the {@linkplain #defaultStrategies} map.
     *
     * <p>Compares classes in such a way so the subclasses go <b>before</b> their superclasses.
     *
     * <p>For the classes without "parent-child" relationship there is no predefined order of
     * storing.
     */
    private static class SimplisticClassComparator implements Comparator<Class<?>>, Serializable {

        private static final long serialVersionUID = 0L;

        @Override
        public int compare(Class<?> o1, Class<?> o2) {
            if (o1.equals(o2)) {
                return 0;
            }
            if (o1.isAssignableFrom(o2)) {
                return 1;
            }
            return -1;
        }
    }

    private static class DefaultStringPersistenceStrategy implements PersistenceStrategy<String> {

        @Override
        public Object apply(String s) {
            return s;
        }

        @Override
        public Type persistAs() {
            return STRING;
        }
    }

    private static class DefaultIntegerPersistenceStrategy implements PersistenceStrategy<Integer> {

        @Override
        public Object apply(Integer integer) {
            return integer;
        }

        @Override
        public Type persistAs() {
            return INT;
        }
    }

    private static class DefaultLongPersistenceStrategy implements PersistenceStrategy<Long> {

        @Override
        public Object apply(Long aLong) {
            return aLong;
        }

        @Override
        public Type persistAs() {
            return LONG;
        }
    }

    private static class DefaultBooleanPersistenceStrategy
            implements PersistenceStrategy<Boolean> {

        @Override
        public Object apply(Boolean aBoolean) {
            return aBoolean;
        }

        @Override
        public Type persistAs() {
            return BOOLEAN;
        }
    }

    private static class DefaultByteStringPersistenceStrategy
            implements PersistenceStrategy<ByteString> {

        @Override
        public Object apply(ByteString bytes) {
            // TODO:2017-07-21:dmytro.kuzmin:WIP Double check.
            return bytes.toByteArray();
        }

        @Override
        public Type persistAs() {
            return BYTE_ARRAY;
        }
    }

    private static class DefaultTimestampPersistenceStrategy
            implements PersistenceStrategy<Timestamp> {

        @Override
        public Object apply(Timestamp timestamp) {
            return toMillis(timestamp);
        }

        @Override
        public Type persistAs() {
            return LONG;
        }
    }

    private static class DefaultVersionPersistenceStrategy
            implements PersistenceStrategy<Version> {

        @Override
        public Object apply(Version version) {
            return version.getNumber();
        }

        @Override
        public Type persistAs() {
            return INT;
        }
    }

    private static class DefaultEnumPersistenceStrategy implements PersistenceStrategy<Enum<?>> {

        @Override
        public Object apply(Enum<?> anEnum) {
            return anEnum.ordinal();
        }

        @Override
        public Type persistAs() {
            return INT;
        }
    }

    private static class DefaultMessagePersistenceStrategy
            implements PersistenceStrategy<Message> {

        @Override
        public Object apply(Message message) {
            return toCompactJson(message);
        }

        @Override
        public Type persistAs() {
            return STRING;
        }
    }
}
