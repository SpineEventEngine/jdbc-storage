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
package io.spine.server.entity.storage;

import io.spine.annotation.Internal;
import io.spine.server.entity.Entity;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.sort;

/**
 * A utility for working with the Entity Columns within the JDBC
 * {@linkplain io.spine.server.storage.Storage storage} implementation.
 *
 * @author Alexander Aleksandrov
 */
@Internal
public final class EntityColumns {

    private EntityColumns() {
        // Prevent utility class instantiation.
    }

    /**
     * Retrieves the {@linkplain Column Entity Columns} from the given {@linkplain Entity}
     * {@linkplain Class class} description.
     *
     * @param cls the type of the {@link Entity} to get the Columns from
     * @return the Entity Columns declared within this {@link Entity}
     */
    public static Collection<Column> getColumns(Class<? extends Entity<?, ?>> cls) {
        final Collection<Column> result = Columns.getColumns(cls);
        return result;
    }

    /**
     * Sorts the given Entity Column names.
     *
     * <p>Use this method to order the Entity Column for an SQL query.
     *
     * @param columns the column names to sort
     * @return a {@code Collection} of sorted column names
     */
    public static Collection<String> sorted(Iterable<String> columns) {
        final List<String> list = newArrayList(columns);
        sort(list);
        return list;
    }
}
