/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.util;

import org.spine3.Internal;
import org.spine3.server.Entity;
import org.spine3.server.EntityId;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.base.Identifiers.idToString;

/**
 * Helps to work with entity ID columns.
 *
 * @see EntityId
 * @author Alexander Litus
 */
@Internal
public abstract class IdColumn<ID> {

    private static final String TYPE_VARCHAR = "VARCHAR(999)";

    private static final String TYPE_BIGINT = "BIGINT";

    private static final String TYPE_INT = "INT";

    /**
     * Creates a new instance.
     *
     * @param entityClass a class of an entity or an aggregate
     * @param <ID> the type of entity IDs
     * @return a new helper instance
     */
    @SuppressWarnings("IfMayBeConditional")
    public static <ID> IdColumn<ID> newInstance(Class<? extends Entity<ID, ?>> entityClass) {
        final IdColumn<ID> helper;
        final Class<ID> idClass = Entity.getIdClass(entityClass);
        if (Long.class.isAssignableFrom(idClass)) {
            helper = new LongIdColumn<>();
        } else if (Integer.class.isAssignableFrom(idClass)) {
            helper = new IntIdColumn<>();
        } else {
            helper = new StringOrMessageIdColumn<>();
        }
        return helper;
    }

    /**
     * Returns the SQL data type string of the ID column, e.g. {@code "BIGINT"}, {@code "VARCHAR(999)"}, etc.
     */
    public abstract String getColumnDataType();

    /**
     * Sets an ID parameter to the given value.
     *
     * @param index     the ID parameter index
     * @param id        the ID value to set
     * @param statement the statement to use
     */
    public abstract void setId(int index, ID id, PreparedStatement statement);

    private static class LongIdColumn<ID> extends IdColumn<ID> {

        @Override
        public String getColumnDataType() {
            return TYPE_BIGINT;
        }

        @Override
        public void setId(int index, ID id, PreparedStatement statement) {
            final Long idLong = (Long) id;
            try {
                statement.setLong(index, idLong);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private static class IntIdColumn<ID> extends IdColumn<ID> {

        @Override
        public String getColumnDataType() {
            return TYPE_INT;
        }

        @Override
        public void setId(int index, ID id, PreparedStatement statement) {
            final Integer idInt = (Integer) id;
            try {
                statement.setInt(index, idInt);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private static class StringOrMessageIdColumn<ID> extends IdColumn<ID> {

        @Override
        public String getColumnDataType() {
            return TYPE_VARCHAR;
        }

        @Override
        public void setId(int index, ID id, PreparedStatement statement) {
            final String idString = idToString(id);
            try {
                statement.setString(index, idString);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
