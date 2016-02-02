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

package org.spine3.server.storage.jdbc;

import org.spine3.server.Identifiers;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Helps to work with entity IDs.
 *
 * @author Alexander Litus
 */
/*package*/ abstract class IdHelper<I> {

    private static final String TYPE_VARCHAR = "VARCHAR(999)";

    private static final String TYPE_BIGINT = "BIGINT";

    private static final String TYPE_INT = "INT";


    /**
     * Returns the type of ID column.
     */
    /*package*/ abstract String getIdColumnType();

    /**
     * Sets an ID parameter to the given value.
     *
     * @param index the ID parameter index
     * @param id the ID value to set
     * @param statement the statement to use
     */
    /*package*/ abstract void setId(int index, I id, PreparedStatement statement);

    /*package*/ static class LongIdHelper<I> extends IdHelper<I> {

        @Override
        /*package*/ String getIdColumnType() {
            return TYPE_BIGINT;
        }

        @Override
        /*package*/ void setId(int index, I id, PreparedStatement statement) {
            final Long idLong = (Long) id;
            try {
                statement.setLong(index, idLong);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /*package*/ static class IntIdHelper<I> extends IdHelper<I> {

        @Override
        /*package*/ String getIdColumnType() {
            return TYPE_INT;
        }

        @Override
        /*package*/ void setId(int index, I id, PreparedStatement statement) {
            final Integer idInt = (Integer) id;
            try {
                statement.setInt(index, idInt);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /*package*/ static class StringOrMessageIdHelper<I> extends IdHelper<I> {

        @Override
        /*package*/ String getIdColumnType() {
            return TYPE_VARCHAR;
        }

        @Override
        /*package*/ void setId(int index, I id, PreparedStatement statement) {
            final String idString = Identifiers.idToString(id);
            try {
                statement.setString(index, idString);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
