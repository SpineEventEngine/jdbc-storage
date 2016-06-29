package org.spine3.server.storage.jdbc.query.tables.projection;


import com.google.protobuf.Timestamp;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.ProjectionTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.validate.Validate.isDefault;

public class SelectTimestampQuery extends AbstractQuery{
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SELECT_QUERY = "SELECT " + ProjectionTable.SECONDS_COL + ", " + ProjectionTable.NANOS_COL + " FROM %s ;";

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @Nullable
    public Timestamp execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final long seconds = resultSet.getLong(ProjectionTable.SECONDS_COL);
            final int nanos = resultSet.getInt(ProjectionTable.NANOS_COL);
            final Timestamp time = Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
            if (isDefault(time)) {
                return null;
            }
            return time;
        } catch (SQLException e) {
            //log().error("Failed to read last event time.", e);
            throw new DatabaseException(e);
        }
    }

    public static Builder getBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(SELECT_QUERY, tableName));
        return builder;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, SelectTimestampQuery> {

        @Override
        public SelectTimestampQuery build() {
            return new SelectTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
