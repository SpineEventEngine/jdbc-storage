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

package org.spine3.server.storage.jdbc.query;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Andrey Lavrov
 */
public abstract class Abstract{

    private final String query;
    protected final DataSourceWrapper dataSource;

    private String messageColumnName;
    private Descriptors.Descriptor messageDescriptor;

    protected Abstract(Builder<? extends Builder, ? extends Abstract> builder) {
        this.query = builder.query;
        this.dataSource = builder.dataSource;
    }

    public void setMessageColumnName(String messageColumnName) {
        this.messageColumnName = messageColumnName;
    }

    public void setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
        this.messageDescriptor = messageDescriptor;
    }

    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        return connection.prepareStatement(query);
    }

    public abstract static class Builder<B extends Builder<B, Q>, Q extends Abstract> {

        private DataSourceWrapper dataSource;
        private String query;

        public abstract Q build();

        protected abstract B getThis();

        public Builder<B, Q> setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return getThis();
        }

        public Builder<B, Q> setQuery(String query) {
            this.query = query;
            return getThis();
        }
    }

}
