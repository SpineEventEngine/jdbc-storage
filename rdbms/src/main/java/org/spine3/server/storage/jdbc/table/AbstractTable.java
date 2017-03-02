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

package org.spine3.server.storage.jdbc.table;

import com.google.common.collect.ImmutableList;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * @author Dmytro Dashenkov.
 */
public abstract class AbstractTable<I, C extends Enum & TableColumn> {

    private final String name;

    private final IdColumn<I> idColumn;

    private final DataSourceWrapper dataSource;

    private ImmutableList<C> columns;

    public AbstractTable(String name, IdColumn<I> idColumn,
                         DataSourceWrapper dataSource) {
        this.name = name;
        this.idColumn = idColumn;
        this.dataSource = dataSource;
    }

    public abstract C getIdColumnDeclaration();

    protected abstract Class<C> getTableColumnType();

    @SuppressWarnings("ReturnOfCollectionOrArrayField") // Returns immutable collection
    public ImmutableList<C> getColumns() {
        if (columns == null) {
            final Class<C> tableColumnsType = getTableColumnType();
            final C[] columnsArray = tableColumnsType.getEnumConstants();
            columns = ImmutableList.copyOf(columnsArray);
        }
        return columns;
    }

    public String getName() {
        return name;
    }

    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    public DataSourceWrapper getDataSource() {
        return dataSource;
    }
}
