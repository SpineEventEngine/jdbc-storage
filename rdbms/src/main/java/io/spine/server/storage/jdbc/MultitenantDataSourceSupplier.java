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

import com.google.common.collect.ImmutableMap;
import io.spine.core.TenantId;
import io.spine.server.tenant.TenantFunction;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

final class MultitenantDataSourceSupplier implements DataSourceSupplier {

    private final Map<TenantId, DataSourceWrapper> dataSourcePerTenant;

    // todo for now assume the data source map is given to us
    MultitenantDataSourceSupplier(Map<TenantId, DataSourceWrapper> dataSourcePerTenant) {
        this.dataSourcePerTenant = ImmutableMap.copyOf(dataSourcePerTenant);
    }

    @Override
    public DataSourceWrapper get() {
        TenantIdRetriever retriever = new TenantIdRetriever();
        TenantId tenantId = retriever.execute();
        DataSourceWrapper dataSource = dataSourcePerTenant.get(tenantId);
        checkNotNull(dataSource,
                     "The data source for the given tenant ID %s is unknown", tenantId);
        return dataSource;
    }

    @Override
    public void closeAll() {
        dataSourcePerTenant.values().forEach(DataSourceWrapper::close);
    }

    /**
     * A function declosuring the current tenant {@linkplain TenantId ID}.
     */
    private static class TenantIdRetriever extends TenantFunction<TenantId> {

        /**
         * Creates a new instance of {@code TenantIdRetriever}.
         *
         * @throws IllegalStateException
         *         if the application has a single tenant
         */
        private TenantIdRetriever() throws IllegalStateException {
            super(true);
        }

        /**
         * Retrieves the passed {@link TenantId}, ensuring it's not equal to {@code null}.
         *
         * @param input
         *         current {@link TenantId}
         * @return the input
         */
        @Override
        public TenantId apply(@Nullable TenantId input) {
            checkNotNull(input);
            return input;
        }
    }
}
