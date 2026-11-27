/*
 * Copyright 2026 CodeMatters, Lda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package io.spine.server.storage.jdbc.delivery;

import com.google.protobuf.Message;
import io.spine.server.ContextSpec;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.StorageGroup;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import org.jspecify.annotations.Nullable;

/**
 * A {@link StorageFactory} that keeps its JDBC delegate open when an application closes.
 *
 * <p>A delivery test closes the black box, and with it the application and the factory
 * the application was given, before the suite's own tear-down reads the catch-up storage.
 * Closing the delegate at that moment makes the tear-down fail. The test closes the
 * delegate itself once the tear-down has finished.
 */
final class FactoryLeftOpen implements StorageFactory {

    private final JdbcStorageFactory delegate;

    FactoryLeftOpen(JdbcStorageFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context,
                        RecordSpec<I, R> recordSpec,
                        @Nullable StorageGroup group) {
        return delegate.createRecordStorage(context, recordSpec, group);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    /**
     * Does not close the delegate.
     *
     * <p>The test closes the delegate after the suite tear-down.
     */
    @Override
    public void close() {
        // See the class documentation.
    }
}
