/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.engine;

import io.spine.server.storage.jdbc.DataSourceMetaData;
import io.spine.server.storage.jdbc.operation.Operation;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The DB engines known to this library,
 * which make difference in terms of providing specific {@link Operation} implementations.
 *
 * <p>This list isn't expected to be complete. However, it may get bigger as far as
 * the Spine routines are getting some known optimizations on other DB engines.
 */
public enum PredefinedEngine implements DetectedEngine {

    /**
     * MySQL of all versions.
     */
    MySQL,

    /**
     * Postgres of all versions.
     */
    Postgres,

    /**
     * None of the above.
     *
     * <p>Some default implementation of {@link Operation}s will be supplied.
     */
    Generic;

    /**
     * Detects the engine from the passed data source metadata.
     *
     * <p>Returns {@link #Generic Generic} if no other was detected.
     *
     * @implNote This implementation is inspired by {@link com.querydsl.sql.SQLTemplatesRegistry}.
     */
    public static PredefinedEngine from(DataSourceMetaData metaData) {
        checkNotNull(metaData);
        var productName = metaData.productName()
                                  .toLowerCase(Locale.getDefault());
        if (productName.contains("mysql")) {
            return MySQL;
        }
        if (productName.contains("postgresql")) {
            return Postgres;
        }
        return Generic;
    }

    /**
     * Returns a generic name for each of predefined engines.
     *
     * <p>Version is not included.
     */
    @Override
    public String id() {
        return this.name();
    }
}
