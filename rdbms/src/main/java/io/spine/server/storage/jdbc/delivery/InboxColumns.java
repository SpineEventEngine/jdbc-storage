/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.collect.ImmutableList;
import io.spine.server.delivery.InboxColumn;
import io.spine.server.delivery.InboxId;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.storage.jdbc.record.column.ColumnSpec;
import io.spine.server.storage.jdbc.record.column.CustomColumns;

import static io.spine.server.storage.jdbc.record.column.ColumnSpec.columnSpec;

/**
 * The columns of {@link InboxMessage} customized for storing in RDBMS.
 */
public final class InboxColumns extends CustomColumns<InboxMessage> {

    private static final ImmutableList<ColumnSpec<InboxMessage, ?>> columns =
            ImmutableList.of(
                    columnSpec(InboxColumn.inbox_id, InboxId::hashCode)
            );

    private static final InboxColumns instance = new InboxColumns(columns);

    private InboxColumns(ImmutableList<ColumnSpec<InboxMessage, ?>> columns) {
        super(columns);
    }

    public static InboxColumns instance() {
        return instance;
    }
}
