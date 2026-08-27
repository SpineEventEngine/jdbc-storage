/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.record

import io.kotest.matchers.shouldBe
import io.spine.core.Event
import io.spine.server.storage.StorageGroup
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Supplements the Java-based [TableNamesTest] with the cases of composing
 * table names from free-form group values, such as Bounded Context names.
 */
@DisplayName("`TableNames` should")
internal class TableNamesSpec {

    @Test
    fun `keep the name derived from a dotted Proto type as it is today`() {
        TableNames.of(Event::class.java) shouldBe "spine_core_Event"
    }

    @Test
    fun `compose a grouped name from the group name and the record type`() {
        TableNames.of(Event::class.java, StorageGroup("Billing")) shouldBe "Billing_Event"
    }

    @Test
    fun `replace the characters prohibited in table names`() {
        TableNames.of(Event::class.java, StorageGroup("Billing Dept")) shouldBe
                "Billing_Dept_Event"
        TableNames.of(Event::class.java, StorageGroup("Context-1")) shouldBe
                "Context_1_Event"
    }
}
