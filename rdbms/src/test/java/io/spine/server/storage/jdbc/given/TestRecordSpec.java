/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.given;

import com.google.common.collect.ImmutableSet;
import io.spine.server.entity.storage.EntityRecordSpec;
import io.spine.server.projection.Projection;
import io.spine.server.storage.MessageRecordSpec;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;

/**
 * Provides {@link MessageRecordSpec}s for tests.
 */
public class TestRecordSpec {

    /**
     * Prevents this utility class from instantiation.
     */
    private TestRecordSpec() {
    }

    /**
     * Creates a new {@code MessageRecordSpec} describing how instances of {@link StgProject}
     * message are stored.
     *
     * <p>Defines no record columns.
     */
    public static MessageRecordSpec<StgProjectId, StgProject> stgProjectSpec() {
        return new MessageRecordSpec<>(
                StgProjectId.class, StgProject.class, StgProject::getId, ImmutableSet.of()
        );
    }

    /**
     * Creates a new {@code EntityRecordSpec} describing how instances of {@link ProjectDetails}
     * projection are stored.
     */
    public static EntityRecordSpec<StgProjectId, StgProject, ProjectDetails> projectDetailsSpec() {
        return EntityRecordSpec.of(ProjectDetails.class);
    }

    private static class ProjectDetails
            extends Projection<StgProjectId, StgProject, StgProject.Builder> {

    }
}