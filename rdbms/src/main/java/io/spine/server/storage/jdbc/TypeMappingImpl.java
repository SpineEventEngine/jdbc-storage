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

package io.spine.server.storage.jdbc;

import io.spine.type.TypeName;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * A basic implementation of {@link TypeMapping}.
 *
 * @author Dmytro Grankin
 */
final class TypeMappingImpl implements TypeMapping {

    private final Map<Type, TypeName> mappedTypes;

    TypeMappingImpl(Map<Type, TypeName> mappedTypes) {
        this.mappedTypes = mappedTypes;
    }

    @Override
    public TypeName typeNameFor(Type type) {
        checkState(mappedTypes.containsKey(type),
                   "The type mapping doesn't define name for %s type.", type);
        final TypeName typeName = mappedTypes.get(type);
        return typeName;
    }
}
