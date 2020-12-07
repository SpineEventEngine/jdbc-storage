/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.truth.StringSubject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("`MultipleExceptionsOnClose` should")
class MultipleExceptionsOnCloseTest {

    @Test
    @DisplayName("describe all given exceptions in `toString`")
    void provideToString() {
        String message1 = "message1";
        String message2 = "message2";
        String message3 = "message3";
        IllegalStateException ex1 = new IllegalStateException(message1);
        IllegalStateException ex2 = new IllegalStateException(message2);
        IllegalStateException ex3 = new IllegalStateException(message3);

        MultipleExceptionsOnClose aggregatingException =
                new MultipleExceptionsOnClose(newArrayList(ex1, ex2, ex3));

        String exceptionsDescription = aggregatingException.toString();
        StringSubject exceptionSubject = assertThat(exceptionsDescription);
        exceptionSubject.contains(ex1.toString());
        exceptionSubject.contains(ex2.toString());
        exceptionSubject.contains(ex3.toString());
    }

    @Test
    @DisplayName("be direct subclass of Throwable")
    void beSubclassOfThrowable() {
        Class<MultipleExceptionsOnClose> clazz = MultipleExceptionsOnClose.class;
        Class<? super MultipleExceptionsOnClose> superclass = clazz.getSuperclass();
        assertEquals(Throwable.class, superclass);
    }
}
