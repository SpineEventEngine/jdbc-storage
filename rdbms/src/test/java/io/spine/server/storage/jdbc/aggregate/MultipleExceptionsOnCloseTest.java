/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Lists.newArrayList;
import static io.spine.test.Verify.assertContains;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("MultipleExceptionsOnClose should")
class MultipleExceptionsOnCloseTest {

    @Test
    @DisplayName("describe all given exceptions in `toString`")
    void provideToString() {
        final String message1 = "message1";
        final String message2 = "message2";
        final String message3 = "message3";
        final IllegalStateException ex1 = new IllegalStateException(message1);
        final IllegalStateException ex2 = new IllegalStateException(message2);
        final IllegalStateException ex3 = new IllegalStateException(message3);

        final MultipleExceptionsOnClose aggregatingException =
                new MultipleExceptionsOnClose(newArrayList(ex1, ex2, ex3));

        final String stringName = "Exception description";
        assertContains(stringName,
                       ex1.toString(),
                       aggregatingException.toString());
        assertContains(stringName,
                       ex2.toString(),
                       aggregatingException.toString());
        assertContains(stringName,
                       ex3.toString(),
                       aggregatingException.toString());
    }

    @Test
    @DisplayName("be direct subclass of Throwable")
    void beSubclassOfThrowable() {
        final Class<MultipleExceptionsOnClose> clazz = MultipleExceptionsOnClose.class;
        final Class<? super MultipleExceptionsOnClose> superclass = clazz.getSuperclass();
        assertEquals(Throwable.class, superclass);
    }
}
