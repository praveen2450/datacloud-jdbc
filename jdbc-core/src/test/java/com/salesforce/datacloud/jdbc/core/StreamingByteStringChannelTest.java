/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@Slf4j
class StreamingByteStringChannelTest {
    @Test
    @SneakyThrows
    void isOpenFollowsNioSemantics() {
        try (val channel = new StreamingByteStringChannel(empty())) {
            assertThat(channel.isOpen()).isTrue(); // Channel starts open regardless of data availability
            // Even with no data, channel remains open until explicitly closed
            assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(-1); // End-of-stream
            assertThat(channel.isOpen()).isTrue(); // Still open after end-of-stream

            channel.close(); // Explicitly close it
            assertThat(channel.isOpen()).isFalse(); // Now it should be closed
        }
    }

    @Test
    @SneakyThrows
    void isOpenDetectsIfIteratorHasRemaining() {
        try (val channel = new StreamingByteStringChannel(some())) {
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void readThrowsClosedChannelExceptionWhenClosed() {
        val channel = new StreamingByteStringChannel(some());
        channel.close();
        assertThat(channel.isOpen()).isFalse();

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1)))
                .isInstanceOf(java.nio.channels.ClosedChannelException.class);
    }

    @Test
    @SneakyThrows
    void readReturnsNegativeOneOnIteratorExhaustion() {
        try (val channel = new StreamingByteStringChannel(empty())) {
            assertThat(channel.read(ByteBuffer.allocateDirect(2))).isEqualTo(-1);
        }
    }

    @SneakyThrows
    @Test
    void readIsLazy() {
        val first = ByteBuffer.allocate(5);
        val second = ByteBuffer.allocate(5);
        val seen = new ArrayList<QueryResult>();

        val iterator = infiniteStream().peek(seen::add).iterator();

        try (val channel = new ReadChannel(new StreamingByteStringChannel(iterator))) {
            channel.readFully(first);
            assertThat(seen).hasSize(5);
            channel.readFully(second);
            assertThat(seen).hasSize(10);

            assertThat(new String(first.array(), StandardCharsets.UTF_8)).isEqualTo("01234");
            assertThat(new String(second.array(), StandardCharsets.UTF_8)).isEqualTo("56789");
        }
    }

    private static Iterator<QueryResult> some() {
        return infiniteStream().iterator();
    }

    private static Iterator<QueryResult> empty() {
        return infiniteStream().limit(0).iterator();
    }

    private static Stream<QueryResult> infiniteStream() {
        return Stream.iterate(0, i -> i + 1)
                .map(i -> Integer.toString(i))
                .map(StreamingByteStringChannelTest::toMessage);
    }

    private static QueryResult toMessage(String string) {
        val byteString = ByteString.copyFromUtf8(string);
        val binaryPart = QueryResultPartBinary.newBuilder().setData(byteString);
        return QueryResult.newBuilder().setBinaryPart(binaryPart).build();
    }
}
