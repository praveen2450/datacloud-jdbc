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
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class TimeStampVectorAccessorTest {

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @InjectSoftAssertions
    private SoftAssertions collector;

    public static final int BASE_YEAR = 2020;
    public static final int NUM_OF_METHODS = 4;

    @Test
    @SneakyThrows
    void testTimestampNanoVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.getAndIncrement()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampNanoTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampMicroVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampMicroTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMicroTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampSecVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = (values.get(i.get()) / 1000) * 1000;

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampSecTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampSecTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = (values.get(i.get()) / 1000) * 1000;

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);

                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testNulledTimestampVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createTimeStampSecTZVector(values, "UTC"))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();

                collector.assertThat(timestampValue).isNull();
                collector.assertThat(dateValue).isNull();
                collector.assertThat(timeValue).isNull();
                collector.assertThat(stringValue).isNull();
            }
        }
        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(NUM_OF_METHODS * values.size());
    }

    @SneakyThrows
    @Test
    void testGetTimestampWithDifferentTimeZone() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            Calendar pstCalendar = Calendar.getInstance(TimeZone.getTimeZone("PST"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(pstCalendar);
                val stringValue = sut.getString();
                val currentMillis = values.get(i.get());

                pstCalendar.setTimeInMillis(currentMillis);
                long pstMillis = pstCalendar.getTimeInMillis();

                LocalDateTime expectedPST = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(pstMillis),
                        TimeZone.getTimeZone("PST").toZoneId());
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedPST);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
    }

    private List<Long> getMilliSecondValues(Calendar calendar, List<Integer> monthNumber) {
        List<Long> result = new ArrayList<>();
        for (int currentNumber : monthNumber) {
            calendar.set(
                    BASE_YEAR + currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber);
            result.add(calendar.getTimeInMillis());
        }
        return result;
    }

    private List<Integer> getRandomMonthNumber() {
        Random rand = new Random();
        int valA = rand.nextInt(10) + 1;
        int valB = rand.nextInt(10) + 1;
        int valC = rand.nextInt(10) + 1;
        return ImmutableList.of(valA, valB, valC);
    }

    private void assertISOStringLike(String value, Long millis) {
        collector.assertThat(value).startsWith(getISOString(millis)).matches(".+Z$");
    }

    private void assertNaiveISOStringLike(String value, Long millis) {
        collector.assertThat(value).startsWith(getISOString(millis)).doesNotMatch(".+Z$");
    }

    private String getISOString(Long millis) {
        val formatter = new DateTimeFormatterBuilder().appendInstant(-1).toFormatter();

        return formatter.format(Instant.ofEpochMilli(millis)).replaceFirst("Z$", "");
    }

    /**
     * Test Avatica detection logic - calendar matching session timezone should ignore calendar for naive timestamps
     */
    @Test
    @SneakyThrows
    void testAvaticaDetectionLogic() {
        // Setup session timezone as America/Los_Angeles
        Properties properties = new Properties();
        properties.setProperty("querySetting.timezone", "America/Los_Angeles");

        // Create calendar with SAME timezone as session (should be ignored for naive timestamps)
        Calendar sessionCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));

        // Test value: 2024-01-01 17:30:00 UTC (1704130200000 ms)
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, properties);

            // For naive timestamps, calendar should be ignored when it matches session timezone
            val timestampValue = sut.getTimestamp(sessionCalendar);
            val stringValue = sut.getString();

            // Should return literal UTC value (ignoring calendar)
            // 2024-01-01 17:30:00 UTC should remain as 2024-01-01 17:30:00 (naive)
            LocalDateTime expectedLiteral = LocalDateTime.of(2024, 1, 1, 17, 30, 0);
            collector.assertThat(timestampValue).isEqualTo(java.sql.Timestamp.valueOf(expectedLiteral));

            // String should be naive (no timezone suffix)
            assertNaiveISOStringLike(stringValue, 1704130200000L);
        }
    }

    /**
     * Test user-provided calendar with different timezone - should respect calendar for naive timestamps
     */
    @Test
    @SneakyThrows
    void testUserProvidedCalendarDifferentTimezone() {
        // Setup session timezone as America/Los_Angeles
        Properties properties = new Properties();
        properties.setProperty("querySetting.timezone", "America/Los_Angeles");

        // Create calendar with DIFFERENT timezone from session (should be respected)
        Calendar userCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        // Test value: 2024-01-01 17:30:00 UTC (1704130200000 ms)
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, properties);

            // For naive timestamps, calendar should be respected when it differs from session timezone
            val timestampValue = sut.getTimestamp(userCalendar);
            val stringValue = sut.getString();

            // Should convert from UTC to user calendar timezone (which is UTC in this case)
            LocalDateTime expectedConverted = LocalDateTime.of(2024, 1, 1, 17, 30, 0);
            collector.assertThat(timestampValue).isEqualTo(java.sql.Timestamp.valueOf(expectedConverted));

            // String should be naive (no timezone suffix)
            assertNaiveISOStringLike(stringValue, 1704130200000L);
        }
    }

    /**
     * Test naive timestamp handling with no calendar - should return literal UTC values
     */
    @Test
    @SneakyThrows
    void testNaiveTimestampNoCalendar() {
        // Setup session timezone as America/Los_Angeles
        Properties properties = new Properties();
        properties.setProperty("querySetting.timezone", "America/Los_Angeles");

        // Test value: 2024-01-01 17:30:00 UTC (1704130200000 ms)
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, properties);

            // For naive timestamps with no calendar, should return literal values
            val timestampValue = sut.getTimestamp(null);
            val stringValue = sut.getString();

            // Should return literal UTC value
            LocalDateTime expectedLiteral = LocalDateTime.of(2024, 1, 1, 17, 30, 0);
            collector.assertThat(timestampValue).isEqualTo(java.sql.Timestamp.valueOf(expectedLiteral));

            // String should be naive (no timezone suffix)
            assertNaiveISOStringLike(stringValue, 1704130200000L);
        }
    }

    /**
     * Test timezone-aware timestamp conversion with calendar - should convert from vector timezone to calendar timezone
     */
    @Test
    @SneakyThrows
    void testTimezoneAwareTimestampWithCalendar() {
        // Setup session timezone as America/Los_Angeles
        Properties properties = new Properties();
        properties.setProperty("querySetting.timezone", "America/Los_Angeles");

        // Create calendar with different timezone
        Calendar userCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        // Test value: 2024-01-01 17:30:00 UTC (1704130200000 ms)
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoTZVector(values, "America/Los_Angeles")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, properties);

            // For timezone-aware timestamps, should convert from vector timezone to calendar timezone
            val timestampValue = sut.getTimestamp(userCalendar);
            val stringValue = sut.getString();

            // Should convert from Los_Angeles timezone to UTC
            // 2024-01-01 17:30:00 UTC stored as Los_Angeles time should convert to UTC
            LocalDateTime expectedConverted = LocalDateTime.of(2024, 1, 1, 17, 30, 0);
            collector.assertThat(timestampValue).isEqualTo(java.sql.Timestamp.valueOf(expectedConverted));

            // String should show timezone-aware format (with Z suffix)
            // The actual output is 09:30 because 17:30 UTC -> 09:30 LA time
            // Use the converted millis value that represents 2024-01-01T09:30:00Z
            assertISOStringLike(stringValue, 1704101400000L);
        }
    }

    /**
     * Test session timezone extraction from Properties - should handle null properties and querySetting.timezone
     */
    @Test
    @SneakyThrows
    void testSessionTimezoneExtraction() {
        // Test with null properties - should use JVM default
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, null);

            // Should work without throwing exception
            val timestampValue = sut.getTimestamp(null);
            collector.assertThat(timestampValue).isNotNull();
        }

        // Test with properties containing timezone
        Properties properties = new Properties();
        properties.setProperty("querySetting.timezone", "UTC");

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, properties);

            // Should work with specified timezone
            val timestampValue = sut.getTimestamp(null);
            collector.assertThat(timestampValue).isNotNull();
        }
    }

    /**
     * Test hasTimezoneInfo detection - should correctly identify naive vs timezone-aware timestamps
     */
    @Test
    @SneakyThrows
    void testHasTimezoneInfoDetection() {
        List<Long> values = ImmutableList.of(1704130200000L);
        val consumer = new TestWasNullConsumer(collector);

        // Test naive timestamp vector (no timezone)
        try (val naiveVector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val naiveSut = new TimeStampVectorAccessor(naiveVector, i::get, consumer);

            val stringValue = naiveSut.getString();
            // Naive timestamps should not have timezone suffix
            assertNaiveISOStringLike(stringValue, 1704130200000L);
        }

        // Test timezone-aware timestamp vector
        try (val tzVector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val tzSut = new TimeStampVectorAccessor(tzVector, i::get, consumer);

            val stringValue = tzSut.getString();
            // Timezone-aware timestamps should have timezone suffix
            assertISOStringLike(stringValue, 1704130200000L);
        }
    }
}
