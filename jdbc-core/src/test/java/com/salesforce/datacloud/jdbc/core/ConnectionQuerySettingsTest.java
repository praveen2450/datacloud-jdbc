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

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.util.Properties;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class ConnectionQuerySettingsTest {
    @Test
    @SneakyThrows
    public void testLegacyQuerySetting() {
        val settings = Maps.immutableEntry("serverSetting.date_style", "YMD");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SHOW date_style");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("ISO, YMD");
                },
                settings);
    }

    @Test
    @SneakyThrows
    public void testQuerySetting() {
        val settings = Maps.immutableEntry("querySetting.date_style", "YMD");

        assertWithStatement(
                statement -> {
                    val result = statement.executeQuery("SHOW date_style");
                    result.next();
                    assertThat(result.getString(1)).isEqualTo("ISO, YMD");
                },
                settings);
    }

    /**
     * Test getSessionTimeZone method with various timezone configurations
     */
    @Test
    @SneakyThrows
    public void testGetSessionTimeZone() {
        // Test with null properties - should return JVM default
        ConnectionQuerySettings settingsWithNull = ConnectionQuerySettings.of(null);
        assertThat(settingsWithNull.getSessionTimeZone()).isEqualTo(TimeZone.getDefault());

        // Test with empty properties - should return JVM default
        Properties emptyProperties = new Properties();
        ConnectionQuerySettings settingsWithEmpty = ConnectionQuerySettings.of(emptyProperties);
        assertThat(settingsWithEmpty.getSessionTimeZone()).isEqualTo(TimeZone.getDefault());

        // Test with querySetting.timezone property
        Properties propertiesWithTimezone = new Properties();
        propertiesWithTimezone.setProperty("querySetting.timezone", "America/Los_Angeles");
        ConnectionQuerySettings settingsWithTimezone = ConnectionQuerySettings.of(propertiesWithTimezone);
        assertThat(settingsWithTimezone.getSessionTimeZone()).isEqualTo(TimeZone.getTimeZone("America/Los_Angeles"));

        // Test with UTC timezone
        Properties utcProperties = new Properties();
        utcProperties.setProperty("querySetting.timezone", "UTC");
        ConnectionQuerySettings utcSettings = ConnectionQuerySettings.of(utcProperties);
        assertThat(utcSettings.getSessionTimeZone()).isEqualTo(TimeZone.getTimeZone("UTC"));

        // Test with invalid timezone - should fall back to GMT
        Properties invalidProperties = new Properties();
        invalidProperties.setProperty("querySetting.timezone", "Invalid/Timezone");
        ConnectionQuerySettings invalidSettings = ConnectionQuerySettings.of(invalidProperties);
        assertThat(invalidSettings.getSessionTimeZone()).isEqualTo(TimeZone.getTimeZone("GMT"));
    }
}
