/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for DirectDataCloudConnectionProperties SSL/TLS property parsing and serialization.
 */
class DirectDataCloudConnectionPropertiesTest {

    @Test
    void testDefaultValues() throws DataCloudJDBCException {
        Properties props = new Properties();
        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isDirectConnection()).isFalse();
        assertThat(properties.isSslDisabledFlag()).isFalse();
        assertThat(properties.getTruststorePathValue()).isEmpty();
        assertThat(properties.getTruststorePasswordValue()).isEmpty();
        assertThat(properties.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(properties.getClientCertPathValue()).isEmpty();
        assertThat(properties.getClientKeyPathValue()).isEmpty();
        assertThat(properties.getCaCertPathValue()).isEmpty();
    }

    @Test
    void testDirectProperty() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isDirectConnection()).isTrue();
    }

    @Test
    void testSslDisabledProperty() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl_disabled", "true");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isSslDisabledFlag()).isTrue();
    }

    @Test
    void testSslCertificateProperties() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("truststore_path", "/path/to/truststore.jks");
        props.setProperty("truststore_password", "password123");
        props.setProperty("truststore_type", "PKCS12");
        props.setProperty("client_cert_path", "/path/to/client.crt");
        props.setProperty("client_key_path", "/path/to/client.key");
        props.setProperty("ca_cert_path", "/path/to/ca.crt");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(properties.getTruststorePasswordValue()).isEqualTo("password123");
        assertThat(properties.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(properties.getClientCertPathValue()).isEqualTo("/path/to/client.crt");
        assertThat(properties.getClientKeyPathValue()).isEqualTo("/path/to/client.key");
        assertThat(properties.getCaCertPathValue()).isEqualTo("/path/to/ca.crt");
    }

    @Test
    void testBooleanParsing() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "false");
        props.setProperty("ssl_disabled", "false");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isDirectConnection()).isFalse();
        assertThat(properties.isSslDisabledFlag()).isFalse();
    }

    @Test
    void testCaseInsensitiveBooleanParsing() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "TRUE");
        props.setProperty("ssl_disabled", "True");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isDirectConnection()).isTrue();
        assertThat(properties.isSslDisabledFlag()).isTrue();
    }

    @Test
    void testToPropertiesSerialization() throws DataCloudJDBCException {
        Properties originalProps = new Properties();
        originalProps.setProperty("direct", "true");
        originalProps.setProperty("ssl_disabled", "true");
        originalProps.setProperty("truststore_path", "/path/to/truststore.jks");
        originalProps.setProperty("truststore_password", "password123");
        originalProps.setProperty("client_cert_path", "/path/to/client.crt");
        originalProps.setProperty("ca_cert_path", "/path/to/ca.crt");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(originalProps);
        Properties serializedProps = properties.toProperties();

        // Check that all SSL-related properties are preserved
        assertThat(serializedProps.getProperty("direct")).isEqualTo("true");
        assertThat(serializedProps.getProperty("ssl_disabled")).isEqualTo("true");
        assertThat(serializedProps.getProperty("truststore_path")).isEqualTo("/path/to/truststore.jks");
        assertThat(serializedProps.getProperty("truststore_password")).isEqualTo("password123");
        assertThat(serializedProps.getProperty("client_cert_path")).isEqualTo("/path/to/client.crt");
        assertThat(serializedProps.getProperty("ca_cert_path")).isEqualTo("/path/to/ca.crt");
    }

    @Test
    void testIsDirectMethod() throws DataCloudJDBCException {
        Properties props = new Properties();

        // Test when direct is not set
        assertThat(DirectDataCloudConnection.isDirect(props)).isFalse();

        // Test when direct is set to true
        props.setProperty("direct", "true");
        assertThat(DirectDataCloudConnection.isDirect(props)).isTrue();

        // Test when direct is set to false
        props.setProperty("direct", "false");
        assertThat(DirectDataCloudConnection.isDirect(props)).isFalse();
    }

    @Test
    void testMixedProperties() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");
        props.setProperty("ssl_disabled", "false");
        props.setProperty("truststore_path", "/path/to/truststore.jks");
        props.setProperty("client_cert_path", "/path/to/client.crt");
        props.setProperty("some_other_property", "some_value");

        DirectDataCloudConnectionProperties properties = DirectDataCloudConnectionProperties.of(props);

        assertThat(properties.isDirectConnection()).isTrue();
        assertThat(properties.isSslDisabledFlag()).isFalse();
        assertThat(properties.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(properties.getClientCertPathValue()).isEqualTo("/path/to/client.crt");

        // Verify that non-SSL properties are not included in serialized properties
        Properties serializedProps = properties.toProperties();
        assertThat(serializedProps.getProperty("some_other_property")).isNull();
    }
}
