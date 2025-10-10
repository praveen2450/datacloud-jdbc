/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for SslProperties class.
 */
class SslPropertiesTest {

    @Test
    void testDefaultValues() throws DataCloudJDBCException {
        SslProperties props = SslProperties.defaultProperties();

        assertThat(props.isSslDisabledFlag()).isFalse();
        assertThat(props.getTruststorePathValue()).isNull();
        assertThat(props.getTruststorePasswordValue()).isNull();
        assertThat(props.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(props.getClientCertPathValue()).isNull();
        assertThat(props.getClientKeyPathValue()).isNull();
        assertThat(props.getCaCertPathValue()).isNull();
    }

    @Test
    void testSslDisabledProperty() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.isSslDisabledFlag()).isTrue();
    }

    @Test
    void testSslCertificateProperties() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        props.setProperty("ssl.truststore.password", "password");
        props.setProperty("ssl.truststore.type", "PKCS12");
        props.setProperty("ssl.client.certPath", "/path/to/client.crt");
        props.setProperty("ssl.client.keyPath", "/path/to/client.key");
        props.setProperty("ssl.ca.certPath", "/path/to/ca.crt");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        assertThat(sslProps.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(sslProps.getTruststorePasswordValue()).isEqualTo("password");
        assertThat(sslProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(sslProps.getClientCertPathValue()).isEqualTo("/path/to/client.crt");
        assertThat(sslProps.getClientKeyPathValue()).isEqualTo("/path/to/client.key");
        assertThat(sslProps.getCaCertPathValue()).isEqualTo("/path/to/ca.crt");
    }

    @Test
    void testBooleanParsing() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "false");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.isSslDisabledFlag()).isFalse();
    }

    @Test
    void testCaseInsensitiveBooleanParsing() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "FALSE");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.isSslDisabledFlag()).isFalse();
    }

    @Test
    void testToPropertiesSerialization() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        props.setProperty("ssl.client.certPath", "/path/to/client.crt");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        Properties serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isEqualTo("true");
        assertThat(serialized.getProperty("ssl.truststore.path")).isEqualTo("/path/to/truststore.jks");
        assertThat(serialized.getProperty("ssl.client.certPath")).isEqualTo("/path/to/client.crt");
    }

    @Test
    void testDetermineSslMode() throws DataCloudJDBCException {
        // Test SSL disabled
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");
        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.determineSslMode()).isEqualTo(SslProperties.SslMode.DISABLED);

        // Test default TLS (system truststore)
        props.remove("ssl.disabled");
        sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.determineSslMode()).isEqualTo(SslProperties.SslMode.DEFAULT_TLS);

        // Test one-sided TLS with custom truststore
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.determineSslMode()).isEqualTo(SslProperties.SslMode.ONE_SIDED_TLS);

        // Test mutual TLS with client certificates
        props.setProperty("ssl.client.certPath", "/path/to/client.crt");
        props.setProperty("ssl.client.keyPath", "/path/to/client.key");
        sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.determineSslMode()).isEqualTo(SslProperties.SslMode.MUTUAL_TLS);
    }
}
