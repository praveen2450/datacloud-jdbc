/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for SslProperties class.
 */
class SslPropertiesTest {

    @Test
    void testDefaultValues() throws SQLException {
        SslProperties props = SslProperties.defaultProperties();

        assertThat(props.getTruststorePathValue()).isNull();
        assertThat(props.getTruststorePasswordValue()).isNull();
        assertThat(props.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(props.getClientCertPathValue()).isNull();
        assertThat(props.getClientKeyPathValue()).isNull();
        assertThat(props.getCaCertPathValue()).isNull();
    }

    @Test
    void testSslDisabledProperty() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify SSL disabled is parsed correctly
        assertThat(sslProps).isNotNull();
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testSslCertificateProperties() throws Exception {
        // Create a temporary file for truststore validation
        java.io.File tempTruststore = java.io.File.createTempFile("test-truststore", ".jks");
        tempTruststore.deleteOnExit();
        // Write some content so file is not empty
        java.nio.file.Files.write(tempTruststore.toPath(), "dummy content".getBytes());

        // Test with only truststore (no client certs or CA cert)
        Properties props = new Properties();
        props.setProperty(SslProperties.SSL_TRUSTSTORE_PATH, tempTruststore.getAbsolutePath());
        props.setProperty(SslProperties.SSL_TRUSTSTORE_PASSWORD, "password");
        props.setProperty(SslProperties.SSL_TRUSTSTORE_TYPE, "PKCS12");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify properties are parsed correctly
        assertThat(sslProps.getTruststorePathValue()).isEqualTo(tempTruststore.getAbsolutePath());
        assertThat(sslProps.getTruststorePasswordValue()).isEqualTo("password");
        assertThat(sslProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testBooleanParsing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "false");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify boolean is parsed correctly (ssl.disabled=false means SSL is enabled)
        assertThat(sslProps).isNotNull();
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testToPropertiesSerialization() throws Exception {
        // Test SSL disabled serialization
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        Properties serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isEqualTo("true");
        assertThat(serialized.getProperty(SslProperties.SSL_TRUSTSTORE_PATH)).isNull();

        // Test one-sided TLS serialization
        // Create a temporary file for truststore validation
        java.io.File tempTruststore = java.io.File.createTempFile("test-truststore", ".jks");
        tempTruststore.deleteOnExit();
        // Write some content so file is not empty
        java.nio.file.Files.write(tempTruststore.toPath(), "dummy content".getBytes());

        props = new Properties();
        props.setProperty(SslProperties.SSL_TRUSTSTORE_PATH, tempTruststore.getAbsolutePath());
        sslProps = SslProperties.ofDestructive(props);
        serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isNull();
        assertThat(serialized.getProperty(SslProperties.SSL_TRUSTSTORE_PATH))
                .isEqualTo(tempTruststore.getAbsolutePath());
    }

    @Test
    void testErrorMissingClientKeyWhenClientCertProvided() {
        // Error occurs at takeRequired() before file validation, so dummy path is sufficient
        Properties props = new Properties();
        props.setProperty(SslProperties.SSL_CLIENT_CERT_PATH, "/dummy/client/cert.pem");
        // Missing SSL_CLIENT_KEY_PATH

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> SslProperties.ofDestructive(props));

        assertThat(exception.getMessage()).contains("ssl.client.keyPath");
        assertThat(exception.getMessage()).contains("missing");
    }

    @Test
    void testErrorBothCaCertAndTruststoreSpecified() {
        // Error occurs at validation check before file extraction, so dummy paths are sufficient
        Properties props = new Properties();
        props.setProperty(SslProperties.SSL_CA_CERT_PATH, "/dummy/ca/cert.pem");
        props.setProperty(SslProperties.SSL_TRUSTSTORE_PATH, "/dummy/truststore.jks");

        SQLException exception = assertThrows(SQLException.class, () -> SslProperties.ofDestructive(props));

        assertThat(exception.getMessage()).contains("Cannot specify both ssl.ca.certPath and ssl.truststore.path");
    }
}
