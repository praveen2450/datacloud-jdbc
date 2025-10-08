/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.DirectDataCloudConnectionProperties;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for DirectDataCloudConnection SSL/TLS functionality and connection creation.
 */
class DirectDataCloudConnectionTest {

    @TempDir
    Path tempDir;

    @Test
    void testDetermineSslMode() throws DataCloudJDBCException {
        // Test SSL disabled
        Properties props = new Properties();
        props.setProperty("direct", "true");
        props.setProperty("ssl_disabled", "true");
        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.determineSslMode(directProps))
                .isEqualTo(DirectDataCloudConnection.SslMode.DISABLED);

        // Test default TLS (system truststore)
        props.remove("ssl_disabled");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.determineSslMode(directProps))
                .isEqualTo(DirectDataCloudConnection.SslMode.DEFAULT_TLS);

        // Test one-sided TLS with custom truststore
        props.setProperty("truststore_path", "/path/to/truststore.jks");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.determineSslMode(directProps))
                .isEqualTo(DirectDataCloudConnection.SslMode.ONE_SIDED_TLS);

        // Test mutual TLS with client certificates
        props.setProperty("client_cert_path", "/path/to/client.crt");
        props.setProperty("client_key_path", "/path/to/client.key");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.determineSslMode(directProps))
                .isEqualTo(DirectDataCloudConnection.SslMode.MUTUAL_TLS);
    }

    @Test
    void testHasTrustConfiguration() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);

        // No trust configuration
        assertThat(DirectDataCloudConnection.hasTrustConfiguration(directProps)).isFalse();

        // With truststore
        props.setProperty("truststore_path", "/path/to/truststore.jks");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.hasTrustConfiguration(directProps)).isTrue();

        // With CA cert
        props.remove("truststore_path");
        props.setProperty("ca_cert_path", "/path/to/ca.crt");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.hasTrustConfiguration(directProps)).isTrue();
    }

    @Test
    void testHasClientCertificates() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);

        // No client certificates
        assertThat(DirectDataCloudConnection.hasClientCertificates(directProps)).isFalse();

        // With both client cert and key
        props.setProperty("client_cert_path", "/path/to/client.crt");
        props.setProperty("client_key_path", "/path/to/client.key");
        directProps = DirectDataCloudConnectionProperties.of(props);
        assertThat(DirectDataCloudConnection.hasClientCertificates(directProps)).isTrue();
    }

    @Test
    void testHasClientCertificatesWithIncompleteConfig() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");
        props.setProperty("client_cert_path", "/path/to/client.crt");
        // Missing client_key_path

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);

        assertThatThrownBy(() -> DirectDataCloudConnection.hasClientCertificates(directProps))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Client certificate path provided without client key path");
    }

    @Test
    void testCreateDirectConnectionWithAdditionalHeaders() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("direct", "true");
        props.setProperty("headers.ctx-tenant-id", "a360/falcondev/22d8c30636264f4b8b55a79a898fc968");
        props.setProperty("headers.custom-header", "custom-value");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);

        // Verify that the properties are correctly parsed
        assertThat(directProps.isDirectConnection()).isTrue();

        // The additional headers should be preserved in the original properties
        assertThat(props.getProperty("headers.ctx-tenant-id"))
                .isEqualTo("a360/falcondev/22d8c30636264f4b8b55a79a898fc968");
        assertThat(props.getProperty("headers.custom-header")).isEqualTo("custom-value");
    }

    @Test
    void testInvalidInputs() {
        Properties props = new Properties();
        props.setProperty("direct", "true");

        assertThatThrownBy(() -> DirectDataCloudConnection.of(null, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage("Connection URL cannot be null");

        assertThatThrownBy(() -> DirectDataCloudConnection.of("jdbc:salesforce-hyper://localhost:7484", null))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage("Connection properties cannot be null");
    }

    @Test
    void testDirectConnectionWithoutDirectFlag() throws DataCloudJDBCException {
        Properties props = new Properties();
        // Missing direct=true

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(props);

        assertThatThrownBy(() ->
                        DirectDataCloudConnection.of("jdbc:salesforce-hyper://localhost:7484", directProps, props))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Cannot establish direct connection without direct enabled");
    }

    @Test
    void testFileValidationWithNonExistentFile() throws IOException {
        Path nonExistentFile = tempDir.resolve("non-existent.pem");

        assertThatThrownBy(() -> DirectDataCloudConnection.of(
                        "jdbc:salesforce-hyper://localhost:7484", createPropsWithCertPath(nonExistentFile.toString())))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("File not found");
    }

    private Properties createPropsWithCertPath(String certPath) {
        Properties props = new Properties();
        props.setProperty("direct", "true");
        props.setProperty("client_cert_path", certPath);
        props.setProperty("client_key_path", certPath); // Use same path for simplicity
        return props;
    }
}
