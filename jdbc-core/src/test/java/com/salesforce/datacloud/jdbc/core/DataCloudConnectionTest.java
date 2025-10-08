/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@ExtendWith(MockitoExtension.class)
class DataCloudConnectionTest extends InterceptedHyperTestBase {

    @Test
    void testCreateStatement() {
        try (val connection = getInterceptedClientConnection()) {
            val statement = connection.createStatement();
            assertThat(statement).isInstanceOf(DataCloudStatement.class);
        }
    }

    @Test
    void testClose() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.isClosed()).isFalse();
            connection.close();
            assertThat(connection.isClosed()).isTrue();
        }
    }

    @Test
    void testGetMetadata() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.getMetaData()).isInstanceOf(DataCloudDatabaseMetadata.class);
        }
    }

    @Test
    void testGetTransactionIsolation() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
        }
    }

    @Test
    void testIsValidNegativeTimeoutThrows() {
        try (val connection = getInterceptedClientConnection()) {
            val ex = assertThrows(DataCloudJDBCException.class, () -> connection.isValid(-1));
            assertThat(ex).hasMessage("Invalid timeout value: -1").hasNoCause();
        }
    }

    @Test
    @SneakyThrows
    void testIsValid() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.isValid(200)).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void testConnectionUnwrap() {
        val connection = getInterceptedClientConnection();
        val unwrapped = connection.unwrap(DataCloudConnection.class);
        assertThat(connection.isWrapperFor(DataCloudConnection.class)).isTrue();
        assertThat(unwrapped).isInstanceOf(DataCloudConnection.class);
        assertThrows(DataCloudJDBCException.class, () -> connection.unwrap(String.class));
        connection.close();
    }

    /**
     * Minimal stub provider for testing purposes.
     */
    private static class TestStubProvider implements HyperGrpcStubProvider {
        @Getter
        public final HyperServiceGrpc.HyperServiceBlockingStub stub =
                mock(HyperServiceGrpc.HyperServiceBlockingStub.class);

        @Override
        public void close() throws Exception {
            // No-op
        }
    }

    @SneakyThrows
    @Test
    void testDriverInterceptorsAreAddedWhenStubProviderIsUsed() {
        val stubProvider = new TestStubProvider();
        val connection = DataCloudConnection.of(stubProvider, ConnectionProperties.defaultProperties(), "", null);
        connection.getStub();
        // Interceptors should have been added to set the default workload header (x-hyperdb-workload)
        verify(stubProvider.stub).withInterceptors(any(ClientInterceptor[].class));
        connection.close();
    }

    @Test
    void testDeriveHeadersFromPropertiesWithNoAdditionalHeaders() {
        val connectionProperties = ConnectionProperties.defaultProperties();
        val metadata = DataCloudConnection.deriveHeadersFromProperties("test-dataspace", connectionProperties);

        // Should contain standard headers
        assertThat(metadata.get(Metadata.Key.of("User-Agent", Metadata.ASCII_STRING_MARSHALLER)))
                .isNotNull();
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("jdbcv3");
        assertThat(metadata.get(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("test-dataspace");

        // Should not contain any additional headers
        assertThat(connectionProperties.getAdditionalHeaders().isEmpty()).isTrue();
    }

    @Test
    void testDeriveHeadersFromPropertiesWithAdditionalHeaders() {
        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("ctx-tenant-id", "a360/falcondev/22d8c30636264f4b8b55a79a898fc968");
        additionalHeaders.put("custom-header", "custom-value");
        additionalHeaders.put("x-custom-header", "x-custom-value");

        val connectionProperties = ConnectionProperties.builder()
                .additionalHeaders(additionalHeaders)
                .build();

        val metadata = DataCloudConnection.deriveHeadersFromProperties("test-dataspace", connectionProperties);

        // Should contain standard headers
        assertThat(metadata.get(Metadata.Key.of("User-Agent", Metadata.ASCII_STRING_MARSHALLER)))
                .isNotNull();
        assertThat(metadata.get(Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("jdbcv3");
        assertThat(metadata.get(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("test-dataspace");

        // Should contain additional headers
        assertThat(metadata.get(Metadata.Key.of("ctx-tenant-id", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("a360/falcondev/22d8c30636264f4b8b55a79a898fc968");
        assertThat(metadata.get(Metadata.Key.of("custom-header", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("custom-value");
        assertThat(metadata.get(Metadata.Key.of("x-custom-header", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("x-custom-value");
    }
}
