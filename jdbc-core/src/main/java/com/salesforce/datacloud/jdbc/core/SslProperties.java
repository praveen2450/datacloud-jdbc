/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Properties;
import javax.net.ssl.TrustManagerFactory;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * SSL/TLS properties for DataCloud JDBC connections.
 * Supports automatic SSL mode detection based on provided certificate properties.
 */
@Slf4j
@Getter
@Builder
public class SslProperties {
    // Property to disable SSL (for testing only, we might change the implementation in future)
    public static final String SSL_DISABLED = "ssl.disabled";

    // JKS truststore properties - for trust verification
    public static final String SSL_TRUSTSTORE_PATH = "ssl.truststore.path";
    public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";

    // PEM certificate properties - for trust verification and client authentication
    public static final String SSL_CLIENT_CERT_PATH = "ssl.client.certPath";
    public static final String SSL_CLIENT_KEY_PATH = "ssl.client.keyPath";
    public static final String SSL_CA_CERT_PATH = "ssl.ca.certPath";

    // Internal constants
    private static final String DEFAULT_TRUSTSTORE_TYPE = "JKS";
    private static final String CA_CERT_ENTRY_NAME = "ca-cert";

    // Instance fields for parsed property values
    @Builder.Default
    private final boolean sslDisabledFlag = false;

    @Builder.Default
    private final String truststorePathValue = null;

    @Builder.Default
    private final String truststorePasswordValue = null;

    @Builder.Default
    private final String truststoreTypeValue = DEFAULT_TRUSTSTORE_TYPE;

    @Builder.Default
    private final String clientCertPathValue = null;

    @Builder.Default
    private final String clientKeyPathValue = null;

    @Builder.Default
    private final String caCertPathValue = null;

    /**
     * Parses SSL properties from a Properties object using destructive parsing.
     * This method removes the SSL properties from the input Properties object.
     *
     * @param props The properties to parse (will be modified)
     * @return A SslProperties instance
     * @throws DataCloudJDBCException if parsing of property values fails
     */
    public static SslProperties ofDestructive(Properties props) throws DataCloudJDBCException {
        if (props == null) {
            return SslProperties.builder().build();
        }

        SslPropertiesBuilder builder = SslProperties.builder();

        // Parse SSL disabled flag
        takeOptional(props, SSL_DISABLED).ifPresent(value -> builder.sslDisabledFlag(Boolean.parseBoolean(value)));

        // Parse truststore properties
        takeOptional(props, SSL_TRUSTSTORE_PATH).ifPresent(builder::truststorePathValue);
        takeOptional(props, SSL_TRUSTSTORE_PASSWORD).ifPresent(builder::truststorePasswordValue);
        takeOptional(props, SSL_TRUSTSTORE_TYPE).ifPresent(builder::truststoreTypeValue);

        // Parse client certificate properties
        takeOptional(props, SSL_CLIENT_CERT_PATH).ifPresent(builder::clientCertPathValue);
        takeOptional(props, SSL_CLIENT_KEY_PATH).ifPresent(builder::clientKeyPathValue);
        takeOptional(props, SSL_CA_CERT_PATH).ifPresent(builder::caCertPathValue);

        return builder.build();
    }

    /**
     * Returns default SSL properties.
     *
     * @return A SslProperties instance with default values
     */
    public static SslProperties defaultProperties() {
        return builder().build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the SSL properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (sslDisabledFlag) {
            props.setProperty(SSL_DISABLED, String.valueOf(sslDisabledFlag));
        }

        if (truststorePathValue != null) {
            props.setProperty(SSL_TRUSTSTORE_PATH, truststorePathValue);
        }

        if (truststorePasswordValue != null) {
            props.setProperty(SSL_TRUSTSTORE_PASSWORD, truststorePasswordValue);
        }

        if (!truststoreTypeValue.equals(DEFAULT_TRUSTSTORE_TYPE)) {
            props.setProperty(SSL_TRUSTSTORE_TYPE, truststoreTypeValue);
        }

        if (clientCertPathValue != null) {
            props.setProperty(SSL_CLIENT_CERT_PATH, clientCertPathValue);
        }

        if (clientKeyPathValue != null) {
            props.setProperty(SSL_CLIENT_KEY_PATH, clientKeyPathValue);
        }

        if (caCertPathValue != null) {
            props.setProperty(SSL_CA_CERT_PATH, caCertPathValue);
        }

        return props;
    }

    /**
     * SSL/TLS connection modes supported by the DataCloud JDBC driver.
     */
    @Getter
    public enum SslMode {
        /** SSL disabled - plaintext connection (testing only) */
        DISABLED("plaintext"),

        /** SSL with system truststore - default secure mode */
        DEFAULT_TLS("SSL with system truststore (one-sided TLS)"),

        /** SSL with custom truststore configuration - custom CA or truststore */
        ONE_SIDED_TLS("SSL with custom truststore (one-sided TLS)"),

        /** SSL with mutual authentication - client certificates required */
        MUTUAL_TLS("SSL with client certificates (two-sided TLS)");

        private final String description;

        SslMode(String description) {
            this.description = description;
        }
    }

    /**
     * Determines the appropriate SSL mode based on properties.
     */
    public SslMode determineSslMode() {
        if (sslDisabledFlag) {
            return SslMode.DISABLED;
        }

        boolean hasTrustConfig = hasTrustConfiguration();
        boolean hasClientCert = hasClientCertificates();

        if (hasClientCert) {
            return SslMode.MUTUAL_TLS;
        } else if (hasTrustConfig) {
            return SslMode.ONE_SIDED_TLS;
        } else {
            return SslMode.DEFAULT_TLS;
        }
    }

    /**
     * Checks if custom trust configuration is provided.
     */
    private boolean hasTrustConfiguration() {
        return truststorePathValue != null || caCertPathValue != null;
    }

    /**
     * Checks if client certificates are provided.
     */
    private boolean hasClientCertificates() {
        boolean hasCert = clientCertPathValue != null;
        boolean hasKey = clientKeyPathValue != null;

        if (hasCert && !hasKey) {
            throw new IllegalArgumentException("Client certificate provided but private key is missing");
        }
        if (!hasCert && hasKey) {
            throw new IllegalArgumentException("Client private key provided but certificate is missing");
        }

        return hasCert && hasKey;
    }

    /**
     * Creates a gRPC channel builder with appropriate SSL/TLS configuration.
     *
     * @param host The target host
     * @param port The target port
     * @return Configured ManagedChannelBuilder
     * @throws DataCloudJDBCException if channel creation fails
     */
    public ManagedChannelBuilder<?> createChannelBuilder(String host, int port) throws DataCloudJDBCException {
        SslMode sslMode = determineSslMode();
        String endpoint = host + ":" + port;

        log.info("Creating {} connection to {}", sslMode.getDescription(), endpoint);

        switch (sslMode) {
            case DISABLED:
                return createPlaintextChannelBuilder(host, port);
            case DEFAULT_TLS:
                return createSslWithSystemTrustBuilder(host, port);
            case ONE_SIDED_TLS:
            case MUTUAL_TLS:
                return createCustomSslChannelBuilder(host, port);
            default:
                throw new IllegalStateException("Unsupported SSL mode: " + sslMode);
        }
    }

    /**
     * Creates a plaintext channel builder (no encryption).
     */
    private ManagedChannelBuilder<?> createPlaintextChannelBuilder(String host, int port) {
        log.info("Creating plaintext connection to {}:{}", host, port);

        return ManagedChannelBuilder.forAddress(host, port).usePlaintext();
    }

    /**
     * Creates an SSL channel builder using system truststore.
     */
    private ManagedChannelBuilder<?> createSslWithSystemTrustBuilder(String host, int port) {
        log.info("Creating SSL with system truststore (one-sided TLS) connection to {}:{}", host, port);

        // Use NettyChannelBuilder for proper SSL support
        return NettyChannelBuilder.forAddress(host, port).useTransportSecurity();
    }

    /**
     * Creates an SSL channel builder with custom trust configuration.
     * Implements proper SSL context with custom certificates and truststore.
     */
    private ManagedChannelBuilder<?> createCustomSslChannelBuilder(String host, int port)
            throws DataCloudJDBCException {
        log.info("Creating SSL with custom configuration connection to {}:{}", host, port);

        try {
            // Create SSL context with custom configuration
            // Validation will be done in the specific SSL context creation methods
            return createSslContextBuilder(host, port);
        } catch (Exception e) {
            log.error("Failed to create custom SSL configuration: {}", e.getMessage(), e);
            throw new DataCloudJDBCException("Failed to create SSL configuration: " + e.getMessage(), e);
        }
    }

    /**
     * Creates SSL context builder with custom certificates and truststore.
     */
    private ManagedChannelBuilder<?> createSslContextBuilder(String host, int port) throws DataCloudJDBCException {
        try {
            log.info(
                    "Using custom SSL configuration with certificates: client_cert={}, client_key={}, ca_cert={}, truststore={}",
                    clientCertPathValue,
                    clientKeyPathValue,
                    caCertPathValue,
                    truststorePathValue);

            // Create SSL context with custom configuration
            SslContext sslContext = createSslContext();

            // Use NettyChannelBuilder with custom SSL context
            return NettyChannelBuilder.forAddress(host, port).sslContext(sslContext);
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create SSL context: " + e.getMessage(), e);
        }
    }

    /**
     * Creates SSL context with custom certificates and truststore.
     */
    private SslContext createSslContext() throws DataCloudJDBCException {
        try {
            boolean hasClientCert = hasClientCertificates();

            if (hasClientCert) {
                return createTwoSidedTlsContext();
            } else {
                return createOneSidedTlsContext();
            }
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create SSL context: " + e.getMessage(), e);
        }
    }

    /**
     * Creates SSL context for one-sided TLS (server authentication only).
     */
    private SslContext createOneSidedTlsContext() throws Exception {
        if (truststorePathValue != null) {
            // JKS truststore
            log.info("Using JKS truststore for server verification: {}", truststorePathValue);
            TrustManagerFactory tmf = createTrustManagerFactory();
            return GrpcSslContexts.forClient().trustManager(tmf).build();
        } else if (caCertPathValue != null) {
            // PEM CA certificate
            log.info("Using PEM CA certificate for server verification: {}", caCertPathValue);
            validatePemFile(caCertPathValue, "CA certificate");
            TrustManagerFactory tmf = createCaTrustManagerFactory();
            return GrpcSslContexts.forClient().trustManager(tmf).build();
        } else {
            // System truststore (fallback)
            log.info("Using system truststore for server verification");
            TrustManagerFactory tmf = createSystemTrustManagerFactory();
            return GrpcSslContexts.forClient().trustManager(tmf).build();
        }
    }

    /**
     * Creates SSL context for two-sided TLS (mutual authentication).
     */
    private SslContext createTwoSidedTlsContext() throws Exception {
        // Validate client certificate files
        validateCertificateFiles(clientCertPathValue, clientKeyPathValue, caCertPathValue);

        if (truststorePathValue != null) {
            // JKS truststore + PEM client certs
            log.info("Using JKS truststore + PEM client certificates for mutual TLS");
            TrustManagerFactory tmf = createTrustManagerFactory();
            return GrpcSslContexts.forClient()
                    .trustManager(tmf)
                    .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                    .build();
        } else if (caCertPathValue != null) {
            // PEM CA cert + PEM client certs
            log.info("Using PEM CA certificate + PEM client certificates for mutual TLS");
            return GrpcSslContexts.forClient()
                    .trustManager(new File(caCertPathValue))
                    .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                    .build();
        } else {
            // System truststore + PEM client certs
            log.info("Using system truststore + PEM client certificates for mutual TLS");
            TrustManagerFactory tmf = createSystemTrustManagerFactory();
            return GrpcSslContexts.forClient()
                    .trustManager(tmf)
                    .keyManager(new File(clientCertPathValue), new File(clientKeyPathValue))
                    .build();
        }
    }

    /**
     * Creates TrustManagerFactory from CA certificate.
     */
    private TrustManagerFactory createCaTrustManagerFactory() throws DataCloudJDBCException {
        try {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);

            // Load CA certificate
            try (InputStream caCertStream = new FileInputStream(caCertPathValue)) {
                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                X509Certificate caCert = (X509Certificate) certFactory.generateCertificate(caCertStream);
                trustStore.setCertificateEntry(CA_CERT_ENTRY_NAME, caCert);
            }

            return createTrustManagerFactoryFromKeyStore(trustStore);
        } catch (Exception e) {
            throw new DataCloudJDBCException(
                    "Failed to create trust manager from CA certificate: " + e.getMessage(), e);
        }
    }

    /**
     * Creates TrustManagerFactory from JKS truststore.
     */
    private TrustManagerFactory createTrustManagerFactory() throws DataCloudJDBCException {
        try {
            KeyStore trustStore = KeyStore.getInstance(truststoreTypeValue);
            try (FileInputStream fis = new FileInputStream(truststorePathValue)) {
                char[] password = truststorePasswordValue != null ? truststorePasswordValue.toCharArray() : null;
                trustStore.load(fis, password);
            }

            return createTrustManagerFactoryFromKeyStore(trustStore);
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create trust manager from truststore: " + e.getMessage(), e);
        }
    }

    /**
     * Creates TrustManagerFactory from a KeyStore.
     * Consolidated method to avoid code duplication.
     */
    private TrustManagerFactory createTrustManagerFactoryFromKeyStore(KeyStore trustStore)
            throws DataCloudJDBCException {
        try {
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create trust manager factory: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a system TrustManagerFactory.
     * Consolidated method for system truststore creation.
     */
    private TrustManagerFactory createSystemTrustManagerFactory() throws DataCloudJDBCException {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            return tmf;
        } catch (Exception e) {
            throw new DataCloudJDBCException("Failed to create system trust manager: " + e.getMessage(), e);
        }
    }

    /**
     * Validates that a PEM file exists and is readable.
     */
    private void validatePemFile(String path, String description) throws DataCloudJDBCException {
        File file = new File(path);
        if (!file.exists()) {
            throw new DataCloudJDBCException(
                    "File not found, ensure the file exists and the path is correct. description=" + description
                            + ", path=" + path);
        }
        if (!file.canRead()) {
            throw new DataCloudJDBCException(
                    "File is not readable, check file permissions. description=" + description + ", path=" + path);
        }
        if (file.length() == 0) {
            throw new DataCloudJDBCException(description + " file is empty: " + path);
        }
    }

    /**
     * Validates that all certificate files for mutual TLS exist and are readable.
     */
    private void validateCertificateFiles(String clientCertPath, String clientKeyPath, String caCertPath)
            throws DataCloudJDBCException {
        if (clientCertPath == null) {
            throw new DataCloudJDBCException("Client certificate path cannot be null");
        }
        if (clientKeyPath == null) {
            throw new DataCloudJDBCException("Client private key path cannot be null");
        }

        validatePemFile(clientCertPath, "Client certificate");
        validatePemFile(clientKeyPath, "Client private key");

        // CA certificate is optional (might use JKS truststore instead)
        if (caCertPath != null) {
            validatePemFile(caCertPath, "CA certificate");
        }
        log.info("Certificate file validation completed successfully");
    }
}
