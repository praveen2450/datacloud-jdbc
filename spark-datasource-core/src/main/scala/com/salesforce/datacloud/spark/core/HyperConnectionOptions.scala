package com.salesforce.datacloud.spark.core

import java.util.Properties
import com.salesforce.datacloud.jdbc.core.{
  DataCloudConnection,
  DataCloudJdbcManagedChannel
}
import io.grpc.ManagedChannelBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.io.{File, FileInputStream}
import java.security.KeyStore
import javax.net.ssl.{TrustManagerFactory, KeyManagerFactory}
import io.grpc.{
  ClientInterceptor,
  MethodDescriptor,
  CallOptions,
  Channel,
  ClientCall,
  ForwardingClientCall,
  Metadata
}
import scala.collection.JavaConverters._

case class HyperConnectionOptions(
    host: String,
    port: Int,
    useTls: Boolean = false,
    audience: Option[String] = None,
    // Unified SSL configuration - support both approaches
    sslMode: String = "auto", // "auto", "jks", "pem", "disabled"
    keystorePath: Option[String] = None,
    keystorePassword: Option[String] = None,
    certPath: Option[String] = None,
    keyPath: Option[String] = None,
    caCertPath: Option[String] = None,
    tenantId: Option[String] = None,
    coreTenantId: Option[String] = None
) {

  def createConnection(): DataCloudConnection = {
    // 🔍 Debug: Print all connection options
    println(s"🔧 HyperConnectionOptions Debug:")
    println(s"   • host: ${host}")
    println(s"   • port: ${port}")
    println(s"   • useTls: ${useTls}")
    println(s"   • sslMode: ${sslMode}")
    println(s"   • keystorePath: ${keystorePath.getOrElse("NOT_SET")}")
    println(s"   • certPath: ${certPath.getOrElse("NOT_SET")}")
    println(s"   • audience: ${audience.getOrElse("NOT_SET")}")
    println(s"   • tenantId: ${tenantId.getOrElse("NOT_SET")}")
    println(s"   • coreTenantId: ${coreTenantId.getOrElse("NOT_SET")}")

    val baseChannelBuilder = createTlsChannelBuilder()

    // Add tenant header interceptor if tenant info is provided (like your existing pattern)
    val channelBuilder = if (tenantId.isDefined || coreTenantId.isDefined) {
      val tenantInterceptor = createTenantHeaderInterceptor()
      baseChannelBuilder.intercept(tenantInterceptor)
    } else {
      println(
        "⚠️  No tenant information provided - this may cause RBAC issues!"
      )
      baseChannelBuilder
    }

    val properties = new Properties()

    // Add audience for authentication if provided
    audience.foreach { aud =>
      properties.setProperty("audience", aud)
      println(s"🎫 Setting audience property: ${aud}")
    }

    // Add tenant information (DataCloudConnection likely handles these as headers)
    tenantId.foreach { tid =>
      properties.setProperty("tenantId", tid)
      // Also try the header-style property names
      properties.setProperty("ctx-tenant-id", tid)
      println(s"📋 Setting tenantId property: ${tid}")
    }
    coreTenantId.foreach { ctid =>
      properties.setProperty("coreTenantId", ctid)
      // Also try the header-style property names
      properties.setProperty("ctx-tenant-core-id", ctid)
      println(s"📋 Setting coreTenantId property: ${ctid}")
    }

    println(
      s"🏗️  Creating DataCloudConnection with ${properties.size()} properties"
    )

    DataCloudConnection.of(channelBuilder, properties)
  }

  private def createTlsChannelBuilder(): ManagedChannelBuilder[_] = {
    sslMode.toLowerCase match {
      case "disabled" =>
        println("🔓 SSL disabled - using plaintext connection")
        ManagedChannelBuilder.forAddress(host, port).usePlaintext()

      case "jks" =>
        println("🔒 Using JKS keystore mode")
        createJksBasedSslChannelBuilder()

      case "pem" =>
        println("📄 Using PEM certificate mode")
        createPemBasedSslChannelBuilder()

      case "auto" | _ =>
        println("🔍 Auto-detecting SSL configuration...")
        autoDetectSslConfiguration()
    }
  }

  private def autoDetectSslConfiguration(): ManagedChannelBuilder[_] = {
    // First try explicit paths if provided
    if (keystorePath.isDefined) {
      println("🔒 Found keystore path - using JKS mode")
      return createJksBasedSslChannelBuilder()
    }

    if (certPath.isDefined && keyPath.isDefined) {
      println("📄 Found PEM paths - using PEM mode")
      return createPemBasedSslChannelBuilder()
    }

    // Try standard production paths
    val productionJksPaths = Seq(
      "/etc/pki-agent/keystore/client.jks",
      "/tmp/client.jks"
    )

    productionJksPaths.find(path => new File(path).exists()) match {
      case Some(jksPath) =>
        println(s"🔒 Found production JKS keystore: ${jksPath}")
        createJksBasedSslChannelBuilder()
      case None =>
        // Try PEM paths
        val pemPaths = (
          certPath.getOrElse("/etc/identity/client/certificates/client.pem"),
          keyPath.getOrElse("/etc/identity/client/keys/client-key.pem"),
          caCertPath.getOrElse("/etc/identity/ca/cacerts.pem")
        )

        if (
          new File(pemPaths._1).exists() && new File(pemPaths._2)
            .exists() && new File(pemPaths._3).exists()
        ) {
          println("📄 Found PEM certificates - using PEM mode")
          createPemBasedSslChannelBuilder()
        } else {
          println("⚠️  No certificates found - falling back to basic TLS")
          ManagedChannelBuilder.forAddress(host, port).useTransportSecurity()
        }
    }
  }

  private def createJksBasedSslChannelBuilder(): ManagedChannelBuilder[_] = {
    try {
      // Production keystore factory with configurable paths
      val dksFactory =
        new ProductionKeyStoreFactory(keystorePath, keystorePassword)
      val dks = dksFactory.getInstance()

      // Get KeyManager from JKS (for client authentication)
      val kmf = dks.getKeyManagerFactory()

      // Check if custom CA cert path is provided for mixed mode
      if (caCertPath.isDefined && new File(caCertPath.get).exists()) {
        println(s"🔗 Mixed mode: JKS client auth + PEM CA verification")
        println(s"   • Client keystore: ${keystorePath.getOrElse("default")}")
        println(s"   • CA certificates: ${caCertPath.get}")

        // Create mixed SSL context: JKS KeyManager + PEM TrustManager
        val sslContext = createMixedSslContext(kmf, caCertPath.get)
        createNettyChannelBuilder(sslContext)
      } else {
        // Traditional JKS mode: use keystore for both client auth AND server verification
        println(
          "🔒 Pure JKS mode: keystore for both client auth and server verification"
        )
        val tmf = dks.getTrustManagerFactory()
        val sslContext = createSslContext(tmf, kmf)
        createNettyChannelBuilder(sslContext)
      }

    } catch {
      case e: Exception =>
        println(s"⚠️  JKS SSL configuration failed: ${e.getMessage}")
        println("💡 Try specifying explicit keystore_path or use ssl_mode=pem")
        throw e
    }
  }

  private def createPemBasedSslChannelBuilder(): ManagedChannelBuilder[_] = {
    try {
      // Local development: use PEM certificates
      println("🔓 Using PEM-based TLS mode (local development)")
      val sslContext = createPemBasedSslContext()
      createNettyChannelBuilder(sslContext)
    } catch {
      case e: Exception =>
        println(s"⚠️  PEM-based SSL setup failed: ${e.getMessage}")
        println(
          "📋 Falling back to basic TLS (may fail without proper certificates)"
        )
        ManagedChannelBuilder.forAddress(host, port).useTransportSecurity()
    }
  }

  private def createPemBasedSslContext(): Any = {
    // PEM file paths (configurable or defaults)
    val CERT_CHAIN_FILE =
      certPath.getOrElse("/etc/identity/client/certificates/client.pem")
    val PRIVATE_KEY_FILE =
      keyPath.getOrElse("/etc/identity/client/keys/client-key.pem")
    val TRUST_CERT_COLLECTION_FILE =
      caCertPath.getOrElse("/etc/identity/ca/cacerts.pem")

    val certChainFile = new File(CERT_CHAIN_FILE)
    val privateKeyFile = new File(PRIVATE_KEY_FILE)
    val trustCertFile = new File(TRUST_CERT_COLLECTION_FILE)

    // Verify files exist
    if (!certChainFile.exists()) {
      throw new java.io.FileNotFoundException(
        s"Certificate chain file not found: ${CERT_CHAIN_FILE}"
      )
    }
    if (!privateKeyFile.exists()) {
      throw new java.io.FileNotFoundException(
        s"Private key file not found: ${PRIVATE_KEY_FILE}"
      )
    }
    if (!trustCertFile.exists()) {
      throw new java.io.FileNotFoundException(
        s"Trust certificate file not found: ${TRUST_CERT_COLLECTION_FILE}"
      )
    }

    println(s"📁 Using PEM certificates:")
    println(s"   • Cert chain: ${CERT_CHAIN_FILE}")
    println(s"   • Private key: ${PRIVATE_KEY_FILE}")
    println(s"   • Trust certs: ${TRUST_CERT_COLLECTION_FILE}")

    try {
      // Use reflection for shaded GrpcSslContexts.forClient().keyManager(certFile, keyFile).trustManager(trustFile).build()
      val grpcSslContextsClass = Class.forName(
        "com.salesforce.datacloud.shaded.io.grpc.netty.GrpcSslContexts"
      )
      val forClientMethod = grpcSslContextsClass.getMethod("forClient")
      val sslContextBuilder = forClientMethod.invoke(null)

      val builderClass = sslContextBuilder.getClass
      println(s"🔧 SSL Context Builder class: ${builderClass.getName}")

      // CRITICAL FIX: Set trustManager FIRST, then keyManager
      // This ensures the trust store is properly configured before client certificates
      println("🔒 Setting up trust manager...")
      val trustManagerMethod =
        builderClass.getMethod("trustManager", classOf[File])
      trustManagerMethod.invoke(
        sslContextBuilder,
        trustCertFile.asInstanceOf[Object]
      )
      println("✅ Trust manager configured")

      // Then set up client certificate authentication
      println("🔑 Setting up key manager...")
      val keyManagerMethod =
        builderClass.getMethod("keyManager", classOf[File], classOf[File])
      keyManagerMethod.invoke(
        sslContextBuilder,
        certChainFile.asInstanceOf[Object],
        privateKeyFile.asInstanceOf[Object]
      )
      println("✅ Key manager configured")

      // build()
      println("🏗️  Building SSL context...")
      val buildMethod = builderClass.getMethod("build")
      val sslContext = buildMethod.invoke(sslContextBuilder)
      println(s"✅ SSL context created: ${sslContext.getClass.getName}")
      
      sslContext
      
    } catch {
      case e: Exception =>
        println(s"❌ SSL Context creation failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  private def createMixedSslContext(
      keyManagerFactory: KeyManagerFactory,
      caCertPemPath: String
  ): Any = {
    println(s"🔗 Creating mixed SSL context:")
    println(s"   • KeyManager: From JKS keystore")
    println(s"   • TrustManager: From PEM CA file (${caCertPemPath})")

    val caCertFile = new File(caCertPemPath)
    if (!caCertFile.exists()) {
      throw new java.io.FileNotFoundException(
        s"CA certificate file not found: ${caCertPemPath}"
      )
    }

    // Use reflection for shaded GrpcSslContexts.forClient()
    val grpcSslContextsClass = Class.forName(
              "com.salesforce.datacloud.shaded.io.grpc.netty.GrpcSslContexts"
    )
    val forClientMethod = grpcSslContextsClass.getMethod("forClient")
    val sslContextBuilder = forClientMethod.invoke(null)

    val builderClass = sslContextBuilder.getClass

    // keyManager(KeyManagerFactory) - from JKS
    val keyManagerFactoryMethod =
      builderClass.getMethod("keyManager", classOf[KeyManagerFactory])
    keyManagerFactoryMethod.invoke(
      sslContextBuilder,
      keyManagerFactory.asInstanceOf[Object]
    )

    // trustManager(File) - from PEM CA cert file
    val trustManagerMethod =
      builderClass.getMethod("trustManager", classOf[File])
    trustManagerMethod.invoke(
      sslContextBuilder,
      caCertFile.asInstanceOf[Object]
    )

    // build()
    val buildMethod = builderClass.getMethod("build")
    buildMethod.invoke(sslContextBuilder)
  }

  private def createSslContext(
      tmf: TrustManagerFactory,
      kmf: KeyManagerFactory
  ): Any = {
    // Use reflection for shaded GrpcSslContexts.forClient().trustManager(tmf).keyManager(kmf).build()
    val grpcSslContextsClass = Class.forName(
              "com.salesforce.datacloud.shaded.io.grpc.netty.GrpcSslContexts"
    )
    val forClientMethod = grpcSslContextsClass.getMethod("forClient")
    val sslContextBuilder = forClientMethod.invoke(null)

    val builderClass = sslContextBuilder.getClass

    // trustManager(tmf)
    val trustManagerMethod =
      builderClass.getMethod("trustManager", classOf[TrustManagerFactory])
    trustManagerMethod.invoke(sslContextBuilder, tmf.asInstanceOf[Object])

    // keyManager(kmf)
    val keyManagerMethod =
      builderClass.getMethod("keyManager", classOf[KeyManagerFactory])
    keyManagerMethod.invoke(sslContextBuilder, kmf.asInstanceOf[Object])

    // build()
    val buildMethod = builderClass.getMethod("build")
    buildMethod.invoke(sslContextBuilder)
  }

  private def createNettyChannelBuilder(
      sslContext: Any
  ): ManagedChannelBuilder[_] = {
    // Use reflection for NettyChannelBuilder.forAddress(host, port).negotiationType(NegotiationType.TLS).sslContext(sslContext)
    val nettyChannelBuilderClass = Class.forName(
              "com.salesforce.datacloud.shaded.io.grpc.netty.NettyChannelBuilder"
    )
    val forAddressMethod = nettyChannelBuilderClass.getMethod(
      "forAddress",
      classOf[String],
      classOf[Int]
    )
    val nettyBuilder = forAddressMethod.invoke(null, host, Int.box(port))

    // negotiationType(NegotiationType.TLS)
    val negotiationTypeClass = Class.forName(
              "com.salesforce.datacloud.shaded.io.grpc.netty.NegotiationType"
    )
    val tlsField = negotiationTypeClass.getField("TLS")
    val tlsValue = tlsField.get(null)

    val negotiationTypeMethod = nettyChannelBuilderClass.getMethod(
      "negotiationType",
      negotiationTypeClass
    )
    negotiationTypeMethod.invoke(nettyBuilder, tlsValue.asInstanceOf[Object])

    // sslContext(sslContext)
    val sslContextMethod = nettyChannelBuilderClass.getMethod(
      "sslContext",
      Class.forName(
        "com.salesforce.datacloud.shaded.io.netty.handler.ssl.SslContext"
      )
    )
    sslContextMethod.invoke(nettyBuilder, sslContext.asInstanceOf[Object])

    nettyBuilder.asInstanceOf[ManagedChannelBuilder[_]]
  }

  private def createTenantHeaderInterceptor(): ClientInterceptor = {
    // Create a simple header interceptor using the HeaderMutatingClientInterceptor pattern
    new ClientInterceptor {
      override def interceptCall[ReqT, RespT](
          method: MethodDescriptor[ReqT, RespT],
          callOptions: CallOptions,
          next: Channel
      ): ClientCall[ReqT, RespT] = {

        val nextCall = next.newCall(method, callOptions)

        new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
          nextCall
        ) {
          override def start(
              responseListener: ClientCall.Listener[RespT],
              headers: Metadata
          ): Unit = {
            println(s"🚀 Intercepting gRPC call: ${method.getFullMethodName}")

            // Add tenant headers
            tenantId.foreach { tid =>
              val tenantKey = Metadata.Key.of(
                "ctx-tenant-id",
                Metadata.ASCII_STRING_MARSHALLER
              )
              headers.put(tenantKey, tid)
              println(s"📋 Added ctx-tenant-id header: ${tid}")
            }

            coreTenantId.foreach { ctid =>
              val coreTenantKey = Metadata.Key.of(
                "ctx-tenant-core-id",
                Metadata.ASCII_STRING_MARSHALLER
              )
              headers.put(coreTenantKey, ctid)
              println(s"📋 Added ctx-tenant-core-id header: ${ctid}")
            }

            // 🔍 Debug: Print all headers being sent
            println(s"🌐 Headers added for tenant authentication")
            if (tenantId.isDefined) {
              println(s"   • ctx-tenant-id: ${tenantId.get}")
            }
            if (coreTenantId.isDefined) {
              println(s"   • ctx-tenant-core-id: ${coreTenantId.get}")
            }

            super.start(responseListener, headers)
          }
        }
      }
    }
  }
}

/** Production keystore factory that mimics your existing DynamicKeyStoreFactory
  * pattern
  */
class ProductionKeyStoreFactory(
    private val defaultKeystorePath: Option[String],
    private val defaultKeystorePassword: Option[String]
) {
  private val DEFAULT_PASSWORD_KEY = "keystore.defaultPass"
  private val FALLBACK_PASSWORD =
    "changeit" // Fallback if properties not available

  private def readDefaultPasswordFromConfig(): String = {
    try {
      val inputStream =
        getClass.getClassLoader.getResourceAsStream("application.properties")
      if (inputStream != null) {
        val properties = new java.util.Properties()
        properties.load(inputStream)
        val password = properties.getProperty(DEFAULT_PASSWORD_KEY)
        inputStream.close()

        if (password != null) {
          println(s"📋 Using keystore password from application.properties")
          return password
        }
      }
    } catch {
      case e: Exception =>
        println(
          s"⚠️  Failed to read password from application.properties: ${e.getMessage}"
        )
    }

    println(s"📋 Using fallback keystore password")
    FALLBACK_PASSWORD
  }

  def getInstance(): ProductionKeyStore = {
    val password =
      defaultKeystorePassword.getOrElse(readDefaultPasswordFromConfig())

    // If explicit path provided, use it
    if (defaultKeystorePath.isDefined) {
      val keystoreFile = new File(defaultKeystorePath.get)
      if (keystoreFile.exists()) {
        println(s"📁 Using explicit keystore path: ${defaultKeystorePath.get}")
        return loadKeystore(keystoreFile, password)
      } else {
        println(
          s"⚠️  Explicit keystore path not found: ${defaultKeystorePath.get}"
        )
      }
    }

    // Use pure Java approach - no Scala collections, try standard paths
    val keystorePath1 = "/etc/pki-agent/keystore/client.jks" // EMR on EKS
    val keystorePath2 = "/tmp/client.jks" // EMR on EC2

    // Check first path
    val keystoreFile1 = new File(keystorePath1)
    if (keystoreFile1.exists()) {
      try {
        println(s"📁 Loading keystore from: ${keystorePath1}")
        return loadKeystore(keystoreFile1, password)
      } catch {
        case e: Exception =>
          println(
            s"⚠️  Failed to load keystore from ${keystorePath1}: ${e.getMessage}"
          )
      }
    } else {
      println(s"📋 Keystore not found: ${keystorePath1}")
    }

    // Check second path
    val keystoreFile2 = new File(keystorePath2)
    if (keystoreFile2.exists()) {
      try {
        println(s"📁 Loading keystore from: ${keystorePath2}")
        return loadKeystore(keystoreFile2, password)
      } catch {
        case e: Exception =>
          println(
            s"⚠️  Failed to load keystore from ${keystorePath2}: ${e.getMessage}"
          )
      }
    } else {
      println(s"📋 Keystore not found: ${keystorePath2}")
    }

    throw new IllegalStateException(
      s"Unable to initialize keystore for SSL context. Checked paths: ${keystorePath1}, ${keystorePath2}"
    )
  }

  private def loadKeystore(
      keystoreFile: File,
      password: String
  ): ProductionKeyStore = {
    val keystore = KeyStore.getInstance(KeyStore.getDefaultType)
    val inputStream = new FileInputStream(keystoreFile)
    try {
      keystore.load(inputStream, password.toCharArray)

      // Create managers (like your DynamicKeyStoreFactory)
      val keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(keystore, password.toCharArray)

      val trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(keystore)

      println(s"✅ Successfully loaded keystore with ${keystore.size()} entries")

      new ProductionKeyStore(
        keystore,
        password.toCharArray,
        keyManagerFactory,
        trustManagerFactory
      )

    } finally {
      inputStream.close()
    }
  }
}

/** Production keystore that follows your existing DynamicKeyStore pattern
  */
class ProductionKeyStore(
    private val keystore: KeyStore,
    private val password: Array[Char],
    private val keyManagerFactory: KeyManagerFactory,
    private val trustManagerFactory: TrustManagerFactory
) {

  def getKeystore(): KeyStore = keystore

  def getTruststore(): KeyStore =
    keystore // Same store for both (like your implementation)

  def getKeyPassword(): Array[Char] = password

  def getKeystorePassword(): Array[Char] = password

  def getKeyManagerFactory(): KeyManagerFactory = keyManagerFactory

  def getTrustManagerFactory(): TrustManagerFactory = trustManagerFactory
}

object HyperConnectionOptions {
  def fromOptions(
      options: java.util.Map[String, String]
  ): HyperConnectionOptions = {
    var host = options.get("host")
    if (host == null) {
      host = "127.0.0.1"
    }

    // Use Integer.parseInt for Scala 2.12/2.13 compatibility
    val port = Integer.parseInt(options.get("port"))

    // Determine if TLS should be used
    val useTls = Option(options.get("use_tls"))
      .map(_.toLowerCase == "true")
      .getOrElse(port == 443) // Default to TLS for port 443

    // Get audience for authentication
    val audience = Option(options.get("audience"))

    // SSL Configuration (unified approach)
    val sslMode = Option(options.get("ssl_mode")).getOrElse {
      // Backward compatibility: map old insecure_tls option
      val insecureTls = Option(options.get("insecure_tls"))
        .map(_.toLowerCase == "true")
        .getOrElse(false)

      if (useTls || port == 443) {
        if (insecureTls) "pem" else "auto"
      } else {
        "disabled"
      }
    }

    // SSL certificate paths (optional - for explicit configuration)
    val keystorePath = Option(options.get("keystore_path"))
    val keystorePassword = Option(options.get("keystore_password"))
    val certPath = Option(options.get("cert_path"))
    val keyPath = Option(options.get("key_path"))
    val caCertPath = Option(options.get("ca_cert_path"))

    // Get tenantId and coreTenantId
    val tenantId = Option(options.get("tenant_id"))
    val coreTenantId = Option(options.get("core_tenant_id"))

    HyperConnectionOptions(
      host,
      port,
      useTls,
      audience,
      sslMode,
      keystorePath,
      keystorePassword,
      certPath,
      keyPath,
      caCertPath,
      tenantId,
      coreTenantId
    )
  }
}
