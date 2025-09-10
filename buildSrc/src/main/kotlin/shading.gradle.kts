import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.gradleup.shadow") // NO version here
}

// Simple extension using extra properties
val shadingRelocations = project.objects.listProperty(String::class.java).convention(emptyList())
val shadingExcludes = project.objects.listProperty(String::class.java).convention(listOf("META-INF/*.SF","META-INF/*.DSA","META-INF/*.RSA"))
val shadingEnableZip64 = project.objects.property(Boolean::class.java).convention(true)
val shadingReplaceMainJar = project.objects.property(Boolean::class.java).convention(true)
val shadingServiceFileExcludes = project.objects.listProperty(String::class.java).convention(emptyList())

// Predefined configuration constants for common use cases
val JDBC_ZIP64_ENABLED = false  // JDBC uses standard JAR size
val SPARK_ZIP64_ENABLED = true  // Enable ZIP64 required for spark

val JDBC_ADDITIONAL_EXCLUSIONS = emptyList<String>()  // No additional exclusions for JDBC
val SPARK_ADDITIONAL_EXCLUSIONS = listOf(
    "org/apache/spark/**",    // Spark runtime exclusions - user provides these at runtime
    "scala/**",
    "org/scala-lang/**"
)

project.extensions.extraProperties["shadingRelocations"] = shadingRelocations
project.extensions.extraProperties["shadingExcludes"] = shadingExcludes
project.extensions.extraProperties["shadingEnableZip64"] = shadingEnableZip64
project.extensions.extraProperties["shadingReplaceMainJar"] = shadingReplaceMainJar
project.extensions.extraProperties["shadingServiceFileExcludes"] = shadingServiceFileExcludes

// Make configuration constants available to build scripts
project.extensions.extraProperties["JDBC_ZIP64_ENABLED"] = JDBC_ZIP64_ENABLED
project.extensions.extraProperties["SPARK_ZIP64_ENABLED"] = SPARK_ZIP64_ENABLED
project.extensions.extraProperties["JDBC_ADDITIONAL_EXCLUSIONS"] = JDBC_ADDITIONAL_EXCLUSIONS
project.extensions.extraProperties["SPARK_ADDITIONAL_EXCLUSIONS"] = SPARK_ADDITIONAL_EXCLUSIONS

// Shared configuration function to eliminate duplication between JDBC and Spark modules
fun Project.configureDataCloudShading(
    zip64: Boolean = true,
    additionalExclusions: List<String> = emptyList()
) {
    // Common relocations for both JDBC and Spark
    val commonRelocations = listOf(
        "com.google",
        "io.grpc", 
        "com.fasterxml.jackson",
        "dev.failsafe",
        "io.jsonwebtoken",
        "io.netty",
        "kotlin",
        "okhttp3",
        "okio",
        "org.apache.arrow",
        "org.apache.calcite",
        "org.apache.commons",
        "org.apache.hc"
    )
    
    // Common exclusions for both JDBC and Spark
    val commonExclusions = listOf(
        "META-INF/maven/**",
        "**/*.proto",
        "org.slf4j",
        "org.apache.calcite.avatica.remote.Driver",
        "META-INF/LICENSE*",
        "META-INF/NOTICE*",
        "META-INF/DEPENDENCIES",
        "META-INF/services/com.fasterxml.*",
        "META-INF/*.xml",
        "META-INF/*.SF",
        "META-INF/*.DSA",
        "META-INF/*.RSA",
        ".netbeans_automatic_build",
        "git.properties",
        "google-http-client.properties",
        "storage.v1.json",
        "pipes-fork-server-default-log4j2.xml",
        "dependencies.properties",
        "arrow-git.properties"
    ) + additionalExclusions
    
    // Common service file exclusions
    val commonServiceFileExclusions = listOf(
        "META-INF/services/java.sql.Driver"
    )
    
    // Configure the properties
    (extensions.extraProperties["shadingRelocations"] as org.gradle.api.provider.ListProperty<String>).set(commonRelocations)
    (extensions.extraProperties["shadingExcludes"] as org.gradle.api.provider.ListProperty<String>).set(commonExclusions)
    (extensions.extraProperties["shadingEnableZip64"] as org.gradle.api.provider.Property<Boolean>).set(zip64)
    (extensions.extraProperties["shadingReplaceMainJar"] as org.gradle.api.provider.Property<Boolean>).set(true)
    (extensions.extraProperties["shadingServiceFileExcludes"] as org.gradle.api.provider.ListProperty<String>).set(commonServiceFileExclusions)
}

// Make the function available to build scripts as an extension function
extensions.extraProperties["configureDataCloudShading"] = fun Project.(zip64: Boolean, additionalExclusions: List<String>) {
    configureDataCloudShading(zip64, additionalExclusions)
}

tasks.withType<ShadowJar>().configureEach {
    archiveClassifier.set("")
    archiveVersion.set("")

    isReproducibleFileOrder = true
    isPreserveFileTimestamps = false
    isZip64 = shadingEnableZip64.get()
    
    // Default service file merging with exclusions
    mergeServiceFiles {
        include("META-INF/services/*")
        shadingServiceFileExcludes.get().forEach { excludePattern ->
            exclude(excludePattern)
        }
    }
    
    // Apply exclusions
    shadingExcludes.get().forEach { exclude(it) }
    
    // Apply relocations with special handling for calcite
    shadingRelocations.get().forEach { pkg ->
        if (pkg == "org.apache.calcite") {
            relocate(pkg, "com.salesforce.datacloud.shaded.$pkg") {
                exclude("org.apache.calcite.avatica.remote.Driver")
            }
        } else {
            relocate(pkg, "com.salesforce.datacloud.shaded.$pkg")
        }
    }
}

// Configure jar replacement and assemble task dependencies
afterEvaluate {
    if (shadingReplaceMainJar.get()) {
        tasks.named<Jar>("jar").configure { enabled = false }
        
        // Create additional shadowJar with "shaded" classifier for publishing (with version like original)
        val shadedJar = tasks.register("shadedJar", com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar::class) {
            from(project.the<SourceSetContainer>()["main"].output)
            configurations = listOf(project.configurations["runtimeClasspath"])
            archiveClassifier.set("shaded")
            // OVERRIDE the archiveVersion.set("") from withType<ShadowJar> to restore version
            // This will produce: jdbc-0.30.0-LOCAL-shaded.jar (like original)
            archiveVersion.set(project.version.toString())
        // Apply same configuration as main shadowJar
        isReproducibleFileOrder = true
        isPreserveFileTimestamps = false
        isZip64 = shadingEnableZip64.get()
        mergeServiceFiles {
            include("META-INF/services/*")
            shadingServiceFileExcludes.get().forEach { excludePattern ->
                exclude(excludePattern)
            }
        }
        shadingExcludes.get().forEach { exclude(it) }
        shadingRelocations.get().forEach { pkg ->
            if (pkg == "org.apache.calcite") {
                // Special case: exclude avatica driver from calcite relocation
                relocate(pkg, "com.salesforce.datacloud.shaded.$pkg") {
                    exclude("org.apache.calcite.avatica.remote.Driver")
                }
            } else {
                relocate(pkg, "com.salesforce.datacloud.shaded.$pkg")
            }
        }
        }
        
        tasks.named("assemble").configure { 
            dependsOn(tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>())
            dependsOn(shadedJar)
        }
        
        // Make shadowJar the main artifact for project dependencies
        configurations.named("runtimeElements").configure {
            outgoing.artifacts.clear()
            outgoing.artifact(tasks.named("shadowJar"))
        }
        configurations.named("apiElements").configure {
            outgoing.artifacts.clear()
            outgoing.artifact(tasks.named("shadowJar"))
        }
        
        // Configure publishing to include the shaded JAR
        plugins.withId("maven-publish") {
            extensions.configure<org.gradle.api.publish.PublishingExtension> {
                publications {
                    named<org.gradle.api.publish.maven.MavenPublication>("mavenJava") {
                        artifact(shadedJar.get()) {
                            classifier = "shaded"
                        }
                    }
                }
            }
        }
    }
}
