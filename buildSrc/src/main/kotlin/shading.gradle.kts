import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.*
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy

plugins {
    java
    id("com.gradleup.shadow") // NO version here
}

// Simple extension using extra properties
val shadingRelocations = project.objects.listProperty(String::class.java).convention(emptyList())
val shadingExcludes = project.objects.listProperty(String::class.java).convention(listOf("META-INF/*.SF","META-INF/*.DSA","META-INF/*.RSA"))
val shadingReplaceMainJar = project.objects.property(Boolean::class.java).convention(true)
val shadingServiceFileExcludes = project.objects.listProperty(String::class.java).convention(emptyList())

val JDBC_ADDITIONAL_EXCLUSIONS = emptyList<String>()  // No additional exclusions for JDBC
val SPARK_ADDITIONAL_EXCLUSIONS = listOf(
    "org/apache/spark/**",    // Spark runtime exclusions - user provides these at runtime
    "scala/**",
    "org/scala-lang/**"
)

project.extensions.extraProperties["shadingRelocations"] = shadingRelocations
project.extensions.extraProperties["shadingExcludes"] = shadingExcludes
project.extensions.extraProperties["shadingReplaceMainJar"] = shadingReplaceMainJar
project.extensions.extraProperties["shadingServiceFileExcludes"] = shadingServiceFileExcludes

// Make configuration constants available to build scripts
project.extensions.extraProperties["JDBC_ADDITIONAL_EXCLUSIONS"] = JDBC_ADDITIONAL_EXCLUSIONS
project.extensions.extraProperties["SPARK_ADDITIONAL_EXCLUSIONS"] = SPARK_ADDITIONAL_EXCLUSIONS

// Shared configuration function to eliminate duplication between JDBC and Spark modules
fun Project.configureDataCloudShading(
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
    (extensions.extraProperties["shadingReplaceMainJar"] as org.gradle.api.provider.Property<Boolean>).set(true)
    (extensions.extraProperties["shadingServiceFileExcludes"] as org.gradle.api.provider.ListProperty<String>).set(commonServiceFileExclusions)
}

// Make the function available to build scripts as an extension function
extensions.extraProperties["configureDataCloudShading"] = fun Project.(additionalExclusions: List<String>) {
    configureDataCloudShading(additionalExclusions)
}

tasks.withType<ShadowJar>().configureEach {
    // Common configuration for all ShadowJar tasks
    isReproducibleFileOrder = true
    isPreserveFileTimestamps = false
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
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

// Extension function to configure JAR artifacts (main, shaded, original)
fun Project.configureJarArtifacts() {
    // Default JAR with no classifier but is shaded to be used for DBeaver until we can update driver definition
    tasks.named<ShadowJar>("shadowJar").configure {
        archiveBaseName.set(project.name)
        archiveClassifier.set("")
        shouldRunAfter(tasks.named("jar"))
    }

    // Create an additional shadowJar with "shaded" classifier
    val shadedJar = tasks.register<ShadowJar>("shadedJar") {
        from(sourceSets.main.get().output)
        configurations = listOf(project.configurations.runtimeClasspath.get())
        archiveBaseName.set(project.name)
        archiveClassifier.set("shaded")
        shouldRunAfter(tasks.named("jar"))
    }

    // This is the base JAR with an "original" classifier, it's not shaded and should become the default JAR after making DBeaver use the shaded classifier
    tasks.named<Jar>("jar").configure {
        archiveClassifier.set("original")
    }

    tasks.named("assemble").configure {
        dependsOn(tasks.named("shadowJar"))
        dependsOn(shadedJar)
    }

    // Configure publishing to include the shaded JAR after evaluation
    afterEvaluate {
        plugins.withId("maven-publish") {
            extensions.configure<PublishingExtension> {
                publications {
                    named<MavenPublication>("mavenJava") {
                        artifact(shadedJar.get()) {
                            classifier = "shaded"
                        }
                    }
                }
            }
        }
    }
}

// Make the function available to build scripts
extensions.extraProperties["configureJarArtifacts"] = fun Project.() {
    configureJarArtifacts()
}
