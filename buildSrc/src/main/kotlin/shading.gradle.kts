import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.kotlin.dsl.*
import javax.inject.Inject

plugins {
    java
    id("com.gradleup.shadow")
}

/**
 * Gradle extension for JAR shading with JDBC/Spark presets.
 */
abstract class ShadingExtension @Inject constructor(
    private val project: Project
) {
    companion object {
        private val JDBC_ADDITIONAL_EXCLUSIONS = emptyList<String>()
        private val SPARK_ADDITIONAL_EXCLUSIONS = listOf(
            "org/apache/spark/**",
            "scala/**",
            "org/scala-lang/**"
        )
    }
    
    fun jdbc() = configureShading(JDBC_ADDITIONAL_EXCLUSIONS)
    fun spark() = configureShading(SPARK_ADDITIONAL_EXCLUSIONS)
    
    private fun configureShading(moduleExclusions: List<String>) {
        val relocations = getCommonRelocations()
        val exclusions = getCommonExclusions() + moduleExclusions
        
        configureShadowJarTasks(relocations, exclusions)
    }
    
    private fun getCommonRelocations() = listOf(
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
    
    private fun getCommonExclusions() = listOf(
        "META-INF/maven/**", "META-INF/DEPENDENCIES",
        "META-INF/LICENSE*", "META-INF/NOTICE*",
        "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.xml",
        "**/*.proto", "org.slf4j",
        "META-INF/services/com.fasterxml.*", "org.apache.calcite.avatica.remote.Driver",
        ".netbeans_automatic_build", "git.properties", "google-http-client.properties",
        "storage.v1.json", "pipes-fork-server-default-log4j2.xml",
        "dependencies.properties", "arrow-git.properties"
    )
    
    private fun configureShadowJarTasks(
        relocations: List<String>,
        exclusions: List<String>
    ) {
        project.tasks.withType<ShadowJar>().configureEach {
            isReproducibleFileOrder = true
            isPreserveFileTimestamps = false
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
            
            mergeServiceFiles {
                exclude("META-INF/services/java.sql.Driver")
            }
            
            exclusions.forEach { exclude(it) }
            
            relocations.forEach { pkg ->
                when (pkg) {
                    "org.apache.calcite" -> relocate(pkg, "com.salesforce.datacloud.shaded.$pkg") {
                        exclude("org.apache.calcite.avatica.remote.Driver")
                    }
                    else -> relocate(pkg, "com.salesforce.datacloud.shaded.$pkg")
                }
            }
        }
    }
}

extensions.create<ShadingExtension>("shading", project)

fun Project.configureJarArtifacts() {
    tasks.named<ShadowJar>("shadowJar").configure {
        archiveBaseName.set(project.name)
        archiveClassifier.set("shaded")
    }
    tasks.named("assemble").configure {
        dependsOn(tasks.named("shadowJar"))
    }
}

extensions.extraProperties["configureJarArtifacts"] = {
    configureJarArtifacts()
}
