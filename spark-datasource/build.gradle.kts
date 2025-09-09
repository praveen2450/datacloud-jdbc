plugins {
    alias(libs.plugins.shadow)
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

// Apply shared shading utilities
apply(from = "${rootProject.projectDir}/gradle/shading.gradle")

description = "Spark Datasource for Salesforce Data Cloud JDBC"
val mavenName: String by extra("Spark Datasource for Salesforce Data Cloud JDBC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    // Core spark datasource implementation
    implementation(project(":spark-datasource-core"))
    
    // Full JDBC driver with all build/config dependencies (includes gRPC implementations)
    implementation(project(":jdbc"))  // This provides DataCloudJDBCDriver with full gRPC runtime
    
    // Spark dependencies: compileOnly because user provides Spark at runtime
    compileOnly(libs.bundles.spark)  // Compile against Spark APIs only, don't include in JAR
    
    // Override transitive Jackson Scala module from Spark with newer version
    implementation(libs.jackson.module.scala)

    // Test dependencies: Need Spark for test compilation since main uses compileOnly
    testImplementation(libs.bundles.spark)  // compileOnly dependencies aren't available at test time
    
    testImplementation(platform(libs.junit.bom))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.scalatest)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.scalatestplus.junit5)
}

// Spark shading configuration using shared script - NO DUPLICATION!
fun com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar.configureSparkShading() {
    (project.ext["applySparkShading"] as groovy.lang.Closure<*>).call(this)
}

// Default JAR with no classifier but is shaded 
tasks.shadowJar {
    // Force include all runtime dependencies (especially project dependencies)
    from(project.configurations.runtimeClasspath.get().map { 
        if (it.isDirectory()) it else zipTree(it) 
    })
    archiveBaseName = "spark-datasource"
    archiveClassifier = ""
    configureSparkShading()
    shouldRunAfter(tasks.jar)
}

// Create an additional shadowJar with "shaded" classifier
val shadedJar = tasks.register<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadedJar") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    archiveBaseName = "spark-datasource"
    archiveClassifier = "shaded"
    configureSparkShading()
    shouldRunAfter(tasks.jar)
}

// This is the base JAR with an "original" classifier, it's not shaded 
tasks.jar {
    archiveClassifier = "original"
}

tasks.named("compileScala") {
    dependsOn(":spark-datasource-core:compileScala")
    dependsOn(":jdbc-core:compileJava")
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
    dependsOn(shadedJar)
}

// Configure publishing to include the shaded JAR after evaluation
afterEvaluate {
   publishing {
       publications {
           named<MavenPublication>("mavenJava") {
               artifact(shadedJar.get()) {
                   classifier = "shaded"
               }
               
               // For shaded JAR, remove all dependencies from POM since everything is included
               pom.withXml {
                   val dependenciesNode = asNode().get("dependencies")
                   if (dependenciesNode is groovy.util.NodeList && dependenciesNode.isNotEmpty()) {
                       asNode().remove(dependenciesNode[0] as groovy.util.Node)
                   }
               }
           }
       }
   }
}