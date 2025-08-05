plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Spark DataSource"
val mavenName: String by extra("Salesforce Data Cloud Spark DataSource")
val mavenDescription: String by extra("${project.description}")

dependencies {
    // ALIGNED WITH JDBC PATTERN: Core + Shaded dependencies
    implementation(project(":spark-datasource-core"))
    implementation(project(":jdbc"))                    // Shaded JDBC (like jdbc depends on jdbc-core)
    implementation(project(":jdbc-core"))               // Need for compilation (shaded jdbc only provides runtime)
    
    // Spark dependencies (user provides these)
    implementation(libs.bundles.spark)
    
    // Force compatible Jackson Scala module version to resolve version conflict
    implementation(libs.jackson.module.scala)

    // Test dependencies need actual implementations
    testImplementation(project(":spark-datasource-core"))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(project(":jdbc-util"))            // For DataCloudJDBCException
    testImplementation(libs.bundles.grpc.impl)           // For tests only
    testImplementation(libs.bundles.scala.testing)
}

tasks.named("compileScala") {
    dependsOn(":spark-datasource-core:compileScala")
    dependsOn(":jdbc-core:compileJava")
}