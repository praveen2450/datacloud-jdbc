plugins {
    alias(libs.plugins.lombok)
    id("java-conventions")
    id("publishing-conventions")
    id("shading")
}

description = "Salesforce Data Cloud JDBC driver"
val mavenName: String by extra("Salesforce Data Cloud JDBC Driver")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    implementation(project(":jdbc-http"))
    implementation(project(":jdbc-grpc"))

    implementation(libs.bundles.grpc.impl)
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
}

tasks.named("compileJava") {
    dependsOn(":jdbc-core:build")
}

// Configure shading using DSL
shading {
    jdbc()
}

// Configure JAR artifacts
val configureJarArtifacts = extensions.extraProperties["configureJarArtifacts"] as () -> Unit
configureJarArtifacts()
