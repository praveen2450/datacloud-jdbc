plugins {
    alias(libs.plugins.shadow)
    alias(libs.plugins.lombok)
    id("java-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud JDBC driver"
val mavenName: String by extra("Salesforce Data Cloud JDBC Driver")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    implementation(project(":jdbc-http"))
    
    // Use compileOnly for compilation + runtimeOnly for shading (not exposed to consumers)
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.bundles.grpc.impl)
    runtimeOnly(project(":jdbc-grpc"))
    runtimeOnly(libs.bundles.grpc.impl)
    
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.bundles.mocking)
    testImplementation(libs.guava)                      // Guava collections in tests
}

// Common shading configuration to be reused
fun com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar.configureShading() {
    val shadeBase = "com.salesforce.datacloud.shaded"
    
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    relocate("com.google", "$shadeBase.com.google")
    relocate("io.grpc", "$shadeBase.io.grpc")
    relocate("com.fasterxml.jackson", "$shadeBase.com.fasterxml.jackson")
    relocate("dev.failsafe", "$shadeBase.dev.failsafe")
    relocate("io.jsonwebtoken", "$shadeBase.io.jsonwebtoken")
    relocate("io.netty", "$shadeBase.io.netty")
    relocate("kotlin", "$shadeBase.kotlin")
    relocate("okhttp3", "$shadeBase.okhttp3")
    relocate("okio", "$shadeBase.okio")
    relocate("org.apache.arrow", "$shadeBase.org.apache.arrow")
    relocate("org.apache.calcite", "$shadeBase.org.apache.calcite") {
        exclude("org.apache.calcite.avatica.remote.Driver")
    }
    relocate("org.apache.commons", "$shadeBase.org.apache.commons")
    relocate("org.apache.hc", "$shadeBase.org.apache.hc")

    mergeServiceFiles {
        exclude("META-INF/services/java.sql.Driver")
    }
    
    exclude("org.slf4j")

    exclude("org.apache.calcite.avatica.remote.Driver")
    exclude("META-INF/LICENSE*")
    exclude("META-INF/NOTICE*")
    exclude("META-INF/DEPENDENCIES")
    exclude("META-INF/maven/**")
    exclude("META-INF/services/com.fasterxml.*")
    exclude("META-INF/*.xml")
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude(".netbeans_automatic_build")
    exclude("git.properties")
    exclude("google-http-client.properties")
    exclude("storage.v1.json")
    exclude("pipes-fork-server-default-log4j2.xml")
    exclude("dependencies.properties")
    exclude("**/*.proto")
    exclude("arrow-git.properties")
}

// Default JAR with no classifier but is shaded to be used for DBeaver until we can update driver definition
tasks.shadowJar {
    archiveBaseName = "jdbc"
    archiveClassifier = ""
    configureShading()
    shouldRunAfter(tasks.jar)
}

// Create an additional shadowJar with "shaded" classifier
val shadedJar = tasks.register<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadedJar") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    archiveBaseName = "jdbc"
    archiveClassifier = "shaded"
    configureShading()
    shouldRunAfter(tasks.jar)
}

// This is the base JAR with an "original" classifier, it's not shaded and should become the default JAR after making DBeaver use the shaded classifier
tasks.jar {
    archiveClassifier = "original"
}

tasks.named("compileJava") {
    dependsOn(":jdbc-core:build")
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
