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
    implementation("org.apache.spark:spark-sql_2.13:3.5.5")
    implementation("org.apache.spark:spark-core_2.13:3.5.5")

    // Test dependencies need actual implementations
    testImplementation(project(":spark-datasource-core"))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(project(":jdbc-util"))            // For DataCloudJDBCException
    testImplementation(libs.bundles.grpc.impl)           // For tests only
    testImplementation("org.scalatest:scalatest_3:3.2.19")
    testRuntimeOnly("org.junit.platform:junit-platform-engine:1.12.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.12.0")
    testRuntimeOnly("org.scalatestplus:junit-5-12_3:3.2.19.0")
}

tasks {
    test{
        useJUnitPlatform {
            includeEngines("scalatest")
            testLogging {
                events("passed", "skipped", "failed")
            }
        }
    }
}

tasks.named("compileScala") {
    dependsOn(":spark-datasource-core:compileScala")
    dependsOn(":jdbc-core:compileJava")
}