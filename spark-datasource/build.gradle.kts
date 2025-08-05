plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}


description = "Spark Datasource for Salesforce Data Cloud JDBC"
val mavenName: String by extra("Spark Datasource for Salesforce Data Cloud JDBC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(project(":spark-datasource-core"))
    implementation(project(":jdbc"))
    implementation(project(":jdbc-grpc"))
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    implementation(libs.bundles.grpc.impl)
    implementation(libs.bundles.spark)
    
    // Override transitive Jackson Scala module from Spark with newer version
    implementation(libs.jackson.module.scala)

    testImplementation(platform(libs.junit.bom))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.scalatest)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.scalatestplus.junit5)
}

tasks.named("compileScala") {
    dependsOn(":spark-datasource-core:compileScala")
    dependsOn(":jdbc-core:compileJava")
}