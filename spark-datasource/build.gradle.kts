plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
    id("shading")
}

description = "Spark Datasource for Salesforce Data Cloud JDBC"
val mavenName: String by extra("Spark Datasource for Salesforce Data Cloud JDBC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    implementation(project(":spark-datasource-core"))
    implementation(project(":jdbc"))
}

// Uses the common shading plugin defined in buildSrc module
val configureDataCloudShading = extensions.extraProperties["configureDataCloudShading"] as Project.(List<String>) -> Unit
val sparkAdditionalExclusions = extensions.extraProperties["SPARK_ADDITIONAL_EXCLUSIONS"] as List<String>
configureDataCloudShading(sparkAdditionalExclusions)

// Configure JAR artifacts (main, shaded, original)
val configureJarArtifacts = extensions.extraProperties["configureJarArtifacts"] as Project.() -> Unit
configureJarArtifacts()
