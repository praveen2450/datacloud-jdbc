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

// Configure shading using DSL
shading {
    spark()
}

// Configure JAR artifacts
val configureJarArtifacts = extensions.extraProperties["configureJarArtifacts"] as () -> Unit
configureJarArtifacts()
