rootProject.name = "substrait"

include("bom", "core", "isthmus", "hiveql")

pluginManagement {
  plugins {
    fun String.v() = extra["$this.version"].toString()
    fun PluginDependenciesSpec.idv(id: String, key: String = id) = id(id) version key.v()

    idv("com.google.protobuf")
    idv("org.jetbrains.gradle.plugin.idea-ext")
    kotlin("jvm") version "kotlin".v()
  }
  if (extra.has("enableMavenLocal") &&
      extra["enableMavenLocal"].toString().ifBlank { "true" }.toBoolean()
  ) {
    repositories {
      mavenLocal()
      gradlePluginPortal()
    }
  }
}
