ThisBuild / organization := "com.sparklogger"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

resolvers += Resolver.mavenLocal

lazy val root = (project in file("."))
  .aggregate(core, multiMachine)
  .settings(
    name := "spark-logger-root",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    name := "spark-logger",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  )

lazy val multiMachine = (project in file("multi_machine_parallel_processing"))
  .settings(
    name := "multi-machine-parallel-processing",
    Compile / mainClass := Some("Master"),
    fork := true,
    connectInput := true
  )
