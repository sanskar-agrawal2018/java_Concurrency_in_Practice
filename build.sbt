ThisBuild / organization := "com.sparklogger"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

resolvers += Resolver.mavenLocal

lazy val root = (project in file("."))
  .aggregate(core, multiMachine, chapter2, chapter3, chapter4, chapter5, chapter6, chapter7, jvmConfiguration)
  .settings(
    name := "parallel-processing-examples",
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

lazy val chapter2 = (project in file("chapter_2"))
  .settings(
    name := "chapter-2",
    fork := true,
    connectInput := true
  )

lazy val chapter3 = (project in file("chapter_3"))
  .dependsOn(chapter2)
  .settings(
    name := "chapter-3",
    fork := true,
    connectInput := true
  )


lazy val chapter4 = (project in file("chapter_4"))
  .settings(
    name := "chapter-4",
    fork := true,
    connectInput := true,
    // JNA: lets us call the C library's sched_getcpu() to read the current core.
    libraryDependencies ++= Seq(
      "net.java.dev.jna" % "jna"       % "5.14.0",
      "org.scalatest"   %% "scalatest" % "3.2.15" % Test
    )
  )


lazy val chapter5 = (project in file("chapter_5"))
  .settings(
    name := "chapter-5",
    fork := true,
    connectInput := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  )

lazy val chapter6 = (project in file("chapter_6"))
  .settings(
    name := "chapter-6",
    Compile / mainClass := Some("ExecutorDemo"),
    fork := true,
    connectInput := true
  )

lazy val chapter7 = (project in file("chapter_7"))
  .settings(
    name := "chapter-7",
    Compile / mainClass := Some("ThreadPoolInterruptionDemo"),
    fork := true,
    connectInput := true
  )


lazy val scala_Learning = (project in file("scala_learning"))
  .settings(
    name := "scala-learning",
    fork := true,
    connectInput := true
  )

lazy val jvmConfiguration = (project in file("jvm_configuration"))
  .settings(
    name := "jvm-configuration",
    fork := true,
    connectInput := true,
    // Default JVM tuning for the demo. Override on the command line, e.g.
    //   sbt -J-Xmx256m -J-Xss512k "jvmConfiguration/run"
    // or set SBT_OPTS / JAVA_TOOL_OPTIONS in the shell.
    javaOptions ++= Seq(
      "-Xms64m",            // initial heap
      "-Xmx256m",           // max heap
      "-Xss512k",           // per-thread stack
      "-XX:MetaspaceSize=32m",
      "-XX:MaxMetaspaceSize=128m",
      "-XX:MaxDirectMemorySize=64m",
      "-XX:ReservedCodeCacheSize=48m",
      "-XX:+PrintFlagsFinal",
      "-XX:+UnlockDiagnosticVMOptions"
    )
  )
