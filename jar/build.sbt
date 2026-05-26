// data-migration-module/jar — multi-cloud fork of IcebergMigrate.
//
// Source of truth for the JAR that runs inside the SparkApplication CR
// produced by `data migration/template_builder.py`. The original lives in
// sds-platform-apps/utility-jobs and is Azure-only at the orchestration
// layer; this fork keeps the same package + main class so the SparkApp CR
// only changes the JAR coordinate, not its mainClass.
//
// sds-pe-core is pulled via the NIKHIL-523 SSH host alias (see
// ~/.ssh/config). Verify with `ssh -T git@github.com-nikhil523`.

ThisBuild / scalaVersion := "2.13.9"
ThisBuild / version      := sys.env.getOrElse("buildVersion", "0.1.0-SNAPSHOT")
ThisBuild / useCoursier  := true

val sparkVersion      = System.getProperty("sparkVersion", "3.5.5")
val sparkMajorVersion = sparkVersion.replaceAll("\\.\\d++$", "")

lazy val coreRepo = ProjectRef(
  uri("ssh://git@github.com-nikhil523/prevalent-ai/sds-pe-core.git#release-4.1.2"),
  "sds-pe-core",
)

lazy val root = (project in file("."))
  .settings(
    name := "data-migration-iceberg",

    assembly / assemblyJarName :=
      s"${name.value}-${sparkMajorVersion}_${scalaBinaryVersion.value}-${version.value}.jar",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "com.typesafe"      % "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test,
    ),

    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "com.fasterxml.jackson.core"    % "jackson-databind"     % "2.15.2",
    ),

    // Same fat-jar merge strategy pattern used by utility-jobs.
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "reference.conf"                         => MergeStrategy.concat
      case "application.conf"                       => MergeStrategy.concat
      case _                                        => MergeStrategy.first
    },

    Test / fork              := true,
    Test / parallelExecution := false,
  )
  .dependsOn(coreRepo)
