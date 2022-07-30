ThisBuild / scalaVersion := "2.12.15"

val arrowVersion = "8.0.0"
val sparkVersion = "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "SparkFlightSql",
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-unsafe" % arrowVersion,
      "org.apache.arrow" % "arrow-flight" % arrowVersion,
      "org.apache.arrow" % "flight-core" % arrowVersion,
      "org.apache.arrow" % "flight-grpc" % arrowVersion,
      "org.apache.arrow" % "flight-sql" % arrowVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,// % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion,// % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion,// % "provided",
      "org.apache.directory.api" % "api-ldap-client-api" % "2.1.0",
      "org.scalatest" %% "scalatest-funsuite" % "3.2.12" % Test,
      "org.apache.curator" % "curator-test" % "5.3.0" % Test,
      "org.apache.directory.server" % "apacheds-core-annotations" % "2.0.0.AM26" % Test,
      "org.apache.directory.server" % "apacheds-server-annotations" % "2.0.0.AM26" % Test,
      "org.apache.directory.server" % "apacheds-core" % "2.0.0.AM26" % Test,
      "org.apache.directory.server" % "apacheds-core-api" % "2.0.0.AM26" % Test,
      "org.apache.directory.api" % "api-ldap-codec-standalone" % "2.1.0" % Test
//      "org.apache.ranger" % "ranger-hive-plugin" % "2.3.0"  % Test

    ),
    assembly / assemblyJarName := "spark-flight-sql.jar"
  )

ThisBuild / parallelExecution := false

ThisBuild / assemblyMergeStrategy := {
  case "reference.conf" => MergeStrategy.concat
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "mail", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("javax", "xml", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("org", "objenesis", xs@_*) => MergeStrategy.last
  case PathList("codegen", "config.fmpp") => MergeStrategy.last
  case PathList("net", "jpountz", xs@_*) => MergeStrategy.last
  case PathList("org", "w3c", xs@_*) => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "application.prod.conf" => MergeStrategy.discard
  case "plugin.xml" => MergeStrategy.last
  case "parquet.thrift" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
