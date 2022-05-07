ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SparkFlightSql",
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % "7.0.0",
      "org.apache.arrow" % "arrow-memory-unsafe" % "7.0.0",
      "org.apache.arrow" % "arrow-flight" % "7.0.0",
      "org.apache.arrow" % "flight-core" % "7.0.0",
      "org.apache.arrow" % "flight-grpc" % "7.0.0",
      "org.apache.arrow" % "flight-sql" % "7.0.0",
      "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.2.1" % "provided",
      "org.scalatest" %% "scalatest-funsuite" % "3.2.12" % Test
    ),
    assembly / assemblyJarName := "spark-flight-sql.jar",
//    assembly / assemblyExcludedJars := {
//      val cp = (assembly / fullClasspath).value
//      cp filter( (d) => {
//          d.data.getName == "netty-transport-native-epoll-4.1.50.Final.jar" ||
//          d.data.getName == "netty-transport-native-unix-common-4.1.50.Final.jar" ||
//          d.data.getName == "javax.annotation-api-1.3.2.jar" ||
//          d.data.getName == "netty-handler-proxy-4.1.52.Final.jar" ||
//          d.data.getName == "netty-codec-http-4.1.52.Final.jar" ||
//          d.data.getName == "netty-codec-http2-4.1.52.Final.jar" ||
//          d.data.getName == "netty-codec-socks-4.1.52.Final.jar"
//      })
//    }
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.12.3"
      ,"com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3"
      ,"com.fasterxml.jackson.core" % "jackson-annotations" % "2.12.3"
    )
  )

//dependencyOverrides ++= Seq(
//  "com.thoughtworks.paranamer" % "paranamer" % "2.8" % Test
//  , "commons-codec" % "commons-codec" % "1.14" % Test
//  , "org.apache.commons" % "commons-lang3" % "3.12.0"
//  ,
//  , "io.netty" % "netty-buffer" % "4.1.68.Final"
//  , "org.apache.hadoop" % "hadoop-client-runtime" % "3.1.1"
//  , "org.apache.hadoop" % "hadoop-client-api" % "3.1.1"



ThisBuild / assemblyMergeStrategy := {
  case "reference.conf" => MergeStrategy.concat
//  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
//  case "META-INF/services/io.grpc.LoadBalancerProvider" => MergeStrategy.concat
  case "META-INF/javamail.charset.map" => MergeStrategy.last
  case "META-INF/javamail.default.address.map" => MergeStrategy.last
  case "META-INF/javamail.default.providers" => MergeStrategy.last

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
