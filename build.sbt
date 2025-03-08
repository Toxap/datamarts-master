name := "DataMarts"
organization := "ru.beeline"
version := "1.2.1"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"
val circeVersion = "0.12.0-M3"

resolvers += "Nexus local" at "https://nexus-repo.dmp.vimpelcom.ru/repository/sbt_releases_/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.4.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "ru.beeline.dmp" %% "clientvictoriametrics" % "0.0.4",
  "org.rogach" %% "scallop" % "4.1.0",
  "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.1.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.12" % "test"
)

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
