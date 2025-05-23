name := "parse_cdr"
organization := "altan_redes"
version := "0.1"
scalaVersion := "2.11.1"

autoScalaLibrary := false
val sparkVersion = "2.4.7"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}