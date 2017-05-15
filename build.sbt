scalaVersion := "2.11.8"

val SparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
