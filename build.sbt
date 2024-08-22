scalaVersion := "2.13.10"

version := "1.0"
name := "dataframe"
organization := "com.hackerrank"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.0",
    "org.apache.spark" %% "spark-sql" % "3.4.0",
    "org.apache.hadoop" % "hadoop-client" % "3.3.4",
    "org.apache.hadoop" % "hadoop-client-api" % "3.3.4",
    "org.apache.hadoop" % "hadoop-common" % "3.3.4",
    "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)

testOptions += Tests.Argument(TestFrameworks.Specs2, "junitxml")

fork := true
Global / javaOptions ++= Seq(
    "base/java.lang",
    "base/java.lang.invoke",
    "base/java.lang.reflect",
    "base/java.io",
    "base/java.net",
    "base/java.nio",
    "base/java.util",
    "base/java.util.concurrent",
    "base/java.util.concurrent.atomic",
    "base/sun.nio.ch",
    "base/sun.nio.cs",
    "base/sun.security.action",
    "base/sun.util.calendar",
    "security.jgss/sun.security.krb5",
).map("--add-exports=java." + _ + "=ALL-UNNAMED")
