import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "br.com.bvs",
      scalaVersion := "2.12.15"
    )),
    name := "bvs-test"
  )

libraryDependencies ++= Seq(
  library.spark,
  library.sparkSql,
  library.sparkYarn,
  library.scalatest % Test
)
