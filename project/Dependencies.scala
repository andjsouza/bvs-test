import sbt._

object Dependencies {

  val library = new {

    object Version {
      val spark = "2.4.4"
      val scalatest = "3.1.0"
    }

    val spark = "org.apache.spark" %% "spark-core" % Version.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val sparkYarn = "org.apache.spark" %% "spark-yarn" % Version.spark
    val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  }

}
