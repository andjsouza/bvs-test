package br.com.bvs.data

import br.com.bvs.data.config.Settings
import org.apache.spark.sql.SparkSession

object IngestionMain {

  def main(args: Array[String]): Unit = {
    val settings = Settings(args)
    val spark = SparkSession
      .builder()
      .appName(settings.jobName)
      .getOrCreate()

    Ingestion(spark, settings).execute()
   }
}
