package br.com.bvs.data

import br.com.bvs.data.config.Settings
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class Ingestion(spark: SparkSession, settings: Settings) {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def execute(): Unit = {
    logger.info(s"Iniciando ingestão de dados...")
    logger.info(s"Bucket de entrada ${settings.gcsInputBucket}")

    val compBoss = readFile("comp_boss.csv")
    val billOfMaterials = transformBillOfMaterials(readFile("bill_of_materials.csv"))
    val priceQuote = readFile("price_quote.csv")

    writeToBigQuery(compBoss, s"${settings.bqDataset}.comp_boss")
    writeToBigQuery(billOfMaterials, s"${settings.bqDataset}.bill_of_materials")
    writeToBigQuery(priceQuote, s"${settings.bqDataset}.price_quote")

    logger.info("Ingestão de dados finalizada!")
  }

  private def readFile(fileName: String): DataFrame = {
    logger.info(s"Lendo arquivo $fileName...")
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${settings.gcsInputBucket}/$fileName")

    df
  }

  private def writeToBigQuery(df: DataFrame, tableName: String): Unit = {
    logger.info(s"Gravando tabela $tableName...")
    df
      .write
      .format("bigquery")
      .option("temporaryGcsBucket", settings.gcsTemporaryBucket)
      .mode(SaveMode.Overwrite)
      .save(tableName)
  }

  private def columnsToUnion(quantityOfColumns: Int): List[(String, String)] = {
    (1 to quantityOfColumns).map(index => (s"component_id_$index", s"quantity_$index")).toList
  }

  private def transformBillOfMaterials(df: DataFrame): DataFrame = {
    val tableName = s"${settings.bqDataset}.bill_of_materials"
    logger.info(s"Transformando tabela $tableName...")

    val quantityOfColumns = 8
    val colsToUnion = columnsToUnion(quantityOfColumns)

    val result = colsToUnion
      .map(names =>
    df.select(
      col("tube_assembly_id"),
      col(names._1).alias("component_id"),
      col(names._2).alias("quantity")
    ).where(
      col(names._1).isNotNull &&
      col(names._1).notEqual("NA") &&
      col(names._2).isNotNull &&
      col(names._2).notEqual("NA"))
    ).reduce(_ union _)

    result
  }
}

object Ingestion {
  def apply(spark: SparkSession, settings: Settings): Ingestion = new Ingestion(spark, settings)
}
