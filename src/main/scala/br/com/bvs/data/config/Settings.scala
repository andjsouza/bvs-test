package br.com.bvs.data.config

import org.apache.commons.cli.{BasicParser, CommandLine, Options}
import org.apache.spark.sql.SaveMode

class Settings(settings: CommandLine) {

  lazy val jobName: String = settings.getOptionValue(Attributes.appName, "bvs-test-job")
  lazy val gcsInputBucket: String = settings.getOptionValue(Attributes.gcsInputBucket)
  lazy val gcsTemporaryBucket: String = settings.getOptionValue(Attributes.gcsTemporaryBucket)
  lazy val bqDataset: String = settings.getOptionValue(Attributes.bqDataset)
  lazy val bqSaveMode: SaveMode = {
    settings.getOptionValue(Attributes.bqSaveMode, SaveMode.Append.name).toLowerCase match  {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
    }
  }
}

object Settings {

  def apply(args: Array[String]): Settings = {
    val options = new Options()
      .addOption("app", Attributes.appName, true, "Nome do job")
      .addOption("gib", Attributes.gcsInputBucket, true, "Bucket contendo os arquivos csv de entrada")
      .addOption("gtb", Attributes.gcsTemporaryBucket, true, "Bucket temporário")
      .addOption("bds", Attributes.bqDataset, true, "Dataset do BigQuery")
      .addOption("bsm", Attributes.bqSaveMode, true, "Save mode para as tabelas no BigQuery [Overwrite, Append]")

    val requiredOpts = Seq(
      Attributes.gcsInputBucket,
      Attributes.gcsTemporaryBucket,
      Attributes.bqDataset
    )

    validateLoadedParams(options, requiredOpts, args)
  }

  def validateLoadedParams(options: Options, mandatoryParams: Seq[String], args: Array[String]): Settings = {
    val parsed = new BasicParser().parse(options, args)
    mandatoryParams.foreach(mandatoryParam => {
      if (parsed.getOptionValue(mandatoryParam) == null) {
        throw new IllegalArgumentException(s"O argumento ${mandatoryParam} é mandatório")
      }
    })

    new Settings(parsed)
  }

}
