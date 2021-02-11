package br.com.bvs.data

import br.com.bvs.data.config.Settings
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SettingsTest extends AnyFlatSpec with Matchers {

  it should "return job name" in {
    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).jobName shouldBe "bvs-test-job"

    Settings(
      Array(
        "--app", "bvs-test-job-2",
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).jobName shouldBe "bvs-test-job-2"
  }

  it should "return GCS input bucket" in {
    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).gcsInputBucket shouldBe "gs://bvs-test-bucket/input"
  }

  it should "throw IllegalArgumentException when GCS input bucket is null" in {
    assertThrows[IllegalArgumentException] {
      Settings(
        Array(
          "--gtb", "bvs-test-tmp",
          "--bds", "bvsdataset"
        )
      ).gcsInputBucket
    }
  }

  it should "return GCS temporary bucket" in {
    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).gcsTemporaryBucket shouldBe "bvs-test-tmp"
  }

  it should "throw IllegalArgumentException when GCS temporary bucket is null" in {
    assertThrows[IllegalArgumentException] {
      Settings(
        Array(
          "--gib", "gs://bvs-test-bucket/input",
          "--bds", "bvsdataset"
        )
      ).gcsTemporaryBucket
    }
  }

  it should "return BigQuery dataset" in {
    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).bqDataset shouldBe "bvsdataset"
  }

  it should "throw IllegalArgumentException when BigQuery dataset is null" in {
    assertThrows[IllegalArgumentException] {
      Settings(
        Array(
          "--gib", "gs://bvs-test-bucket/input",
          "--gtb", "bvs-test-tmp"
        )
      ).bqDataset
    }
  }

  it should "return BigQuery save mode" in {
    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset"
      )
    ).bqSaveMode shouldBe SaveMode.Append

    Settings(
      Array(
        "--gib", "gs://bvs-test-bucket/input",
        "--gtb", "bvs-test-tmp",
        "--bds", "bvsdataset",
        "--bsm", "Overwrite"
      )
    ).bqSaveMode shouldBe SaveMode.Overwrite
  }

}
