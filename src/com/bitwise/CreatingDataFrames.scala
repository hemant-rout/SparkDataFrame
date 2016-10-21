package com.bitwise

import org.apache.spark.sql.SparkSession

/**
  * Created by hemantr on 10/21/2016.
  */
object CreatingDataFrames {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val df = spark.read.json("input/json/world_bank.json")

    // Displays the content of the DataFrame to stdout
    df.show()
  }
}
