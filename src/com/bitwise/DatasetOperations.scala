package com.bitwise

import org.apache.spark
import org.apache.spark.sql.SparkSession

/**
  * Created by hemantr on 10/21/2016.
  */
object DatasetOperations {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("input/json/world_bank.json")


   /* df.printSchema()*/
    /*df.show()*/
    /*df.select("approvalfy").show()*/
    /*df.select($"approvalfy",$"grantamt" + 5).show()*/
    /*df.select($"grantamt" >100).show()*/
    /*df.filter($"grantamt" >100).show()*/
    /*df.groupBy("approvalfy").max("grantamt").show()*/

   /* df.filter($"grantamt" >100).select("approvalfy","grantamt").show()*/

    df.createOrReplaceTempView("world_bank")

    val sqlDF = spark.sql("SELECT * FROM world_bank w where w._id!=null")
    sqlDF.show()

  }
}
