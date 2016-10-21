package com.bitwise

import org.apache.spark.sql.SparkSession

/**
  * Created by hemantr on 10/21/2016.
  */
object CreatingDataSet {

  case class Person(name: String, age: Long)
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    /*val caseClassDS = List(Person("Andy", 32),Person("More", 32)).toDS()
    caseClassDS.show()*/


    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(println)

    val path = "input/json/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }
}
