package cn.jpush.spark.etl

import com.alibaba.fastjson.{JSON, JSONException}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RegisterUserLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RegisterUserLog2")
      .master("yarn")
      .getOrCreate()

    // create an Origin RDD
    val regRDD = spark.sparkContext.textFile(args(0))
    // the schema is encode in a string
    val schemaString = "uid appkey platform"
    // generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName,StringType,nullable = true))
    val schema = StructType(fields)

    println(regRDD.count())
    // convert records of the origin RDD (regRDD) to Rows
    val rowRDD  = regRDD.map(line => {
        val json = JSON.parseObject(line)
        val uid = json.getString("uid")
        val appkey = json.getString("appkey")
        val platformId = json.getInteger("platform")
        var platform: String = null
        if (platformId == 0) {
          platform = "a"
        } else if (platformId == 1) {
          platform = "i"
        } else if (platformId == 2) {
          platform = "w"
        }
        uid+"\t"+appkey+"\t"+platform
    }).map(_.split("\t")).map(attributes => Row(attributes(0),attributes(1),attributes(2)))

    // apply the Schema to the row RDD
    val regDF = spark.createDataFrame(rowRDD,schema)

    println(regDF.printSchema())

    regDF.select("uid","appkey","platform").write.format("parquet").save(args(1))

    // create a  temporary veiw using the DataFrame
    regDF.createTempView("register_user_log")

    // SQL can be run over a temporary view

    spark.sql("select uid,appkey from register_user_log limit 10").show(6)
  }
}
