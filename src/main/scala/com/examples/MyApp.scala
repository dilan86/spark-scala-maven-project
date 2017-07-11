package com.examples

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, LocalDate}
import org.json4s.JValue
import org.json4s.native.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.util.Try

object MyApp {
  val LOG = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Sample spark app").setMaster("local[*]"))
    var listRDD = Seq[RDD[String]]()
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    var currentDate = LocalDate.parse("2017-07-11", formatter)
    var apiDuration = 2

    implicit lazy val formats = org.json4s.DefaultFormats
    var dataSet = parseRDD(sc, getPath("/home/mylocation/spark-resources/ucs_mcs", currentDate.toString("yyyy-MM-dd")))
    dataSet.foreach(println)
    val jsonDataSet = convertToJSON(dataSet)

    try {
      jsonDataSet.map(m => (getFormattedDatetime((m \ "payload" \ "transactionDt").extract[String]), m))
    }catch {
      case t: Throwable => LOG.error(t.getMessage)
    }
  }

  def getFormattedDatetime(eventDateTime: String): DateTime = {
    var convertedDateTime: DateTime = null

    try {
      val parser = ISODateTimeFormat.dateTimeParser
      convertedDateTime = parser.parseDateTime(eventDateTime).withZone(DateTimeZone.forID("UTC"))

    } catch {
      case e: Exception => LOG.error("Error in date conversion")
    }
    convertedDateTime
  }

  def parseRDD(sparkContext: SparkContext, s3Path: String): RDD[String] = {
    val rdd = sparkContext.sequenceFile(s3Path, classOf[BytesWritable], classOf[BytesWritable])
      .map((hadoopFile: (BytesWritable, BytesWritable)) => {
        val bytes = hadoopFile._2.getBytes
        (hadoopFile._1.get(), new String(bytes.slice(0, hadoopFile._2.getLength)))
      }).map(_._2)

    def emptyStringRDD(): RDD[String] = {
      sparkContext.parallelize(Seq())
    }

    val validatedRDD = {
      val r = Try(rdd.first)
      if (!r.isFailure) {
        rdd
      } else {
        LOG.warn("Returning Empty RDD:  " + r.failed.get.getMessage)
        emptyStringRDD()
      }
    }
    validatedRDD
  }

  def getPath(basePath: String, dateString: String): String = {
    val s3Path = basePath + "/dt=" + dateString + "/*"
    LOG.info("S3 Path : " + s3Path)
    s3Path
  }

  def convertToJSON(rDD: RDD[String]): RDD[JValue] = {
    try {
      return rDD.map(line => toJSON(line)).filter(_.isSuccess).map(line => line.get)
    } catch {
      case e: Throwable => println(e.getStackTrace)
        return null
    }
  }

  def toJSON(line: String): Try[JValue] = {
    val r = Try(parse(line))
    if (r.isFailure) {
      LOG.error("Failed to parse: " + line)
    }
    r
  }

}
