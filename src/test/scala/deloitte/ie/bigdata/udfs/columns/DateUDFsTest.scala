package deloitte.ie.bigdata.udfs.columns

import deloitte.ie.bigdata.commons.Contexts
import deloitte.ie.bigdata.udfs.ImplicitConversions._
import deloitte.ie.bigdata.udfs.TestSets
import deloitte.ie.bigdata.udfs.columns.DateUDFs._
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Inside, Matchers}



class DateUDFsTest extends FlatSpec with Matchers with Inside {

  case class Epoch(epochTime: Long)

  "longToTime" should "convert a Long Field to Time String in format HH:mm:ss" in {
    val df = Contexts.sqlCtx.createDataFrame(Seq(Epoch(31200000L)))
    val dfUpdated = df.withColumn("epochTime", udfLongToTimeDefault(df("epochTime")))
    dfUpdated.first.apply(0) should equal("09:40:00")
  }

  "longToDate" should "convert a Long Field to Time String in format YYYY.MM.dd" in {
    println("The test is running succesfully")
    val df = Contexts.sqlCtx.createDataFrame(Seq(Epoch(1451257200000L)))
    val dfUpdated = df.updateColumns(udfLongToDateDefault, "epochTime")
    dfUpdated.first.apply(0) should equal("28.12.2015")
  }

  "longToDateTime" should "convert a Long Field to Time String in format YYYY.MM.dd HH:mm:ss" in {
    val df = Contexts.sqlCtx.createDataFrame(Seq(Epoch(1466151140000L)))
    val dfUpdated = df.updateColumns(udfLongToDateTimeDefault, "epochTime")
    dfUpdated.first.apply(0) should equal("17.06.2016 10:12:20")
  }

  "Date UDF funtion" should "converts a timestamp to a date in the desired string format" in {
    val l = List(1441978500000L, 1439975400000L, 1423477500000L, 1467533100000L, -1L)

    val result = l.map(getStringDate(_, "MMM"))
    val result1 = l.map(getStringDate(_, "MM"))
    val result2 = l.map(getStringDate(_, "YYYY-MM-dd"))
    val result3 = l.map(getStringDate(_, "dd.MM.yyyy HH:mm:ss"))
    val result4 = l.map(getStringDate(_, "dd.MM.yyyy HH:mm:ss", "Europe/Dublin" ))
    val result5 = l.map(getStringDate(_, "dd.MM.yyyy HH:mm:ss", "ERROR TIME ZONE"))

    result shouldEqual List("Sep", "Aug", "Feb", "Jul", "Jan")
    result1 shouldEqual List("09", "08", "02", "07", "01")
    result2 shouldEqual List("2015-09-11", "2015-08-19", "2015-02-09", "2016-07-03", "1970-01-01")
    result3 shouldEqual List("11.09.2015 15:35:00", "19.08.2015 11:10:00", "09.02.2015 11:25:00", "03.07.2016 10:05:00", "01.01.1970 00:59:59")
    result4 shouldEqual List("11.09.2015 14:35:00", "19.08.2015 10:10:00", "09.02.2015 10:25:00", "03.07.2016 09:05:00", "01.01.1970 00:59:59")
    result5 shouldEqual List("11.09.2015 15:35:00", "19.08.2015 11:10:00", "09.02.2015 11:25:00", "03.07.2016 10:05:00", "01.01.1970 00:59:59")
  }

  it should "replace timestamps by desired date format" in {
    val df = TestSets.dfMonth
    val dfResult = df.withColumn("string", udfLongToDate(col("ts")))
    val dfResult1 = df.withColumn("string", udfLongToDateTimeFormat(col("ts"), lit("YYYY-MM-dd")))
    val dfResult2 = df.withColumn("string", udfLongToDateTimeFormatZone(col("ts"), lit("YYYY-MM-dd"), lit("Europe/Dublin")))

    dfResult.filter(col("string").rlike("[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}\\s[0-9]{2}:[0-9]{2}:[0-9]{2}")).count shouldEqual 6
    dfResult1.filter(col("string").rlike("[0-9]{4}-[0-9]{2}-[0-9]{2}")).count shouldEqual 6
    dfResult2.filter(col("string").rlike("[0-9]{4}-[0-9]{2}-[0-9]{2}")).count shouldEqual 6
  }
}
