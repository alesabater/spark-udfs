package deloitte.ie.bigdata.udfs.columns

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.{Failure, Success, Try}

object DateUDFs extends Logging {

  /**
    * Start UDF definitions
    */
  val udfLongToDate = udf[String, Long]((dt: Long) => getStringDate(dt))
  val udfLongToTimeDefault = udf[String, Long]((dt: Long) => getStringDate(dt, "HH:mm:ss"))
  val udfLongToDateDefault = udf[String, Long]((dt: Long) => getStringDate(dt, "dd.MM.yyyy"))
  val udfLongToDateTimeDefault = udf[String, Long]((dt: Long) => getStringDate(dt, "dd.MM.YYYY HH:mm:ss"))
  val udfLongToDateTimeFormat = udf[String, Long, String]((dt: Long, format: String) => getStringDate(dt, format))
  val udfLongToDateTimeFormatZone = udf[String, Long, String, String]((dt: Long, format: String, tz: String) => getStringDate(dt, format, tz))
  /**
    * End UDF definitions
    */


  private val BERLIN: DateTimeZone = DateTimeZone.forID("Europe/Berlin")
  private val FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss")


  /**
    * Converts a timestamp to a date in the desired string format
    *
    * @param date             Long representation of timestamp
    * @param outputDateFormat Desired date output format
    * @return
    */
  def getStringDate(date: Long, outputDateFormat: String = "dd.MM.yyyy HH:mm:ss", timeZone: String = "Europe/Berlin") = {
    val defaultFormat = "dd.MM.yyyy HH:mm:ss"
    val format = Try(DateTimeFormat.forPattern(outputDateFormat)) match {
      case Success(s) => s
      case Failure(f) => log.warn("Provided output format: " + outputDateFormat + " is incorrect, default to: dd.MM.yyyy HH:mm:ss");
        FORMAT
    }
    val tz = Try(DateTimeZone.forID(timeZone)) match {
      case Success(s) => s
      case Failure(f) => BERLIN
    }
    Try(new DateTime(date).withZone(tz)) match {
      case Success(s) => s.toString(format)
      case Failure(f) => (new DateTime(0L)).withZone(tz).toString(format)
    }
  }

}
