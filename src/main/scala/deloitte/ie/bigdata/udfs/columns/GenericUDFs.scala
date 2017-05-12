package deloitte.ie.bigdata.udfs.columns

import org.apache.spark.Logging
import org.apache.spark.sql.functions._

object GenericUDFs extends Logging {

  /**
    *  Start UDF definitions
    */
  val udfToInt = udf[Int, String](toInt(_))
  val udfRoundAt = udf[Double, Int, Double]((p: Int, n: Double) => roundAt(p, n))
  val udfToDouble = udf[Double, String](toDouble(_))
  val udfReplaceEmpty = udf[String, String, String]((col: String, value: String) => replaceEmpty(col, value))
  val udfReplaceWithRegex = udf[String, String, String]((col: String, value: String) => replaceEmpty(col, value))
  /**
    *  End UDF definitions
    */


  /**
    * Replaces an empty String by another String, if not returns initial value
    *
    * @param columnValue Initial value
    * @param repValue    desired replacement
    * @return String
    */
  def replaceEmpty(columnValue: String, repValue: String): String = {
    val pattern = "^\\s*$".r
    if (columnValue == null) repValue
    else pattern.findFirstMatchIn(columnValue) match {
      case Some(m) => repValue
      case _ => columnValue
    }
  }

  /**
    * Casts a string number into a Int datatype. Replaces commas if existing by empty spaces.
    * Does Regex over the string to ensure int format. If any invalid value is detected then a -1 will be returned
    *
    * @param str String number to be casted
    * @return Int
    */
  def toInt(str: String) = {
    if (str == null) -1
    else {
      val strNoCommas = str.replaceAll("[.]", "")
      val numbersRegex = "[0-9]+".r
      numbersRegex.findFirstIn(strNoCommas) match {
        case Some(s) => s.toInt
        case None => -1
      }
    }
  }

  /**
    * Casts a string number into a double datatype. Replaces commas if existing by points.
    * Does Regex over the string to ensure double format. If any invalid value is detected then a -1 will be returned
    *
    * @param str string number to be casted
    * @return Double
    */
  def toDouble(str: String) = {
    if (str == null) -1.0
    else {
      val strNoCommas = str.replaceAll("[,]", ".")
      val numbersRegex = "[0-9]+[\\.]?[0-9]*".r
      numbersRegex.findFirstIn(strNoCommas) match {
        case Some(s) => s.toDouble
        case None => -1.0
      }
    }
  }

  /**
    * Method to round numbers to desired number of decimals
    *
    * @param p number of decimals
    * @param n number
    * @return Double
    */
  def roundAt(p: Int, n: Double): Double = {
    val s = math pow(10, p)
    (math round n * s) / s
  }


}
