package deloitte.ie.bigdata.udfs.columns

import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}
import deloitte.ie.bigdata.udfs.columns.GenericUDFs._
import deloitte.ie.bigdata.udfs.TestSets

class GenericUDFsTest extends FlatSpec with Matchers {

  "util functions" should "replace blank values by desired value" in {
    val l = List(" ", "a ", null, "")
    val result = l.map(replaceEmpty(_, "value"))

    result shouldEqual List("value", "a ", "value", "value")
  }

  it should "cast string int value to int datatype" in {
    val l = List("1", "2.000", null, "a  ", "?", "2,0", "2.000,00", "2,000.00")
    val result = l.map(toInt)

    result shouldEqual List(1, 2000, -1, -1, -1, 2, 2000, 2)
  }

  it should "cast string to double" in {
    val l = List("1", "1.0", "2,0", null, "a  ", "?", "2,345")
    val result = l.map(toDouble)

    result shouldEqual List(1.0, 1.0, 2.0, -1.0, -1.0, -1.0, 2.345)
  }

  it should "test decimal rounding" in {
    val decimals1 = List(0.16, 0.14, 0.35, 0.247, 1.0004).map(roundAt(1, _))
    val decimals2 = List(0.116, 0.124, 0.335, 0.235).map(roundAt(2, _))
    val decimals3 = List(0.1114, 0.1436, 0.3336, 0.2245).map(roundAt(3, _))
    val decimals4 = List(0.111112, 0.1, 0.3334, 0.2246).map(roundAt(0, _))
    val decimals5 = List(1.743, 0.234, 0.5, 0.6).map(roundAt(0, _))

    decimals1 shouldEqual List(0.2, 0.1, 0.4, 0.2, 1.0)
    decimals2 shouldEqual List(0.12, 0.12, 0.34, 0.24)
    decimals3 shouldEqual List(0.111, 0.144, 0.334, 0.225)
    decimals4 shouldEqual List(0.0, 0.0, 0.0, 0.0)
    decimals5 shouldEqual List(2.0, 0.0, 1.0, 1.0)
  }

  "Column UDFs" should "replace blank values over a Column" in {
    val df = TestSets.dfEmptyStrings
    val dfResult = df.withColumn("no_blanks", udfReplaceEmpty(col("blanks"), lit("none"))).select("no_blanks").filter(col("no_blanks") !== "none")

    dfResult.count shouldEqual 3
  }

  it should "round Doubles over a column to de desired number of decimals" in {
    val df = TestSets.dfCategories
    val dfDivided = df.withColumn("rounded", udfRoundAt(lit(1), col("string1")))
    val dfDivided1 = df.withColumn("rounded", udfRoundAt(lit(3), col("string2")))
    val dfDivided2 = df.withColumn("rounded", udfRoundAt(lit(5), col("string3")))

    dfDivided.filter(col("rounded") rlike "[0-9]\\.[0-9]{1}")
    dfDivided1.filter(col("rounded") rlike "[0-9]\\.[0-9]{3}")
    dfDivided2.filter(col("rounded") rlike "[0-9]\\.[0-9]{5}")
  }

  it should "turn string integers into Ints" in {
    val df = TestSets.dfFullSchema
    val dfResult = df.withColumn("toInt", udfToInt(col("StringToInt")))

    dfResult.filter(col("toInt") === -1).count shouldEqual 2
    dfResult.filter(col("toInt") rlike  "^[0-9]+").count shouldEqual 7

  }
}
