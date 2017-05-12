package deloitte.ie.bigdata.udfs.dataframes

import com.typesafe.config.Config
import deloitte.ie.bigdata.commons.LoadedProperties
import deloitte.ie.bigdata.udfs.TestSets
import org.scalatest.{FlatSpec, Matchers}
import deloitte.ie.bigdata.udfs.ImplicitConversions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class GenericUDFsTest extends FlatSpec with Matchers {

  "dropColumns" should "return a dataframe without the specified columns" in {

    val conf = LoadedProperties.conf
    val c: String = conf.getString("application.id")
    //val d: String = conf.getString("spark.application.master")
    //println(c)
    val df = TestSets.dfMixed
    val df1 = df.dropColumns("int1","string1","double1","long1","boolean1")
    val df2 = df.dropColumns("int1")
    val df3 = df.dropColumns("int1","non-existent-columns")
    val df4 = df.dropColumns("non-existent-columns")

    df1.columns shouldEqual Array("string","int","double","boolean","long")
    df1.columns.length shouldEqual 5
    df2.columns.length shouldEqual 9
    df3.columns shouldEqual Array("string","int","double","boolean","long","string1","double1","boolean1","long1")
    df3.columns.length shouldEqual 9
    df4.columns.length shouldEqual 10
    df4.columns shouldEqual Array("string","int","double","boolean","long","string1","int1","double1","boolean1","long1")
  }

  "renameColumns" should "return a dataframe with columns renamed" in {
    val df = TestSets.dfRename
    val df1 = df.renameColumns(Map("three" -> "newThree"))
    val df2 = df.renameColumns(Map("one.two" -> "one","three" -> "two", "four" -> "three", "five" -> "four", "six" -> "five"))
    val df3 = df.renameColumns(Map("bunch" -> "of","non" -> "existent", "columns" -> "here"))

    df1.columns shouldEqual Array("`one.two`", "newThree", "four", "five", "six")
    df2.columns shouldEqual Array("one", "two", "three", "four", "five")
    df3.columns shouldEqual Array("`one.two`", "three", "four", "five", "six")
  }

  "concatColumns" should "concatenate column values using a delimeter" in {
    val df = TestSets.dfMixed

    val df1 = df.concatStringColumns("newCol", ";", "string", "string1")
    val df2 = df.concatStringColumns("newCol", ";", "int", "int1")
    val df3 = df.concatStringColumns("newCol", ";", "double", "double1")
    val df4 = df.concatStringColumns("newCol", ";", "long", "long1")
    val df5 = df.concatStringColumns("newCol", ";", "boolean", "boolean1")

    df1.columns.contains("newCol") shouldEqual true
    df2.columns.contains("newCol") shouldEqual true
    df3.columns.contains("newCol") shouldEqual true
    df4.columns.contains("newCol") shouldEqual true
    df5.columns.contains("newCol") shouldEqual true
  }

  "updateColumns" should "update a list of Columns applying the same function to all of them" in {
    val df = TestSets.dfMixed
    val addUDF = udf[Int, Int]((col: Int) => col * 5)
    val boolUDF = udf[String, Boolean]((col: Boolean) => col.toString)
    val strUDF = udf[String, String]((col: String) => col + "_changed")

    val df1 = df.updateColumns(addUDF, "int")
    val df2 = df.updateColumns(boolUDF, "boolean")
    val df3 = df.updateColumns(strUDF, "string")

    df1.select("int").collect().map(r => (r.getInt(0) % 5) == 0).reduce((a,b) => a && b) shouldEqual true
    df2.select("boolean").collect().map(r => r.getString(0).isInstanceOf[String]).reduce((a,b) => a && b) shouldEqual true
    df3.select("string").collect().map(r => r.getString(0).matches("^.*_changed")).reduce((a,b) => a && b) shouldEqual true
  }

  "applySchema" should "apply a schema to a DataFrame" in {
    val df = TestSets.noSchemaDf
    val schema = StructType(Seq(
      StructField("stringToInteger", IntegerType, true),
      StructField("stringToString", StringType, true),
      StructField("stringToBoolean", BooleanType, true),
      StructField("stringToDouble", DoubleType, true),
      StructField("integerToString", StringType, false),
      StructField("stringToString1", StringType, true),
      StructField("booleanToString", StringType, false),
      StructField("doubleToString", StringType, false)
    ))
    val schemaSkewed = StructType(Seq(
      StructField("stringToInteger", IntegerType, true),
      StructField("stringToString", StringType, true),
      StructField("stringToBoolean", BooleanType, true),
      StructField("stringToDouble", DoubleType, true),
      StructField("integerToString", StringType, false),
      StructField("stringToString1", StringType, true)
    ))
    val schemaOverflow = StructType(Seq(
      StructField("stringToInteger", IntegerType, true),
      StructField("stringToString", StringType, true),
      StructField("stringToBoolean", BooleanType, true),
      StructField("stringToDouble", DoubleType, true),
      StructField("integerToString", StringType, false),
      StructField("stringToString1", StringType, true),
      StructField("booleanToString", StringType, false),
      StructField("doubleToString", StringType, false),
      StructField("extraField", StringType, true),
      StructField("extraField1", StringType, true)
    ))

    val dfResult = df.applySchema(schema)
    val dfResultSkewed = df.applySchema(schemaSkewed)
    val dfResultOverflow = df.applySchema(schemaOverflow)

    dfResult.count() shouldEqual 10
    dfResult.columns.size shouldEqual 8
    dfResultSkewed.count() shouldEqual 10
    dfResultSkewed.columns.size shouldEqual 8
    dfResultOverflow.count() shouldEqual 10
    dfResultOverflow.columns.size shouldEqual 10

    val schemaSkewedTest = schemaSkewed.add("_7", BooleanType, false).add("_8", DoubleType, false)

    dfResult.printSchema()

    dfResult.schema shouldEqual schema
    dfResultOverflow.schema shouldEqual schemaOverflow
    dfResultSkewed.schema shouldEqual schemaSkewedTest
  }

  "concatActionsUdf" should "concat an action name and and action value" in {
    val dummyDf = TestSets.dfDummy
    val newDf = dummyDf.concatActions("name", "gender", "newColumn", "=", ";")
    newDf.take(1)(0).getString(2) should equal ("name=gender;")
  }

  it should "generate a null value" in {
    val dummyDf = TestSets.emptyDfDummy
    val newDf = dummyDf.concatActions("name", "gender", "newColumn", "=", ";")
    newDf.take(1)(0).getString(2) should equal (null)
  }


}
