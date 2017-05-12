package deloitte.ie.bigdata.udfs

import deloitte.ie.bigdata.commons.Contexts
import org.apache.spark.sql.DataFrame

object TestSets {

  val sc = Contexts.sc
  val sqlCtx = Contexts.sqlCtx

  val dfFullSchema = sqlCtx.createDataFrame(Seq(
    ("a", "1", "1.0"),
    ("a", "2", "2.232"),
    ("a", "3", "3.33"),
    ("a", "4", "4.1"),
    ("a", "5", "5.45"),
    ("a", null, null),
    ("a", "?", "?"),
    ("a", "1,000,000", "3443,433"),
    ("a", "1,000,943", "43.433")
  )).toDF("StringToString", "StringToInt", "StringToDouble")


  val dfInt = sqlCtx.createDataFrame(Seq(
    (10, 1),
    (23, 1),
    (14, 1),
    (34, 1),
    (11, 1)
  )).toDF("count", "any")



  val dfRename = sqlCtx.createDataFrame(Seq(
    (1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1)
  )).toDF("one.two", "three", "four", "five", "six")

  val dfEmptyStrings = sqlCtx.createDataFrame(Seq(
    ("a", 1),
    ("a ", 1),
    (" ", 1),
    ("      ", 1),
    ("      3", 1)
  )).toDF("blanks", "any")

  val dfMixed = sqlCtx.createDataFrame(Seq(
    ("a", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("e", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("b", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("e", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L),
    ("e", 1, 0.0, true, 1245L, "b", 2, 0.0, false, 1L))
  ).toDF("string", "int", "double", "boolean", "long", "string1", "int1", "double1", "boolean1", "long1")

  val dfMonth = sqlCtx.createDataFrame(Seq(
    (1441978500000L, 1),
    (1439975400000L, 1),
    (1423477500000L, 1),
    (1467533100000L, 1),
    (1398149100000L, 1),
    (-1L, 1))
  ).toDF("ts", "int")

  val dfBlanks = sqlCtx.createDataFrame(Seq(
    ("a", "any"),
    ("b", "any"),
    ("", ""),
    ("", "any"),
    ("   ", ""),
    ("c", "any"),
    ("a", "any"),
    ("d", "")
  )).toDF("blanks", "blanks1")

  val dfBadRows = sqlCtx.createDataFrame(Seq(
    ("a", "?", "a", "a"),
    ("b", "a", "", "a"),
    ("b", "a", "a", "a"),
    ("b", "a", "?", "a"),
    ("?", "?", "?", "?"),
    ("b", "a", "a", "a"),
    ("b", "a", "a", "a"),
    ("b", "a", "?", "?"),
    ("?", "?", "a", ""),
    ("e", "a", "a", "a"),
    ("e", "a", "a", "a"))
  ).toDF("string", "string1", "string2", "string3")

  val dfCategories = sqlCtx.createDataFrame(Seq(
    (0.0234, 1.011, 0.034354535324, 0.0323344),
    (2.0234, 0.011, 2.034354535324, 0.0323344),
    (2.0234, 0.011, 0.034354535324, 0.0323344),
    (2.0234, 0.011, 1.034354535324, 0.0323344),
    (1.0234, 1.011, 1.034354535324, 1.0323344),
    (2.0234, 0.011, 0.034354535324, 0.0323344),
    (2.0234, 0.011, 0.034354535324, 0.0323344),
    (2.0234, 0.011, 1.034354535324, 1.0323344),
    (1.0234, 1.011, 0.034354535324, 2.0323344),
    (3.0234, 0.011, 0.034354535324, 0.0323344),
    (3.0234, 0.011, 0.034354535324, 0.0323344))
  ).toDF("string", "string1", "string2", "string3")

  val dfCategoriesString = sqlCtx.createDataFrame(Seq(
    ("FRA", "N", "O"),
    ("FRA", "N", "O"),
    ("MUC", "N", "O"),
    ("FRA", "N", "O"),
    ("MUC", "N", "O"),
    ("FRA", "Y", "O"),
    ("MUC", "Y", "O"),
    ("BCN", "Y", "O"),
    ("FRA", "N", "O"),
    ("TXL", "Y", "O"),
    ("FRA", "Y", "O"))
  ).toDF("string", "string1", "string2")


  val samplingDataFrame: DataFrame = sqlCtx.createDataFrame(Seq(
    ("FRA", 5),
    ("FRA", 9),
    ("FRA", 3),
    ("FRA", 18),
    ("FRA", 6),
    ("FRA", 23),
    ("WAW", 90),
    ("WAW", 12)
  )).toDF("nextDestination", "avg_stay_days")

  val noSchemaDf: DataFrame = sqlCtx.createDataFrame(Seq(
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3),
    ("1","a","true","0.3",1,"a",true,0.3)
  ))

  def dfDummy = sqlCtx.createDataFrame(Seq(
    ("name", "gender"),
    ("name1", "gender1")
  )).toDF("name", "gender")

  def emptyDfDummy = sqlCtx.createDataFrame(Seq(
    (null, null),
    ("name1", "gender1")
  )).toDF("name", "gender")
}
