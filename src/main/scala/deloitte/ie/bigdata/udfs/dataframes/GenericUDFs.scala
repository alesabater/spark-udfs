package deloitte.ie.bigdata.udfs.dataframes

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class GenericUDFs(df: DataFrame) extends Logging {

  /**
    * Drops one or multiple columns from a dataframe
    *
    * @param columns Columns to be dropped
    * @return DataFrame without the specified columns
    */
  def dropColumns(columns: String*) = columns match {
    case Nil => df
    case _ => {
      var dataFrame = df
      for (c <- columns) {
        dataFrame = dataFrame.drop(c)
        log.info(c + " Column has been dropped")
      }
      dataFrame
    }
  }

  /**
    * UDF that based on a string mapping of the form current_name -> new_name, renames columns of a dataFrame
    *
    * @param renamingMap Map where they key is the existing name and the value the new name [String, String]
    * @return
    */
  def renameColumns(renamingMap: Map[String, String]): DataFrame = {
    df.select(df.columns.map(c => {
      val target = if (c.contains(".")) "`%s`".format(c) else c
      col(target).as(renamingMap.getOrElse(c, target))
    }): _*)
  }

  /**
    * UDF that concatenates the value of multiples columns into a desired new column. The columns to concatenate can be
    * of any dataType as long as these are able to be casted to Strings
    *
    * @param newColumnName  Name of new column
    * @param delimiter      delimiter between the values of the columns concatenated
    * @param columnsToMerge Columns to merge
    * @return DataFrame result
    */
  def concatStringColumns(newColumnName: String, delimiter: String, columnsToMerge: String*): DataFrame = {
    val concatColumns = udf[String, String, String, String]((del: String, column1: String, column2: String) => List(column1, column2) mkString (del))
    val columns = columnsToMerge.filter(s => df.columns.contains(s))
    val dfWithRowKey = df.withColumn(newColumnName, col(columns.head).cast(StringType))
    val dfWithRowKeyAndDelimiter = columns.tail.foldLeft(dfWithRowKey) {
      (data, column) =>
        data.withColumn(newColumnName, concatColumns(lit(Option(delimiter) getOrElse ("##")), col(newColumnName), col(column).cast(StringType)))
    }
    dfWithRowKeyAndDelimiter
  }

//  TODO: Decide which of these 2 methods stays !!!
  def concatActions(column1: String, column2: String, concatenatedCol: String, columnSeparator: String = "", endSignal: String = ""): DataFrame = {
    val concatColumnUdf = udf[String, String, String]((column1: String, column2: String) => {
      if(column1 != null) (column1 + columnSeparator + column2 + endSignal)
      else column1
    })
    val allColumns = df.columns.toList
    val trimmedColumns = allColumns.map(a => a.trim)
    val doColumnsExist = trimmedColumns.contains(column1) && trimmedColumns.contains(column2)
    doColumnsExist match {
      case true => df.withColumn(concatenatedCol, concatColumnUdf(col(column1), col(column2)))
      case false => throw new NoSuchElementException("Column does not exist!")
    }
  }

  /**
    * UDF that updates a list of columns in place, without changing their names
    *
    * @param cols List os columns to be updated
    * @param udf  UserDefinedFunction to update the columns with
    * @return DataFrame that results with updated columns
    */
  def updateColumns(udf: UserDefinedFunction, cols: String*): DataFrame = {
    val allColumns = df.columns.toList
    val columnsToModify = cols.filter(s => allColumns.contains(s))
    columnsToModify.foldLeft(df) {
      (data, column) => data.withColumn(column, udf(col(column)))
    }
  }

  def applySchema(schema: StructType): DataFrame = {
    val newSchemaCols: Array[(String, DataType)] = schema.map(sf => (sf.name, sf.dataType)).toArray
    val oldSchemaCols: Array[String] = df.columns
    val newSchemaSize: Int = newSchemaCols.size
    val oldSchemaSize: Int = df.columns.size
    val diff = newSchemaSize - oldSchemaSize
    val dfR = diff match {
      case x if x == 0 => {
        log.warn("ApplySchema => Both DataFrame and Schema provided have the same number of columns")
        (0 until newSchemaSize).foldLeft(df) { (data, index) =>
          val (newName, newDataType) = newSchemaCols(index)
          val oldName = oldSchemaCols(index)
          data.withColumnRenamed(oldName, newName).withColumn(newName, col(newName).cast(newDataType))}
      }
      case x if x < 0 => {
        log.warn("ApplySchema => The Schema provided has " + Math.abs(x) + " less columns than the provided DataFrame")
        (0 until newSchemaSize).foldLeft(df) { (data, index) =>
          val (newName, newDataType) = newSchemaCols(index)
          val oldName = oldSchemaCols(index)
          data.withColumnRenamed(oldName, newName).withColumn(newName, col(newName).cast(newDataType))}
      }
      case x if x > 0 => {
        log.warn("ApplySchema => The Schema provided has " + Math.abs(x) + " more columns than the provided DataFrame")
        val dfMid = (0 until oldSchemaSize).foldLeft(df) { (data, index) =>
          val (newName, newDataType) = newSchemaCols(index)
          val oldName = oldSchemaCols(index)
          data.withColumnRenamed(oldName, newName).withColumn(newName, col(newName).cast(newDataType))}
        (oldSchemaSize until newSchemaSize).foldLeft(dfMid) { (data, index) =>
          val (newName, newDataType) = newSchemaCols(index)
          data.withColumn(newName, lit(null).cast(newDataType))
        }
      }
    }
    dfR
  }
}