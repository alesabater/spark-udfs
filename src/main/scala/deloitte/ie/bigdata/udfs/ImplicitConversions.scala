package deloitte.ie.bigdata.udfs

import deloitte.ie.bigdata.udfs.dataframes.GenericUDFs
import org.apache.spark.sql.DataFrame

object ImplicitConversions {
  implicit def dataframe2GenericUDF(df: DataFrame): GenericUDFs = GenericUDFs(df)
  implicit def genericUDF2DataFrame(dfFunction: GenericUDFs): DataFrame = dfFunction.df
}
