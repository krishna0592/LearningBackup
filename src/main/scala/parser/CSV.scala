package parser

import java.io.FileNotFoundException

import app.MainApp.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

class CSV {

  def csvAsDF(filePath: String): DataFrame ={
    var df: DataFrame = null
    try
      df = spark.read
          .option("header", "true")                           // mention whether header is there in the file or not
          .option("delimiter", ",")                           // mention the delimiter of the csv file
          .option("ignoreLeadingWhiteSpace","true")           // ignores the leading white space in each and every cell
          .option("ignoreTrailingWhiteSpace","true")          // ignores the leading white space in each and every cell
          .option("timestampFormat" ,"yyyy-MM-dd HH:mm:ss")   // reads the data in specific timestamp format
          .option("inferSchema","true")                       // reads the data and creates the schema based on the datatype (i.e,) int, timestamp, string etc
          //.schema(schema)                                   // If need also pass the schema of the file like val schema = new StructType(Array(StructField("Name",StringType,true),StructField("Place",StringType,true),StructField("Age",IntegerType,true)))
          //.option("mode","DROPMALFORMED")                   // eliminates the row which is not matching the schema and creates dataframe with remaining rows
          //.option("badRecordsPath", "/tmp/badRecordsPath")  // moves all rows in a seperate file which are not matching schema
          //.option("nullValue", "")                          // option to handle the null values in the data
          //.csv(paths:_*)                                    // read multiple files from a particular directory (val paths = List("C:spark\\sample_data\\tmp\\cars1.csv", "C:spark\\sample_data\\tmp\\cars2.csv"))
          //.csv(folders:_*)                                  // read multiple csv files from multiple directory (val folders = List("C:spark\\sample_data\\tmp", "C:spark\\sample_data\\tmp1"))
        .csv(filePath) // read one file from one directory
    catch{
      case fnf: FileNotFoundException => println("There is no file in the location")
      case analysis: AnalysisException => println("There is some special characters like '.' or '/' in the column name/data \n" + analysis)
      case e: Exception => println("Couldn't able to read csv data")
    }
    /*
    Note:
      1. mode possibilities:
          a. PERMISSIVE: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
          b. DROPMALFORMED: drops lines which have fewer or more tokens than expected or tokens which do not match the schema
          c. FAILFAST: aborts with a RuntimeException if encounters any malformed line
      2. if you are use '/' and '.' these special characters in your data it is better to replace these characters with another unique special characters.
   */
    df
  }

  def csvAsRdd(filepath: String): RDD[Row] = csvAsDF(filepath).rdd

  def dfAsCSV(df: DataFrame,filepath: String): Unit ={
    df.coalesce(1).write
      .option("header","true")                           // mention whether header is there in the file or not
      .option("delimiter",",")                           // mention the delimiter of the csv file
      .option("quote", "\u0000")                         // eliminate the double quotes while writting the data
      .mode("overwrite")                      // we can mention whether we can overwrite or append to the existing file
      .csv(filepath)
    /*
    Note:
      coalesce uses existing partitions to minimize the amount of data that's shuffled.
      repartition creates new partitions and does a full shuffle.
      coalesce results in partitions with different amounts of data (sometimes partitions that have much different sizes) and repartition results in roughly equal sized partitions.

      coalesce may run faster than repartition, but unequal sized partitions are generally slower to work with than equal sized partitions.
      You'll usually need to repartition datasets after filtering a large data set.
      I've found repartition to be faster overall because Spark is built to work with equal sized partitions.
    */
  }

}
