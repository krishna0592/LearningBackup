package parser

import java.io.FileNotFoundException

import app.MainApp.spark
import org.apache.spark.sql.{AnalysisException, DataFrame}

class CSV {

  def csvAsDF(filePath: String): DataFrame ={
    var df: DataFrame = null
    try
      df = spark.read
        .option("header", "true") // mention whether header is there in the file or not
        .option("delimiter", ",") // mention the delimiter of the csv file
        //.schema(schema)                                   // If need also pass the schema of the file like val schema = new StructType(Array(StructField("Name",StringType,true),StructField("Place",StringType,true),StructField("Age",IntegerType,true)))
        //.option("mode","DROPMALFORMED")                    // eliminates the row which is not matching the schema and creates dataframe with remaining rows
        //.option("badRecordsPath", "/tmp/badRecordsPath")  // moves all rows in a seperate file which are not matching schema
        //.option("nullValue", "")                          // option to handle the null values in the data
        //.csv(paths:_*)                                    // read multiple files from a particular directory (val paths = List("C:spark\\sample_data\\tmp\\cars1.csv", "C:spark\\sample_data\\tmp\\cars2.csv"))
        //.csv(folders:_*)                                  // read multiple csv files from multiple directory (val folders = List("C:spark\\sample_data\\tmp", "C:spark\\sample_data\\tmp1"))
        .csv(filePath) // read one file from one directory
    catch{
      case fnf: FileNotFoundException => println("There is no file in the location")
      case analysis: AnalysisException => println("There is some special characters like '.' or '/' in the column name/data \n" + analysis)
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


}
