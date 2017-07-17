# StructuredOperations-in-Spark
StructuredOperations in Spark

DataFrame consists of a series of records (like rows in a table), that are of type Row, and a number of columns (like columns in a spreadsheet) that represent an computation expression that can performed on each individual record in the dataset. The schema defines the name as well as the type of data in each column. The partitioning of the DataFrame defines the layout of the DataFrame or Dataset’s physical distribution across the cluster. The partitioning scheme defines how that is broken up, this can be set to be based on values in a certain column or non-deterministically.

we can define a DataFrame with


            val df = spark.read.format("json")
          .load("path/to/json/file.json")
          
A schema is a StructType made up of a number of fields, StructFields, that have a name, type, and a boolean flag which specifies whether or not that column can contain missing or null values. Schemas can also contain other StructType (Spark’s complex types). We will see this in the next chapter when we discuss working with complex types.

Here’s out to create, and enforce a specific schema on a DataFrame. If the types in the data (at runtime), do not match the schema. Spark will throw an error.

          import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

            val myManualSchema = new StructType(Array(
              new StructField("DEST_COUNTRY_NAME", StringType, true),
              new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
              new StructField("count", LongType, false) // just to illustrate flipping this flag
            ))

            val df = spark.read.format("json")
              .schema(myManualSchema)
              .load("path/to/json/file.json.json")
              
#Columns and Expressions

columns in Spark are similar to columns in a spreadsheet, R dataframe, pandas DataFrame. We can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions.

To Spark, columns are logical constructions that simply represent a value computed on a per-record basis by means of an expression. This means, in order to have a real value for a column, we need to have a row, and in order to have a row we need to have a DataFrame. This means that we cannot manipulate an actual column outside of a DataFrame, we can only manipulate a logical column’s expressions then perform that expression within the context of a DataFrame.





