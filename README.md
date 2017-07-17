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
              
# Columns and Expressions

columns in Spark are similar to columns in a spreadsheet, R dataframe, pandas DataFrame. We can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions.

To Spark, columns are logical constructions that simply represent a value computed on a per-record basis by means of an expression. This means, in order to have a real value for a column, we need to have a row, and in order to have a row we need to have a DataFrame. This means that we cannot manipulate an actual column outside of a DataFrame, we can only manipulate a logical column’s expressions then perform that expression within the context of a DataFrame.

# Accessing a DataFrame’s Columns

Sometimes you’ll need to see a DataFrame’s columns, you can do this by doing something like printSchema however if you want to programmatically access columns, you can use the columns method to see all columns listed.

                  Sometimes you’ll need to see a DataFrame’s columns, you can do this by doing something like printSchema however if you want to programmatically access columns, you can use the columns method to see all columns listed.

                        spark.read.format("json")
                          .load("/mnt/defg/flight-data/json/2015-summary.json")
                          .columns
                          
 # Records and Rows
 
In Spark, a record or row makes up a “row” in a DataFrame. A logical record or row is an object of type Row. Row objects are the objects that column expressions operate on to produce some usable value. Row objects represent physical byte arrays. The byte array interface is never shown to users because we only use column expressions to manipulate them.


We can see a row by calling first on our DataFrame.

                        df.first()
                    


# DataFrame Transformations

Now that we briefly defined the core parts of a DataFrame, we will move onto manipulating DataFrames. When working with individual DataFrames there are some fundamental objectives. These break down into several core operations.



    We can add rows or columns

    We can remove rows or columns

    We can transform a row into a column (or vice versa)

    We can change the order of rows based on the values in columns

Luckily we can translate all of these into simple transformations, the most common being those that take one column, change it row by row, and then return our results.

Creating DataFrames

we can create DataFrames from raw data sources.  we will also register this as a temporary view so that we can query it with SQL.

                        val df = spark.read.format("json")
             .load("path/to/file.json")

                  df.createOrReplaceTempView("dfTable")
                  
                  
  We can also create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.
  
                        import org.apache.spark.sql.Row
                        import org.apache.spark.sql.types.{StructField, StructType,
                                   StringType, LongType}

                        val myManualSchema = new StructType(Array(
                        new StructField("some", StringType, true),
                        new StructField("col", StringType, true),
                        new StructField("names", LongType, false) // just to illustrate flipping this flag
                        ))

                        val myRows = Seq(Row("Hello", null, 1L))
                        val myRDD = spark.sparkContext.parallelize(myRows)

                        val myDf = spark.createDataFrame(myRDD, myManualSchema)
                        myDf.show()
                  
