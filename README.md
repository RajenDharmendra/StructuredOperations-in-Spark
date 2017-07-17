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
                  
# Select

Select  allow us to do the DataFrame equivalent of SQL queries on a table of data.


            SELECT * FROM dataFrameTable
            SELECT columnName FROM dataFrameTable
            SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable
            
In the simplest possible terms, it allows us to manipulate columns in our DataFrames. Let’s walk through some examples on DataFrames to talk about some of the different ways of approaching this problem. The easiest way is just to use the select method and pass in the column names as string that you would like to work with.

            df.select("DEST_COUNTRY_NAME").show(2)
            %sql
            SELECT DEST_COUNTRY_NAME
            FROM dfTable
            LIMIT 2
            
As we’ve seen thus far, expr is the most flexible reference that we can use. It can refer to a plain column or a string manipulation of a column. To illustrate, let’s change our column name, then change it back as an example using the AS keyword and then the alias method on the column.

            df.select(expr("DEST_COUNTRY_NAME AS destination"))
            
            
            %sql

            SELECT
            DEST_COUNTRY_NAME as destination
            FROM
             dfTable
             
             
             
            %sql
            
            SELECT
             *,
             (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
            FROM
              dfTable
              
              
              
we can specify aggregations over the entire DataFrame 

            %sql

            SELECT
              avg(count),
             count(distinct(DEST_COUNTRY_NAME))
            FROM
            dfTable
         
         
Renaming Columns

we can remane a column using the withColumnRenamed method. This will rename the column with the name of the string in the first argument, to the string in the second argument.

    
            df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
            
# Removing Columns

Now that we’ve created this column, let’s take a look at how we can remove columns from DataFrames. However there is also a dedicated method called drop.
            
            df.drop("ORIGIN_COUNTRY_NAME").columns
            
We can drop multiple columns by passing in multiple columns as arguments.

           dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
     
Changing a Column’s Type (cast)

Sometimes we may need to convert from one type to another, for example if we have a set of StringType that should be integers. We can convert columns from one type to another by casting the column from one type to another. For instance let’s convert our count column from an integer to a Long type.

            %sql

            SELECT
            cast(count as int)
            FROM
             dfTable
          
Getting Unique Rows

A very common use case is to get the unique or distinct values in a DataFrame. These values can be in one or more columns. The way we do this is with the distinct method on a DataFrame that will allow us to deduplicate any rows that are in that DataFrame. For instance let’s get the unique origins in our dataset. This of course is a transformation that will return a new DataFrame with only unique rows.
            
            df.select("ORIGIN_COUNTRY_NAME").distinct().count()
            
            df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").count()
     
using %sql
            
            %sql

            SELECT
             COUNT(DISTINCT ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)
            FROM dfTable
            
            
            %sql
            SELECT
              COUNT(DISTINCT ORIGIN_COUNTRY_NAME)
            FROM dfTable
         
Random Samples

Sometimes you may just want to sample some random records from your DataFrame. This is done with the sample method on a DataFrame that allows you to specify a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without replacement.

            val seed = 5
            val withReplacement = false
            val fraction = 0.5

            df.sample(withReplacement, fraction, seed).count()
