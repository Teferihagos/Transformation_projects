import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, regexp_replace, when}

object Main {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSql")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val csvFilePath = raw"data/sample_data.csv"
    // Read the CSV file
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    // Show the original DataFrame
    df.show()


    //    Transformation 1: Selecting specific columns (name, age)
      df.select("name", "age").show()

    //    Transformation 2: Filtering rows based on a condition Age >30"""
       df.filter(col("age") > 40).show()

    //    """Transformation 3: Adding a new column (Salary+10000)"""
      df.withColumn("Salary", col("Salary") + 1000).show()

    //    """ Transformation 4: Grouping and aggregating data, based on age calculate average salary"""
      df.groupBy("Age").agg(avg("age").as("Average_age")).show()

    //    """Transformation 5: Sorting by a column (order by age)"""
      df.orderBy("Age").show()

    //    """Transformation 6: Adding a new column with a conditional expression( if age >30 "Yes" else "No")"""
        val greater_30 = df.withColumn("Age_group",
          when(col("age")>30, "Yes").otherwise("No")
        )
        greater_30.show()


    //    """Transformation 7: Dropping a column (drop salary)"""
       df.drop("Salary").show()

    //    """Transformation 8: Renaming columns (name to full name)"""
       df.withColumnRenamed("Name", "Full Name").show()

    //    """Transformation 9: Union of two DataFrames"""
    val data = Seq(("Hardy", 30, 3444), ("DRog", 22, 50000))
    val columns = Seq("Name", "Age", "Salary")

    val df2: DataFrame = spark.createDataFrame(data).toDF(columns: _*)
    df2.show()

    val newdf = df.union(df2)
    newdf.show()
    
    spark.stop()
  }
}