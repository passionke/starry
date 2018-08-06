# What's Starry?

Starry brings amazingly `1-2ms`  response time to spark  
when you deploy spark application with local mode.

# Why Starry 

Since Spark supports complex sql and if you want a memory db, with Starry, Spark will be a possible solution.
We also use starry to deploy ML predict service.

## maven repo

```
<dependency>
  <groupId>com.github.passionke</groupId>
  <artifactId>starry</artifactId>
  <version>1.0</version>
</dependency>
```


## Quick tutorial

Starry has enhanced SparkContext and Spark SQL engine, so you should use StarrySparkContext instead of original 
SparkContext.

```scala
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.execution.LocalBasedStrategies

val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("aloha")
  sparkConf
    .set("spark.default.parallelism", "1")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.broadcast.manager", "rotary")
    .set("rotary.shuffer", "true")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.extensions", "org.apache.spark.sql.StarrySparkSessionExtension")
  
  val sparkContext = new StarrySparkContext(sparkConf)
  // now you have got one enhanced sparkSession. Using it just as usual.
  val sparkSession: SparkSession =
    SparkSession.builder
      .sparkContext(sparkContext)
      .getOrCreate
  // Starry also provide some extraStrategies optimized for local mode. 
  // Using LocalBasedStrategies to register.
  LocalBasedStrategies.register(sparkSession)  
```

## Tips

Use `createDataSet(Seq[T])` instead of `createDataSet(RDD[T])` to build your data Eg.

```scala
//do like this:
val strList = JSONArray.fromObject(param("data", "[]")).map(f => StringFeature(f.toString))
import sparkSession.implicits._
val res = sparkSession.createDataset(strList).selectExpr(sql).toJSON.collect().mkString(",")

//do not like this:
val strList = JSONArray.fromObject(param("data", "[]")).map(f => StringFeature(f.toString))
val rdd = sparkSession.sparkContext.parallelize(strList, perRequestCoreNum)
import sparkSession.implicits._
val res = sparkSession.createDataset(rdd).selectExpr(sql).toJSON.collect().mkString(",")
```
