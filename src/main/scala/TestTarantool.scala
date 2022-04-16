package demo.tarantool

import org.apache.spark.sql.{SaveMode, SparkSession}
import io.tarantool.driver.api.conditions.Conditions
import io.tarantool.driver.api.tuple.{DefaultTarantoolTupleFactory, TarantoolTuple}
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory
import io.tarantool.spark.connector.toSparkContextFunctions



import scala.collection.JavaConverters.seqAsJavaListConverter


object TestTarantool extends App {

  // 1. Set up the Spark session
  val spark = SparkSession.builder()
    .appName("test tarantool")
    .master("local[1]")
    .config("tarantool.hosts", "127.0.0.1:3301")
    .config("tarantool.username", "admin")
    .config("tarantool.password", "testapp-cluster-cookie")
    .enableHiveSupport()
    .getOrCreate()


  val sc = spark.sparkContext

  // 2. Load the whole space
  val rdd: Array[TarantoolTuple] = sc.tarantoolSpace("test_space", Conditions.any()).collect()

  // 3. Filter using conditions
  // This mapper will be used implicitly for tuple conversion
  val mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()

  val startTuple = new DefaultTarantoolTupleFactory(mapper).create(List(1).asJava)
  val cond: Conditions = Conditions
    .indexGreaterThan("id", List(1).asJava)
    .withLimit(2)
    .startAfter(startTuple)
  val tuples: Array[TarantoolTuple] = sc.tarantoolSpace("test_space", cond).collect()

  // 4. Load the whole space into a DataFrame
  val df = spark.read
    .format("org.apache.spark.sql.tarantool")
    .option("tarantool.space", "test_space")
    .load()

  // Space schema from Tarantool will be used for mapping the tuple fields
  val tupleIDs: Array[Any] = df.select("id").rdd.map(row => row.get(0)).collect()

  // 5. Write a Dataset to a Tarantool space

  val ds = spark.sql(
    """
      |select 1 as id, null as bucketId, 'Don Quixote' as bookName, 'Miguel de Cervantes' as author, 1605 as year union all
      |select 2, null, 'The Great Gatsby', 'F. Scott Fitzgerald', 1925 union all
      |select 2, null, 'War and Peace', 'Leo Tolstoy', 1869
      |""".stripMargin)


  try {
    // Write to the space. Different modes are supported
    ds.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Overwrite)
      .option("tarantool.space", "test_space")
      .save()
  } finally {
    // If you don't close the context even in the case of an exception,
    // it will not be closed automatically, and so the application listeners closing
    // the Tarantool connections will not be invoked.
    sc.stop()
    spark.close()
  }
}

case class Book(
  id: Int,
  bucketId: Int,
  bookName: String,
  author: String,
  year: Int
)