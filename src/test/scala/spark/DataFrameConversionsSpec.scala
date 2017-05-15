package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.scalatest.{FunSpec, Matchers}

case class Outermost[+T](
  obj: T,
  someValue: Double
)
case class Middle[T](
  scope: T
)
case class Inner(
  id: Int
)

class DataFrameConversionsSpec extends FunSpec with Matchers {

  val spark = SparkSession
    .builder()
    .master(s"local[2]")
    .getOrCreate()
  import spark.implicits._

  val flatSchema = Seq(col("obj.scope.id"), col("someValue"))

  def toFlatDataFrame(ts: Seq[Outermost[Middle[Inner]]]): DataFrame =
    spark.createDataset[Outermost[Middle[Inner]]](ts).toDF.select(flatSchema: _*)

  val rows = List(Outermost(Middle(Inner(1)),1.0), Outermost(Middle(Inner(2)),2.0))
  val innerSchema = StructType(Seq(StructField("id",IntegerType,true)))
  val middleSchema = StructType(Seq(StructField("scope",innerSchema,true)))

  def unflattenDf(df: DataFrame): Seq[Outermost[Middle[Inner]]] = {
    val dataset = df.withColumn("obj", struct(struct(col("id"))).cast(middleSchema)).as[Outermost[Middle[Inner]]]
    dataset.collect
  }

  it("failing example without cache") {
    unflattenDf(toFlatDataFrame(rows)) should equal(rows)
  }

  it("working example with cache") {
    unflattenDf(toFlatDataFrame(rows).cache) should equal(rows)
  }

  it("same as the failing example, but this time it works") {
    unflattenDf(toFlatDataFrame(rows)) should equal(rows)
  }
}
