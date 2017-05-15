package spark

import DataFrameConversions._

import java.time.LocalDate
import org.apache.spark.sql.types.StructField
import org.scalatest.{EitherValues, FunSpec, Matchers, BeforeAndAfter}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, DataTypes}
import scala.reflect.runtime.universe._
import scala.collection.mutable.LinkedHashMap

case class Outermost[+T](
  obj: T,
  someValue: Double
)
case class Middle[T](
  scope: T
)
case class Inner(
  id: Long
)
class DataFrameConversionsSpec
  extends FunSpec
    with Matchers
{
  val spark = SparkSession
    .builder()
    .master(s"local[2]")
    .getOrCreate()
  implicit val sparkImplicits = spark.implicits
  implicit val sparkConfig = spark.sparkContext.getConf
  implicit val sqlContext = spark.sqlContext
  import sparkImplicits._

  val flatSchema = Seq(col("obj.scope.id"), col("someValue"))

  def toFlatDataFrame(ts: Seq[Outermost[Middle[Inner]]]): DataFrame =
    spark.createDataset[Outermost[Middle[Inner]]](ts).toDF.select(flatSchema: _*)

  val columnSpec = StructColumn("",Vector(StructColumn("obj",Vector(StructColumn("scope",Vector(PrimitiveColumn("id",typeOf[Long],None))))), PrimitiveColumn("someValue",typeOf[Double],None)))
  def doBusyWork(): Unit = {
    for(i <- 0 to 10) {
      val flatDf = toFlatDataFrame(Seq.empty[Outermost[Middle[Inner]]]).cache
      val dataset = flatDf.toDsFromSpec[Outermost[Middle[Inner]]](columnSpec)
      dataset.collect
    }
  }

  val rows = List(Outermost(Middle(Inner(-262085534340088811L)),-1.932810953673972E247), Outermost(Middle(Inner(3975961295672879120L)),3.0437389599087003E-48))

  it("failing example without cache") {
    doBusyWork()
    val flatDf = toFlatDataFrame(rows)
    val dataset = flatDf.toDsFromSpec[Outermost[Middle[Inner]]](columnSpec)
    dataset.collect should equal(rows)
  }

  it("working example with cache") {
    doBusyWork()
    val flatDf = toFlatDataFrame(rows).cache
    val dataset = flatDf.toDsFromSpec[Outermost[Middle[Inner]]](columnSpec)
    dataset.collect should equal(rows)
  }
}
