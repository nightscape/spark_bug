package spark

import util.{CaseClass, ReflectionUtil}
import ReflectionUtil._
import DataFrameConversions._

import java.time.LocalDate
import org.apache.spark.sql.types.StructField
import org.scalatest.{EitherValues, FunSpec, Matchers, BeforeAndAfter}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, DataTypes}
import scala.reflect.runtime.universe._
import scala.collection.mutable.LinkedHashMap

case class Dummy(someId: Long, otherId: Long)
case class Middle[T](
  scope: T
)
case class Inner(
  id: Long
)
case class Outermost[+T](
  obj: T,
  someValue: Double
)
class DataFrameConversionsSpec
  extends FunSpec
    with Matchers
{
  def getSession(threads:String = "*", applicationName:String = "TestApp"): SparkSession = {
    SparkSession
      .builder()
      .master(s"local[$threads]")
      .appName(applicationName)
      .getOrCreate()
  }

  val spark = getSession()
  implicit val sparkImplicits = spark.implicits
  implicit val sparkConfig = spark.sparkContext.getConf
  implicit val sqlContext = spark.sqlContext
  import sparkImplicits._

  def flatFieldsAndTypesFor[T : TypeTag]: Seq[(String, Type)] = {
    val tt = implicitly[TypeTag[T]]
    flatFieldsAndTypes("", tt.tpe)
  }

  def flatFieldsAndTypes(name: String, tpe: Type): Seq[(String, Type)] = {
    tpe match {
      case CaseClass(tpe, fields, _) => fields.flatMap(f => flatFieldsAndTypes(f.name.toString, f.typeSignatureIn(tpe).finalResultType))
      case _ => Seq(name -> tpe)
    }
  }

  def flattenSchema(schema: StructType, prefix: Option[String] = None) : Array[Column] = {
    schema.fields.flatMap { f =>
      val colName = prefix.map(_ + ".").getOrElse("") + f.name
      f.dataType match {
        case st: StructType => flattenSchema(st, Some(colName))
        case _ => Array(col(colName))
      }
    }
  }

  def flattenDataFrame(df: DataFrame): DataFrame =
    df.select(flattenSchema(df.schema): _*)

  def toFlatDataFrame[T <: Product : TypeTag](ts: Seq[T]): DataFrame =
    flattenDataFrame(spark.createDataset[T](ts).toDF)

  def doBusyWork(): Unit = {
    for(i <- 0 to 10) {
      val flatDf = toFlatDataFrame(Seq.empty[Outermost[Middle[Inner]]]).cache
      val dataset = flatDf.toDS[Outermost[Middle[Inner]]]
      dataset.collect
    }
  }

  val rows = List(Outermost(Middle(Inner(-262085534340088811L)),-1.932810953673972E247), Outermost(Middle(Inner(3975961295672879120L)),3.0437389599087003E-48))

  it("failing example without cache") {
    doBusyWork()
    val flatDf = toFlatDataFrame(rows)
    val dataset = flatDf.toDS[Outermost[Middle[Inner]]]
    dataset.collect should equal(rows)
  }

  it("working example with cache") {
    doBusyWork()
    val flatDf = toFlatDataFrame(rows).cache
    val dataset = flatDf.toDS[Outermost[Middle[Inner]]]
    dataset.collect should equal(rows)
  }
}
