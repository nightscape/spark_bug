package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SQLImplicits}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object DataFrameConversions {
  trait ColumnSpec {
    def name: String
  }
  case class StructColumn(name: String, columns: Seq[ColumnSpec]) extends ColumnSpec
  case class PrimitiveColumn(name: String, tpe: Type, defaultValue: Option[Any] = None) extends ColumnSpec

  implicit class RichDataFrame(val df: DataFrame) {
    def toDsFromSpec[T <: Product : TypeTag](structColumn: StructColumn)(implicit sparkConf: SparkConf, sparkImplicits: SQLImplicits): Dataset[T] = {
      import sparkImplicits._
      val (columnMap, _) = sparkColumnsForStruct(structColumn, df.schema, needsStructName = false)
      val mappedDf = columnMap.foldLeft(df) { case (renamedDf, (newColumnName, columnDefinition)) => renamedDf.withColumn(newColumnName, columnDefinition)}
      val smallDf = mappedDf.select(columnMap.map(t => col(t._1)) : _*)
      smallDf.as[T]
    }

    val dataTypeForClass: Map[Class[_], DataType] = Map(
      classOf[Double] -> DataTypes.DoubleType,
      classOf[Boolean] -> DataTypes.BooleanType,
      classOf[java.lang.Boolean] -> DataTypes.BooleanType
    )

    private def toSparkColumn(columnSpec: ColumnSpec, inSchema: StructType, needsStructName: Boolean = true): (String, Column, DataType) = columnSpec match {
      case PrimitiveColumn(name, _, defaultValue) =>
        val fieldOption = correspondingColumn(name, inSchema)
        val (column, dt) =
          fieldOption.map(f => col(f.name).as(name) -> f.dataType)
            .orElse(defaultValue.map(d => lit(d) -> dataTypeForClass(d.getClass)))
            .getOrElse(throw new RuntimeException(s"Could not find corresponding column for $name in $inSchema"))
        (name, column, dt)
      case s @ StructColumn(name, _) =>
        val (columns, outSchema) = sparkColumnsForStruct(s, inSchema)
        val strct = struct(columns.map(_._2): _*).cast(outSchema)
        (name, if(needsStructName) strct.as(name) else strct, outSchema)
    }

    private def sparkColumnsForStruct(structColumn: StructColumn, inSchema: StructType, needsStructName: Boolean = true): (Seq[(String, Column)], StructType) = {
      val (names, columns, dataTypes) = structColumn.columns.map(c => toSparkColumn(c, inSchema, needsStructName)).unzip3
      val outSchema =
        StructType(
          names
            .zip(dataTypes)
            .map { case(name, dataType) =>
              StructField(name, dataType)
            }
        )
      (names.zip(columns), outSchema)
    }

    private def correspondingColumn(str: String, schema: StructType): Option[StructField] = schema.find(a => str == a.name)

  }
}
