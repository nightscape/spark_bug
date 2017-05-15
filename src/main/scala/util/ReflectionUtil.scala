package util

import reflect._
import scala.reflect.runtime.{ currentMirror => cm }
import scala.reflect.runtime.universe._

object ReflectionUtil {
  def companionApplyDefaultValues[A](implicit t: TypeTag[A]): Map[TermSymbol, Any] =
    constructorParametersWithDefaults(t.tpe).collect { case(sym, Some(defaultValue)) => sym -> defaultValue}

  def constructorParametersWithDefaults(tpe: Type): Map[TermSymbol, Option[Any]] = {
    val cls = cm classSymbol cm.runtimeClass(tpe)
    val modul = cls.companion.asModule
    val im = cm reflect (cm reflectModule modul).instance
    val ts = im.symbol.typeSignature
    val name = "apply"
    val at = TermName(name)
    val method = (ts member at).asMethod

    def valueFor(p: Symbol, i: Int): Option[Any] = {
      val defarg = ts member TermName(s"$name$$default$$${i+1}")
      if (defarg != NoSymbol) {
        Some((im reflectMethod defarg.asMethod)())
      } else {
        None
      }
    }
    method.paramLists.flatten.zipWithIndex.map(p => p._1.asTerm -> valueFor(p._1,p._2)).toMap
  }
}
