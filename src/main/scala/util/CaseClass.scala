package util

import ReflectionUtil._

import scala.reflect.runtime.universe._

object CaseClass {
  def unapply(tpe: Type): Option[(Type, Seq[MethodSymbol], Map[MethodSymbol, Any])] = {
    val caseAccs = tpe.decls.collect { case m: MethodSymbol if m.isCaseAccessor => m }.to[Vector]
    if (caseAccs.isEmpty)
      None
    else {
      val constructorParameters = constructorParametersWithDefaults(tpe)
      val constructorParametersByName = constructorParameters.map { case (ts, default) => ts.name.toString -> default }
      val defaults = caseAccs.flatMap(c => constructorParametersByName.get(c.name.toString).flatten.map(v => c -> v)).toMap
      Some((tpe, caseAccs, defaults))
    }
  }
}
