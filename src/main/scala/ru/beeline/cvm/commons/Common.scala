package ru.beeline.cvm.commons

object Common {

  def cond[T](p: => Boolean, v: T): List[T] = {
    p match {
      case true => v :: Nil
      case false => Nil
    }
  }

}
