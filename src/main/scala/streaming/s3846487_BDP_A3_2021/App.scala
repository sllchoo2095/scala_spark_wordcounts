package streaming.s3846487_BDP_A3_2021

/**
 * @author ${user.name}
 */

import scala.annotation.switch
import scala.util.Random
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    

  }
  
 

}
