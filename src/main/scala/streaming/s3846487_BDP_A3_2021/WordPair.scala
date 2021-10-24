package streaming.s3846487_BDP_A3_2021


//https://github.com/ramuramaiah/Spark-Odyssey/blob/master/src/main/scala/spark/odyssey/cooccur/WordPair.scala

case class WordPair(val word: String, val neighbour: String) {
override def toString: String = word + "," + neighbour
}

object WordPair {
  

  
  def apply(input: String): WordPair = {
    val separatedWords = input.toString.split(" ")
    new WordPair(separatedWords.apply(0), separatedWords.apply(1))
    
  }
  
}