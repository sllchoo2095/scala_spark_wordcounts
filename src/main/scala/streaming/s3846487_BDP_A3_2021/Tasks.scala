package streaming.s3846487_BDP_A3_2021

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.util.Calendar;

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import java.text.SimpleDateFormat

import scala.collection.mutable.ArrayBuffer

//Example http://beginnershadoop.com/2016/04/20/spark-streaming-word-count-example/

//https://stackoverflow.com/questions/33509483/how-to-filter-out-alphanumeric-strings-in-scala-using-regular-expression

object Tasks {

  def main(args: Array[String]) {
    
    def updateFunction(newValues: Seq[(Int)], runningCount: Option[Int]): Option[Int] = {
     val newCount = runningCount.getOrElse(0) +newValues.sum
     
    Some(newCount)
}

    if (args.length < 3) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(3)
      
    }else if (args(2).contentEquals("1")==true){
      
      
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
   
    val ssc = new StreamingContext(sparkConf, Seconds(10)) //Checks the file every 2 seconds for new files in the file

    val lines = ssc.textFileStream(args(0)) //what we are monitoring in the folder
     
     
    lines.foreachRDD(rdd => {
      
      if(!rdd.isEmpty()){

      val words = rdd.flatMap(_.split(" ").filter(x => x.matches("[A-Za-z]+")))

      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)  //(pig, 2) 

      val now = Calendar.getInstance().getTime()//get current time 

      val datetimeFormat = new SimpleDateFormat("dd-MM-yyy_HH-mm-ss")

      val dateTime = datetimeFormat.format(now).toString()

      wordCounts.saveAsTextFile(args(1) + dateTime)
      
      }

    })

    println("Input file == " + args(0))
    println("Output file == " + args(1))

    ssc.start()
    ssc.awaitTermination()
      
    }else if (args(2).contentEquals("2")==true){
      
      
      ///=================================== TASK2 
      
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10)) 

    
    val lines = ssc.textFileStream(args(0)) //what we are monitoring in the folder
    
    lines.foreachRDD{(rdd) => 

     if(!rdd.isEmpty()){
      
       val cMatrix = new ArrayBuffer[(String, Int)]()
       
       //ref  https://stackoverflow.com/questions/4539878/strange-string-split-n-behavior/46288888
       // ref https://github.com/ChenLangChen/Compute-Co-occurrence-Matrix-Saprk-Streaming/blob/master/s3754699_BDP_A2_S2_2020/s3754699_BDP_A2T2_S2_2020/lab9/src/main/scala/streaming/lab9/NetworkWordCount.scala
       
       val lines_array= rdd.map(x=>x.split("[\\r\\n]+")).collect()
       
       lines_array.foreach{
         
         line_array =>
           
           val tokenWords = line_array.flatMap(_.split(" ").filter(x => x.length() >4 & x.matches("[A-Za-z]+")))
           
         
           
           for( i <-0 to  tokenWords.length-1){
             
             for(j<- 0 to tokenWords.length-1){
               
               if(i !=j || tokenWords(i).contentEquals(tokenWords(j))==false){
                 cMatrix +=((tokenWords(i) + "_"+ tokenWords(j), 1))
               }//if
               
             }//j
           }//i
 

       }// for each linesarray 
   
       val rddMatrix = ssc.sparkContext.parallelize(cMatrix)
          val finalMatrix = rddMatrix.reduceByKey(_ + _)
  val now = Calendar.getInstance().getTime()
      val datetimeFormat = new SimpleDateFormat("dd-MM-yyy_HH-mm-ss")

      val dateTime = datetimeFormat.format(now).toString()
      
     
      finalMatrix.saveAsTextFile(args(1) + dateTime)
       
     }//if RDD IS NOT  EMPTY 



    }//LINES FOR EACH RDD. 
    
    
      println("Input file == " + args(0))
    println("Output file == " + args(1))

    ssc.start()
    ssc.awaitTermination()
    
      
    }else if (args(2).contentEquals("3")==true){
      
      
      //ref https://stackoverflow.com/questions/41196826/scala-spark-rdd-combination-in-a-file-to-match-pairs
      
      // https://stackoverflow.com/questions/50696420/scala-count-word-co-occurrence-performance-is-really-low
  
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(20))//Checks the file every 2 seconds for new files in the file
 
    val lines = ssc.textFileStream(args(0))
    
    ssc.checkpoint(".")
    
    val words = lines.map(_.split(" ").filter(x => x.matches("[A-Za-z]+")& x.length()>4).zipWithIndex.combinations(2).toList).flatMap(_.groupBy(word=> (word(0)._1, word(1)._1)).mapValues(_.size))
   val runningCounts = words.updateStateByKey(updateFunction)
 
    val now = Calendar.getInstance().getTime()//get current time 

    val datetimeFormat = new SimpleDateFormat("dd-MM-yyy_HH-mm-ss")

    val dateTime = datetimeFormat.format(now).toString()
    runningCounts.saveAsTextFiles(args(1) + dateTime) 

   
    ssc.start()
    ssc.awaitTermination()
      
      
       
    }else{
      println("ELSE STATEMENT")
      
    }//else
    
 

  }

}
//https://stackoverflow.com/questions/28837908/apache-spark-regex-extract-words-from-rdd

