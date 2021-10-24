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

    if (args.length < 3) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(3)
      
    }else if (args(2).contentEquals("1")){
      
      //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10)) //Checks the file every 2 seconds for new files in the file

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0)) //what we are monitoring in the folder

    lines.foreachRDD(rdd => {

      val words = rdd.flatMap(_.split(" ").filter(x => x.matches("[A-Za-z]+")))

      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

      val now = Calendar.getInstance().getTime()//get current time 

      val datetimeFormat = new SimpleDateFormat("dd-MM-yyy_HH-mm-ss")

      val dateTime = datetimeFormat.format(now).toString()

      wordCounts.saveAsTextFile(args(1) + dateTime)

    })

    println("Input file == " + args(0))
    println("Output file == " + args(1))

    ssc.start()
    ssc.awaitTermination()
      
    }else if (args(2).contentEquals("2")){
      
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10)) 

    
    val lines = ssc.textFileStream(args(0)) //what we are monitoring in the folder
    
    lines.foreachRDD((rdd,Time) => {

     if(!rdd.isEmpty()){
       
       val cMatrix = new ArrayBuffer[(String, Int)]()
       
       //ref  https://stackoverflow.com/questions/4539878/strange-string-split-n-behavior/46288888
       
       val lines_array= rdd.map(x=>x.split("[\\r\\n]+")).collect()
       
       lines_array.foreach{
         
         line_array =>
           
           val tokenWords = line_array.flatMap(_.split(" ").filter(x => x.length() >4 & x.matches("[A-Za-z]+")))
           
           //val alphaWords = tokenWords.map(x=> ("[A-Za-z]+"))
           
        
           
          // val longWords= alphaWords.filter(x => x.length() >4)
           
           for( i <-0 to  tokenWords.length-1){
             
             for(j<-tokenWords.length-1 to 0){
               
               if(i !=j || tokenWords(i).contentEquals(tokenWords(j))==false){
                 cMatrix +=((tokenWords(i) + "_"+ tokenWords(j), 1))
               }//if
               
             }//j
           }//i
               
           val rddMatrix = ssc.sparkContext.parallelize(cMatrix)
           val finalMatrix = rddMatrix.reduceByKey(_ + _)
           
               val now = Calendar.getInstance().getTime()//get current time 

      val datetimeFormat = new SimpleDateFormat("dd-MM-yyy_HH-mm-ss")

      val dateTime = datetimeFormat.format(now).toString()
      
      finalMatrix.saveAsTextFile(args(1) + dateTime)
           
       }
       
       
     }

   
      

    })
    
    
      println("Input file == " + args(0))
    println("Output file == " + args(1))

    ssc.start()
    ssc.awaitTermination()
    
      
    }else{
       println("ELSE STATEMENT")
    }
    
 

  }

}
//https://stackoverflow.com/questions/28837908/apache-spark-regex-extract-words-from-rdd

