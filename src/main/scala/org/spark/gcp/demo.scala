package org.spark.gcp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.pubsub.SparkGCPCredentials
import java.nio.charset.StandardCharsets
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.PubsubUtils

object demo {
  
  
  def main(args:Array[String]){
    
//    val sc =new SparkContext(sparkConf)
//    val r1= sc.range(1,100)
//    r1.collect.foreach(println)
//    sc.stop()
    // Create Spark context
    val projectID= "sharp-matter-286015"
//    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
//      () => createContext(projectID))
    val sparkConf = new SparkConf()
    sparkConf.setAppName("TrendingHashtags")
    sparkConf.set("spark.master","local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    
//    val yarnTags = sparkConf.get("spark.yarn.tags")
//    val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    //ssc.checkpoint(checkpointDirectory + '/' + jobId)
    // Create stream
    val messagesStream: DStream[String] = PubsubUtils.createStream(ssc,
        projectID,
        None,
        "Sub2topic1",  // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
    // Start streaming until we receive an explicit termination
    messagesStream.print()
    
    ssc.start()
    
    
    ssc.awaitTermination()
//    if (totalRunningTime.toInt == 0) {
//      ssc.awaitTermination()
//    }
//    else {
//      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
//    }
    
  }
}