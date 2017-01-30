import kafka.serializer.StringDecoder
import scredis._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.typesafe.config._
import java.io._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import scala.util.Try




object PriceDataStreaming {
 

   def main(args: Array[String]) {

    val conf = ConfigFactory.load()	

    val brokers = conf.getString("kafka.host")
    val topics = conf.getString("kafka.topic")
    val topicsSet = topics.split(",").toSet
	

    // Create context with 2 second batch interval
    
    val sparkConf = new SparkConf().setAppName("store_data")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    object RedisClient extends Serializable {
  

    val redisHost = conf.getString("redis.hostName")
    val redisPort = conf.getString("redis.port")
    val redisTimeout = 30000

    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, 6379, redisTimeout)
    lazy val hook = new Thread {
      override def run = {

      println("Execute hook thread: " + this)

      pool.destroy()

    }

  }

  sys.addShutdownHook(hook.run)

}



// Get the lines and show results
    messages.foreachRDD ( rdd=> { 
    val csvmessage = rdd.map(_._2)
        // csvmessage.collect().foreach(println)
	csvmessage.foreachPartition( partitionIter=> {
		   
		  
                    val jedis = RedisClient.pool.getResource
                    jedis.auth(conf.getString("redis.pass"))
		   
      		   partitionIter.foreach( record=> {
		    
                    val tokens = record.split(";")
                    jedis.publish("toredis",tokens(1)+tokens(2)+tokens(3)+tokens(4)+tokens(5))
		    println(record)
		}
					
	)   
   })
})

//rdd.foreachPartition(partitionIter => {
        
  //     partitionIter.foreach(rows=>{ 
	
	//val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        //import sqlContext.implicits._
        //rows.toDF().show()

    //     println("====================================================================================================================================="+rows)
        //val lines = rows
        
  /*     /* val ticksDF = lines.map(x => {
                                    
                                 val tokens = x.split(";")
                                    val jedis = RedisClient.pool.getResource
                                    jedis.auth(conf.getString("redis.pass"))
                                    jedis.set(tokens(2),tokens(3)+tokens(4)+tokens(5))
                                    Tick(tokens(0), 1.2, 2)}).toDF()

        val ticks_per_source_DF = ticksDF.groupBy("source")
                                .agg("price" -> "avg", "volume" -> "sum")
                                .orderBy("source")
        //RedisConnection.client.set("currtime-"+, )
        ticksDF.show()
        ticks_per_source_DF.show()
*/   */
// Start the computation
   // })
//})
//}
//rdd.collect()
//rdd.toDF().show()
//}

//messages.print()
    ssc.start()
    ssc.awaitTermination()
  }
}



case class Tick(source: String, price: Double, volume: Int)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
