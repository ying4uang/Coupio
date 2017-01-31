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
import scala.collection.JavaConverters._
import java.util.ArrayList

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

    object RedisClient1 extends Serializable {
  

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

//Get promotion data


    val jed = RedisClient1.pool.getResource
    jed.auth(conf.getString("redis.pass"))
    jed.select(1)


     val keys = jed.keys("*")
     val key1 = keys.asScala
     val rows = collection.mutable.MutableList[(Data)]()
    

 
     for(key<-key1)
	{
		val row = Data(key, jed.get(key))
		rows+=(row)
	} 
     
	
		
     val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
     import sqlContext.implicits._
			
     val ds = sqlContext.createDataset(rows)
     //val ds = sqlContext.createDataset(new ArrayList(keys))
     ds.show()


// Get the lines and show results
    messages.foreachRDD ( rdd=> { 
    val csvmessage = rdd.map(_._2)
   	val transDF = csvmessage.map(x => {
                                  val tokens = x.split(";")
                                  Transaction(tokens(0), tokens(1), tokens(2), tokens(3),tokens(4),tokens(5), tokens(6))}).toDF() 
        transDF.show()

	val result = ds.join(transDF, ds.col("product") === transDF.col("product"))
	result.show()
        
	result.foreachPartition( partitionIter=> {
		   	    
                    val jedis = RedisClient.pool.getResource
                    jedis.auth(conf.getString("redis.pass"))
                    jedis.select(2)
		   
		   
      		    partitionIter.foreach( record=> {
		    
                    jedis.publish("tored",record.getString(1)+record.getString(2))
		}
					
	)
	})   

        
	csvmessage.foreachPartition( partitionIter=> {
		   	    
                    val jedis = RedisClient.pool.getResource
                    jedis.auth(conf.getString("redis.pass"))
                    jedis.select(0)
		   
		   
      		    partitionIter.foreach( record=> {
		    
                    val tokens = record.split(";")
                    jedis.publish("toredis",tokens(1)+tokens(2)+tokens(3)+tokens(4)+tokens(5))
		    println(record)
		}
					
	)   
   })
})

        

    ssc.start()
    ssc.awaitTermination()
  }
}


case class Data(product: String, promotion: String) 

case class Transaction(source: String, time: String, price: String, volume: String, cust: String,  product: String, category: String)

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
