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
import redis.clients.jedis._
import scala.util.Try
import scala.collection.JavaConverters._
import java.util.ArrayList
import org.apache.spark.sql.functions._

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


    //Join campaign data with  the lines and show results
    messages.foreachRDD ( rdd=> {
   
   
    val jed = new Jedis(conf.getString("redis.hostName"),6379)
    jed.auth(conf.getString("redis.pass"))
    jed.select(1)

    val keys = jed.keys("*")
    val key1 = keys.asScala
    val rows = collection.mutable.MutableList[(Promotion)]()
    
 
    for(key<-key1){
                val element = jed.hgetAll(key)
		val row = Promotion(key.toInt, element.get("product"), element.get("promotion"), element.get("campaign"), element.get("coupon_count").toInt, element.get("campaign_target").toInt)
		rows+=(row)
    } 
     
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    import sqlContext.implicits._			

    val ds = sqlContext.createDataset(rows)
    jed.close()

 
    val csvmessage = rdd.map(_._2)
    val transDF = csvmessage.map(x => {

    val tokens = x.split(";")
    
    Transaction(tokens(0), tokens(1), tokens(2), tokens(3),tokens(4),tokens(5), tokens(6))}).toDF() 
    transDF.show()
 
    val result = ds.join(transDF, ds.col("product") === transDF.col("product")).groupBy(ds.col("campaign_id"),ds.col("campaign")).agg(count("*"), sum(ds.col("campaign_target")))
    result.show()
	
    result.foreachPartition( partitionIter=> {
		   	    
		       
    		val jedis = new Jedis(conf.getString("redis.hostName"),6379)
    		jedis.auth(conf.getString("redis.pass"))
    		jedis.select(1)
		   
      		partitionIter.foreach( record=> {
		//update coupon count    
                jedis.hincrBy(record.getInt(0).toString,"coupon_count", record.getLong(2))
		
		})

	jedis.close()

	})   

})

        

    ssc.start()
    ssc.awaitTermination()
  }
}


case class Promotion(campaign_id: Int, product: String, promotion: String, campaign: String, coupon_count: Int, campaign_target: Int) 

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
