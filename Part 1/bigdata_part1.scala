import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP



object bigdata_part1 {

  trait sentiment_type
  case object negative extends sentiment_type
  case object positive extends sentiment_type

  def get_sentiment(tweet: String): sentiment_type = {

    val sentiment_prop             = new Properties()
    sentiment_prop.setProperty("annotators", "tokenize, split, parse, sentiment")
    val pipelines                       = new StanfordCoreNLP(sentiment_prop)
    val annotation                     = pipelines.process(tweet)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int]         = ListBuffer()

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val row      = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(row)

      sentiments += sentiment.toDouble
      sizes      += sentence.toString.length
    }

    var weight_sentiment = 0.0
    if (sentiments.isEmpty){
      weight_sentiment   = -1.0}
    else {
      weight_sentiment   = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size).sum / sizes.sum }

    weight_sentiment match {
      case s if s <  3 => negative
      case s if s >= 3 => positive
    }

  }

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Arguments must be provided in this order Kafka-Topic API-key API-secret-key Access-token Access-token-secret>")
      System.exit(1)
    }

    val spark = SparkSession.builder.master("local[2]").appName("Spark Streaming with Twitter and Kafka").getOrCreate()
    val sc   = new StreamingContext(spark.sparkContext, Seconds(2))

    // Connecting to Twitter API
    val kafka_Topic        = args(0)
    val consumer_Key       = args(1)
    val consumer_Secret    = args(2)
    val access_Token       = args(3)
    val accessToken_Secret = args(4)

    val conf_build = new ConfigurationBuilder
    conf_build.setDebugEnabled(true)
      .setOAuthConsumerKey(consumer_Key)
      .setOAuthConsumerSecret(consumer_Secret)
      .setOAuthAccessToken(access_Token)
      .setOAuthAccessTokenSecret(accessToken_Secret)

    val newauth    = new OAuthAuthorization(conf_build.build)
    val filter = Seq("covid")

    // Getting tweets
    val tweets         = TwitterUtils.createStream(sc, Some(newauth), filter)
    val english_tweets = tweets.filter(_.getLang() == "en")

    // Processing tweets
    english_tweets.foreachRDD(rdd => {
      rdd.cache()
      val producer_property  = new Properties()
      val bootstrap = "localhost:9092"
      producer_property.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producer_property.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producer_property.put("bootstrap.servers", bootstrap)

      val producer = new KafkaProducer[String, String](producer_property)
      rdd.collect().toList.foreach(tweet => {
        val text      = tweet.getText()
        val sentiment = get_sentiment(text).toString()
        print(text, sentiment)
        val inpt      = new ProducerRecord[String, String](kafka_Topic, text, sentiment)
        producer.send(inpt)
      })

      producer.flush()
      producer.close()
      rdd.unpersist()
    })

    sc.checkpoint("../checkpoint")
    sc.start()
    sc.awaitTermination()

  }

}
