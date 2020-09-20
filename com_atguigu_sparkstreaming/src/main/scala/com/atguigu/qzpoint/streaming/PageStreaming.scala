package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.alibaba.fastjson.JSONObject
import com.atguigu.qzpoint.streaming.QzPointStreaming.{groupid, qzQuestionUpdate}
import com.atguigu.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable

object PageStreaming {
  private val groupid = "page_group"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //控制消费速度的参数，意思是每个分区上每秒钟消费的条数
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
           .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //.setMaster("local[*]")
    //第二个参数代表批次时间，即三秒一批数据
    val ssc = new StreamingContext(conf, Seconds(3))

    //用数组存放topic，意思就是我这里可以监控多个topic
    val topics = Array("page_topic")
    //注意Map里面的泛型必须是String，和Object
    val kafkaMap: Map[String, Object] = Map[String, Object](
      //kafka监控地址
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //指定kafka反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者组
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",   //sparkstreaming第一次启动，不丢数
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection//在driver
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        //第一个参数是存放一个StreamingContext
        //第二个参数是消费数据平衡策略，这里用的是均匀消费
        //第三个参数是具体要监控的对象（里面包含topic，kafkaMap，偏移量）
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    //stream原始流无法进行使用和打印，会报序列化错误，所以需要做下面的map转换
    stream.map(item =>item.key())
    val dsStream: DStream[(String, String, String, String, String, String, String)] = stream.map(item => item.value()).filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        val uid: String = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id: String = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id: String = if (jsonObject.containsKey("device_id")) jsonObject.getString("device") else ""
        val ip: String = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id: String = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid: String = if (jsonObject.containsKey("pageid")) jsonObject.getString("pageid") else ""
        val next_page_id: String = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    }).filter(item => {
      !item._5.equals("") && !item._6.equals("") && !item._7.equals("")
    })
    dsStream.cache()
    //对上一页page_id,下一页page_id进行分组求和
    val pageValueDStream: DStream[(String, Int)] = dsStream.map(item=>(item._5+"_"+item._6+"_"+item._7,1))
    val resultDStream: DStream[(String, Int)] = pageValueDStream.reduceByKey(_+_)
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition =>{
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy
        val client: Connection = DataSourceUtil.getConnection
        try {
          partition.foreach { item =>
            calcPageJumpCount(sqlProxy,item,client) //对题库进行更新操作
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  //计算页面跳转个数
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection) = {
    val keys: Array[String] = item._1.split("_")
    val num: Int = item._2
  }
}

