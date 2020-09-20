package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.atguigu.qzpoint.streaming.RegisterStreaming._
import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable

object QzPointStreaming {
  private val groupid = "qz_point_group"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //控制消费速度的参数，意思是每个分区上每秒钟消费的条数
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    //第二个参数代表批次时间，即三秒一批数据
    val ssc = new StreamingContext(conf, Seconds(3))

    //用数组存放topic，意思就是我这里可以监控多个topic
    val topics = Array("qz_log")
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
    val dsStream = stream.filter(item => item.value().split("\t").length == 6).
      mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val uid = arr(0) //用户id
          val courseid = arr(1) //课程id
          val pointid = arr(2) //知识点id
          val questionid = arr(3) //题目id
          val istrue = arr(4) //是否正确
          val createtime = arr(5) //创建时间
          (uid, courseid, pointid, questionid, istrue, createtime)
        })
      })
    dsStream.foreachRDD(rdd => {
      //在操控mysql之前先聚合rdd，预防多线程安全问题
      //获取相同用户 同一课程 同一知识点的数据
      val groupRdd = rdd.groupBy(item => item._1 + "-" + item._2 + "-" + item._3)
      groupRdd.foreachPartition(partition => {
        //在分区下获取jdbc连接  减少jdbc连接个数 在excuter端执行
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach { case (key, iters) =>
            qzQuestionUpdate(key, iters, sqlProxy, client) //对题库进行更新操作
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          sqlProxy.shutdown(client)
        }
      }
      )
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

  def qzQuestionUpdate(keys: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection)={
    val userid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt

    //同一用户id，课程id，知识点id下的数据总量转换成数组，后期多次使用
    val array = iters.toArray

    //对当前批次的数据下questionid题目id 去重
    val questionids = array.map(_._4).distinct

    //查询历史数据下的 questionid
    var questionids_history: Array[String] = Array()
    sqlProxy.executeQuery(client, "select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
          }
          rs.close() //关闭游标
        }
      })
    //需求2：同一个用户做在同一门课程同一知识点下做题需要去重，并且需要记录去重后的做题id与个数

    //获取到历史数据后再与当前数据进行拼接 去重  spark里union算子是不会去重的
    val resultQuestionid = questionids.union(questionids_history).distinct

    //获取后边统计指标需要的参数，比如用户做题总个数(不去重)，用户做过的questionid字符串，做正确题的个数（不去重），用户做题总数（去重）
    val countSize = resultQuestionid.length   //用户做的题目总个数（去重）
    val resultQuestionid_str = resultQuestionid.mkString(",")   //用户做的题目questionid用逗号拼接成一个字符串存入历史记录表
    val qz_count = questionids.length //获取当前批次下用户的做题个数（去重），（后边没用到）
    var qz_sum = array.length //获取当前批次用户做题总数，array拿的是前面的数据总量array
    var qz_istrue = array.filter(_._5.equals("1")).size //获取当前批次做正确的题个数
    val createtime = array.map(_._6).min //获取最早的创建时间 作为表中创建时间
    //获取当前系统时间，作为表中更新时间
    //SimpleDateFormat线程不安全
    val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())


    //更新qz_point_history 历史记录表 此表用于存当前用户做过的questionid表
    // on duplicate key作用如果说1001_100_1如果说有这条数据回去执行后面update这个修改语句，如果不存在则会执行上面插入语句
    sqlProxy.executeUpdate(client, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      " on duplicate key update questionids=?,updatetime=?", Array(userid, courseid, pointid, resultQuestionid_str, createtime, createtime, resultQuestionid_str, updatetime))

    var qzSum_history = 0
    var istrue_history = 0
    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            qzSum_history += rs.getInt(1)  //求出用户做题总数历史值
            istrue_history += rs.getInt(2)  //求出用户做对的题目数的历史值
          }
          rs.close()
        }
      })
    qz_sum += qzSum_history   //当前批次的做题总数+做题总数历史值=做题总数（不去重）
    qz_istrue += istrue_history  //当前批次的做题正确数+正确数历史值=做题正确总个数（不去重）


    //    需求3：计算知识点正确率 正确率计算公式：做题正确总个数/做题总数 保留两位小数

    //识点正确率 = 做题正确总个数 / 做题总数
    val correct_rate = qz_istrue.toDouble / qz_sum.toDouble

    //    需求4：计算知识点掌握度 去重后的做题个数/当前知识点总题数（已知30题）*当前知识点的正确率
    //计算完成率（知识点掌握度） = 去重后的用户做题个数 / 当前知识点总题数（已知30题）* 当前知识点的正确率
    //假设每个知识点下一共有30道题  先计算用户的做题情况 再计算知识点掌握度
    val qz_detail_rate = countSize.toDouble / 30 //算出做题情况乘以 正确率 得出完成率 假如30道题都做了那么正确率等于知识点掌握度
    val mastery_rate = qz_detail_rate * correct_rate

    //将数据跟新到做题详情表
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))

  }
}
