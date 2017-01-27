package fh.flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09


object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

        val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "ec2-35-161-228-158.us-west-2.compute.amazonaws.com:9092")
    kafkaProps.setProperty("zookeeper.connect", "ec2-35-161-228-158.us-west-2.compute.amazonaws.com:2181")
    kafkaProps.setProperty("group.id", "org.apache.flink")

    // get input data
    val text = env
                .addSource(new FlinkKafkaConsumer09[String](
      "my-topic",
      new SimpleStringSchema(),
      kafkaProps
    )).print

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // execute and print result
    counts.print()

  }
}
