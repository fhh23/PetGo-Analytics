package fh.flink

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

import java.util.Properties

import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

	val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "ec2-35-161-228-158.us-west-2.compute.amazonaws.com:9092")
    kafkaProps.setProperty("zookeeper.connect", "ec2-35-161-228-158.us-west-2.compute.amazonaws.com:2181")
    kafkaProps.setProperty("group.id", "flink")
	
	val kafkaConsumer = new FlinkKafkaConsumer09[String](
      my-topic,
      new SimpleStringSchema(),
      kafkaProps
    )
	
    // get input data
    val text = env.addSource(kafkaConsumer)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // execute and print result
    counts.print()

  }
}
