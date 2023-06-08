/*
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

package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<TemperatureReading> source = KafkaSource.<TemperatureReading>builder()
				.setBootstrapServers("localhost:9094")
				.setTopics("Temperatura")
				.setValueOnlyDeserializer(new JsonDeserializationSchema<>(TemperatureReading.class))
				.build();

		DataStream<TemperatureReading> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


		KafkaSink<Alarm> sink = KafkaSink.<Alarm>builder()
				.setBootstrapServers("localhost:9094")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
								.setTopic("Alarm")
								.setValueSerializationSchema(new JsonSerializationSchema<Alarm>())
								.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		stream.filter((FilterFunction<TemperatureReading>) temperatureReading -> temperatureReading.value < 0.0f)
				.map(reading -> new Alarm(reading.timestamp, reading.value))
				.sinkTo(sink);

		env.execute();
	}

	public static class TemperatureReading {
		public String id;
		public Long timestamp;
		public Float value;
		public TemperatureReading() {}

		public TemperatureReading(String id, Long timestamp, Float value) {
			this.id = id;
			this.timestamp = timestamp;
			this.value = value;

		}

		public String toString() {
			return this.id + ": " + this.timestamp + ": value " + this.value.toString();
		}
	}

	public static class Alarm {
		public Long timestamp;
		public Float value;
		public Alarm() {}

		public Alarm(Long timestamp, Float value) {
			this.timestamp = timestamp;
			this.value = value;

		}

		public String toString() {
			return this.timestamp + ": value " + this.value.toString();
		}
	}
}
