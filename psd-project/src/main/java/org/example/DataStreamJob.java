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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class DataStreamJob {

	private static double getOverrun(Double statValue, Double statRef) {
		return (statRef - statValue)/(1 + statRef);
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<RateOfReturn> source = KafkaSource.<RateOfReturn>builder()
				.setBootstrapServers("localhost:9094")
				.setTopics("A", "B", "C", "D", "E")
				.setValueOnlyDeserializer(new JsonDeserializationSchema<>(RateOfReturn.class))
				.build();

		DataStream<RateOfReturn> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		KafkaSink<PossibleDropAlarm> sink = KafkaSink.<PossibleDropAlarm>builder()
				.setBootstrapServers("localhost:9094")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("Alarm")
						.setValueSerializationSchema(new JsonSerializationSchema<PossibleDropAlarm>())
						.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		stream.keyBy(value -> value.investmentName)
				.flatMap(new RichFlatMapFunction<RateOfReturn, PossibleDropAlarm>() {
					private transient MapState<String, List<RateOfReturn>> mapState;

					private transient Map<String, Double> averageRef = new HashMap<String, Double>() {{
						put("A", 0.0004356);
						put("B", -0.0001445);
						put("C", -0.0007732);
						put("D", -0.0007882);
						put("E", 0.000249);
					}};
					private transient Map<String, Double> quantileRef = new HashMap<String, Double>() {{
						put("A", -0.0799804);
						put("B", -0.0800384);
						put("C", -0.0800072);
						put("D", -0.0799892);
						put("E", -0.0799709);
					}};
					private transient Map<String, Double> avgSmallest10Percent = new HashMap<String, Double>() {{
						put("A", -0.0900045);
						put("B", -0.0900022);
						put("C", -0.0900292);
						put("D", -0.0900108);
						put("E", -0.0899973);
					}};

					@Override
					public void open(Configuration parameters) throws Exception {
						MapStateDescriptor<String, List<RateOfReturn>> descriptor = new MapStateDescriptor<>(
								"myMapState",
								TypeInformation.of(String.class),
								TypeInformation.of(new TypeHint<List<RateOfReturn>>() {})
						);
						mapState = getRuntimeContext().getMapState(descriptor);
						averageRef = new HashMap<String, Double>() {{
							put("A", 0.0004356);
							put("B", -0.0001445);
							put("C", -0.0007732);
							put("D", -0.0007882);
							put("E", 0.000249);
						}};
						quantileRef = new HashMap<String, Double>() {{
							put("A", -0.0799804);
							put("B", -0.0800384);
							put("C", -0.0800072);
							put("D", -0.0799892);
							put("E", -0.0799709);
						}};
						avgSmallest10Percent = new HashMap<String, Double>() {{
							put("A", -0.0900045);
							put("B", -0.0900022);
							put("C", -0.0900292);
							put("D", -0.0900108);
							put("E", -0.0899973);
						}};
					}

					@Override
					public void flatMap(RateOfReturn rateOfReturn, Collector<PossibleDropAlarm> collector) throws Exception {
						String key = rateOfReturn.investmentName;
						Double refAverage = averageRef.get(key);
						Double refQuantile = quantileRef.get(key);
						Double refAverageSmallest10Percent = avgSmallest10Percent.get(key);

						List<RateOfReturn> currentList = mapState.get(key);
						if (currentList == null) {
							currentList = new ArrayList<>();
						}

						currentList.add(rateOfReturn);
						if (currentList.size() > 30) {
							currentList.subList(currentList.size() - 30, currentList.size());
							List<Double> values = currentList.stream().map(rate -> rate.rate).flatMapToDouble(DoubleStream::of).boxed().collect(Collectors.toList());
							List<Double> sortedValues = values.stream().sorted().collect(Collectors.toList());

							OptionalDouble avg = values.stream().mapToDouble(Double::doubleValue).average();

							int index = (int) (0.1 * sortedValues.size());
							double quantile = sortedValues.get(index);

							int cutoffIndex = (int) (0.1 * sortedValues.size());
							OptionalDouble avgOfSmallest10Percent = sortedValues.subList(0, cutoffIndex).stream().mapToDouble(Double::doubleValue).average();

							double averageOverrun = avg.isPresent() ? DataStreamJob.getOverrun(avg.getAsDouble(), refAverage) : 0.0;
							double quantileOverrun = DataStreamJob.getOverrun(quantile, refQuantile);
							double avgSmallest10PercentOverrun = avgOfSmallest10Percent.isPresent() ? DataStreamJob.getOverrun(avgOfSmallest10Percent.getAsDouble(), refAverageSmallest10Percent) : 0.0;

							double overrunThreshold = 0.01;
							PossibleDropAlarm alarm;
							if (averageOverrun > overrunThreshold) {
								alarm = new PossibleDropAlarm(key, rateOfReturn.timestamp, "average");
							} else if (quantileOverrun > overrunThreshold) {
								alarm = new PossibleDropAlarm(key, rateOfReturn.timestamp, "quantile10");
							} else if (avgSmallest10PercentOverrun > overrunThreshold) {
								alarm = new PossibleDropAlarm(key, rateOfReturn.timestamp, "avgSmallest10Percent");
							} else {
								alarm = new PossibleDropAlarm(key, rateOfReturn.timestamp, "");
							}
							collector.collect(alarm);
						} else {
							PossibleDropAlarm alarm = new PossibleDropAlarm(key, rateOfReturn.timestamp, "");
							collector.collect(alarm);
						}
						mapState.put(key, currentList);
					}
				}).filter((FilterFunction<PossibleDropAlarm>) possibleDropAlarm -> !possibleDropAlarm.droppedStatName.equals(""))
				.sinkTo(sink);

		env.execute();
	}
}
