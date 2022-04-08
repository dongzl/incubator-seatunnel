/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.console.sink;

import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/**
 * Redis sink.
 */
public class RedisSink implements FlinkStreamSink {

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Nullable
    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        FlinkJedisConfigBase redisConfig = new JedisConfigFactory(config).create();
        String redisDataType = config.getString("data_type");
        String redisKey = config.getString("key");
        SinkFunction sinkFunction = new org.apache.flink.streaming.connectors.redis.RedisSink(redisConfig, new RedisOperator(redisDataType, redisKey));
        DataStreamSink<Row> result = dataStream.addSink(sinkFunction);
        if (config.hasPath("parallelism")) {
            int parallelism = config.getInt("parallelism");
            result.setParallelism(parallelism);
        }
        return result;
    }
}
