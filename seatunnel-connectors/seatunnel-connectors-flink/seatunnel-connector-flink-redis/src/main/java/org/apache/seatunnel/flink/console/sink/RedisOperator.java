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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Redis operator.
 *
 * @see <a href="https://bahir.apache.org/docs/flink/current/flink-streaming-redis/">Flink Redis Connector</a>
 */
public class RedisOperator implements RedisMapper<Tuple2<String, String>> {

    private String dataType;

    private String key;

    public RedisOperator(String dataType, String key) {
        this.dataType = dataType;
        this.key = key;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        if (StringUtils.equals(dataType, "STRING")) {
            return new RedisCommandDescription(RedisCommand.SET, key);
        }
        if (StringUtils.equals(dataType, "LIST")) {
            return new RedisCommandDescription(RedisCommand.LPUSH, key);
        }
        if (StringUtils.equals(dataType, "HASH")) {
            return new RedisCommandDescription(RedisCommand.HSET, key);
        }
        if (StringUtils.equals(dataType, "SET")) {
            return new RedisCommandDescription(RedisCommand.SADD, key);
        }
        if (StringUtils.equals(dataType, "SORTED_SET")) {
            return new RedisCommandDescription(RedisCommand.ZADD, key);
        }
        if (StringUtils.equals(dataType, "PUBSUB")) {
            return new RedisCommandDescription(RedisCommand.PUBLISH, key);
        }
        if (StringUtils.equals(dataType, "HYPER_LOG_LOG")) {
            return new RedisCommandDescription(RedisCommand.PFADD, key);
        }
        throw new RuntimeException("");
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
