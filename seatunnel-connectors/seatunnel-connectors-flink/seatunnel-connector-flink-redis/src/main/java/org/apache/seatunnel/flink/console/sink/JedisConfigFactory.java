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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Jedis config factory.
 */
public class JedisConfigFactory {

    private Config config;

    private RedisMode mode;

    public JedisConfigFactory(Config config) {
        this.config = config;
        Optional<RedisMode> optional = RedisMode.getByMode(config.getString("redis"));
        if (optional.isPresent()) {
            throw new IllegalArgumentException("Init Jedis config redis mode is unknown.");
        }
        mode = optional.get();
    }

    public FlinkJedisConfigBase create() {
        switch (mode) {
            case STANDALONE:
            case MASTER_SLAVE:
                return createFlinkJedisPoolConfig();
            case CLUSTER:
                return createFlinkJedisClusterConfig();
            case SENTINEL:
                return createFlinkJedisSentinelConfig();
            default:
                throw new RuntimeException("Init Jedis config redis mode is unknown.");
        }
    }

    private FlinkJedisConfigBase createFlinkJedisPoolConfig() {
        if (!config.hasPath("server")) {
            throw new IllegalArgumentException("Jedis server config must be exist.");
        }
        String server = config.getString("server");
        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        Optional<InetSocketAddress> address = getInetSocketAddress(server);
        if (address.isPresent()) {
            builder.setHost(address.get().getHostName()).setPort(address.get().getPort());
        }
        if (config.hasPath("database")) {
            builder.setDatabase(config.getInt("database"));
        }
        if (config.hasPath("password")) {
            builder.setPassword(config.getString("password"));
        }
        return builder.build();
    }

    private FlinkJedisConfigBase createFlinkJedisClusterConfig() {
        if (!config.hasPath("clusters")) {
            throw new IllegalArgumentException("Jedis clusters config must be exist.");
        }
        List<String> clusters = config.getStringList("clusters");
        FlinkJedisClusterConfig.Builder builder = new FlinkJedisClusterConfig.Builder();
        Set<InetSocketAddress> addressSet = new HashSet<>();
        for (String server : clusters) {
            Optional<InetSocketAddress> address = getInetSocketAddress(server);
            if (address.isPresent()) {
                continue;
            }
            addressSet.add(address.get());
        }
        return builder.setNodes(addressSet).build();
    }

    private Optional<InetSocketAddress> getInetSocketAddress(String server) {
        if (StringUtils.split(server, ":").length > 1) {
            String host = StringUtils.split(server, ":")[0];
            int port = Integer.valueOf(StringUtils.split(server, ":")[1]);
            return Optional.of(new InetSocketAddress(host, port));
        }
        return Optional.empty();
    }

    private FlinkJedisConfigBase createFlinkJedisSentinelConfig() {
        if (!config.hasPath("master")) {
            throw new IllegalArgumentException("Jedis Sentinel master config must be exist.");
        }
        FlinkJedisSentinelConfig.Builder builder = new FlinkJedisSentinelConfig.Builder();
        builder.setMasterName(config.getString("master"));
        if (!config.hasPath("sentinels")) {
            throw new IllegalArgumentException("Jedis sentinels config must be exist.");
        }
        Set<String> sentinels = new HashSet<>(config.getStringList("sentinels"));
        builder.setSentinels(sentinels);
        if (config.hasPath("database")) {
            builder.setDatabase(config.getInt("database"));
        }
        if (config.hasPath("password")) {
            builder.setPassword(config.getString("password"));
        }
        return builder.build();
    }

    public enum RedisMode {

        STANDALONE("standalone"),

        MASTER_SLAVE("master-slave"),

        CLUSTER("cluster"),

        SENTINEL("sentinel");

        RedisMode(String mode) {
            this.mode = mode;
        }

        private String mode;

        public static Optional<RedisMode> getByMode(String mode) {
            for (RedisMode m : RedisMode.values()) {
                if (StringUtils.equals(m.mode, mode)) {
                    return Optional.of(m);
                }
            }
            return Optional.empty();
        }
    }
}
