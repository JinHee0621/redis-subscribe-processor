/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.subscribe.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Tags({"Redis", "PubSub", "Subscribe"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RedisSubscribe extends AbstractProcessor {
    public static final PropertyDescriptor HOST_NUM = new PropertyDescriptor
            .Builder().name("Redis host")
            .displayName("Redis host")
            .description("Input Redis host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Redis port")
            .displayName("Redis port")
            .description("Input Redis port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder().name("Redis channel")
            .displayName("Redis channel")
            .description("Input Redis channel")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("Redis Host Password")
            .displayName("Redis Host Password")
            .description("Input Redis Password")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Can Subscribe Redis Channel")
            .build();

    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("failure")
            .description("Redis Error Occurred")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOST_NUM);
        descriptors.add(PORT);
        descriptors.add(CHANNEL);
        descriptors.add(PASSWORD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    Jedis subscriberJedis = null;
    JedisPool jedisPool = null;
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    ExecutorService subscribers;
    RedisRes subscriber = null;
    @OnScheduled()
    public void onScheduled(final ProcessContext context) {
    }
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        try {
            String redisHost = context.getProperty(HOST_NUM).getValue();
            int redisPort = Integer.parseInt(context.getProperty(PORT).getValue());
            String channelNm = context.getProperty(CHANNEL).getValue();
            String password = context.getProperty(PASSWORD).getValue();

            if(password != null) {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000, password);
            } else {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000);
            }

            subscriberJedis = jedisPool.getResource();
            subscribers = Executors.newSingleThreadExecutor();
            subscriber = new RedisRes(subscriberJedis, session, REL_SUCCESS, subscribers);

            subscribers.submit(() -> {
                        subscriberJedis.subscribe(subscriber, channelNm);
                }
            );

            subscribers.awaitTermination(5000, TimeUnit.MILLISECONDS);

            if(subscribers.isShutdown()) {
                subscribers.shutdownNow();
                subscribers = null;
            }

            if(subscriberJedis.isConnected()) {
                subscriberJedis.disconnect();
                subscriberJedis.close();
                subscriberJedis = null;
            }

        } catch (Exception e) {
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream outputStream) throws IOException {
                    IOUtils.write(e.getMessage(), outputStream, "UTF-8");
                }
            });
            session.transfer(flowFile, REL_FAIL);
            if(subscriberJedis != null) subscriberJedis.close();
        }
    }
    @OnStopped
    public void stopSubscription(final ProcessContext context) {
        if (subscriber != null && subscriber.isSubscribed()) {
            try {
                subscriber.unsubscribe();
                subscribers.awaitTermination(5000, TimeUnit.MILLISECONDS);
                subscribers.shutdownNow();
                subscribers = null;
            } catch (InterruptedException e) {
                getLogger().warn("Unable to cleanly shutdown due to {}", new Object[]{e});
            }
        }
    }
}

class RedisRes extends JedisPubSub {
    private Jedis subscriber;
    private ProcessSession session;
    private Relationship REL_SUCCESS;
    private ExecutorService executor;
    public RedisRes(Jedis subscriber,final ProcessSession session, Relationship REL_SUCCESS, ExecutorService executor) {
        this.subscriber = subscriber;
        this.session = session;
        this.REL_SUCCESS = REL_SUCCESS;
        this.executor = executor;
    }
    @Override
    public void onMessage(String channel, String message) {
        FlowFile flowFile = session.create();
        final String output = "Messsage : " + message ;//"name:"+ name + "method:" + "onUnsubscribe" + "channel: "+channel+" subscribedChannels: %d\n";
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream outputStream) throws IOException {
                IOUtils.write(output, outputStream, "UTF-8");
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        this.unsubscribe();

        // Redis Client Close
        subscriber.disconnect();
        subscriber.close();
        executor.shutdownNow();
    }
}




