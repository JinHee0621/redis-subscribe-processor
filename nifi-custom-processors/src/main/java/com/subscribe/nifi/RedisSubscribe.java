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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        Jedis subscriberJedis = null;
        try {
            String redisHost = context.getProperty(HOST_NUM).getValue();
            int redisPort = Integer.parseInt(context.getProperty(PORT).getValue());
            String channelNm = context.getProperty(CHANNEL).getValue();
            String password = context.getProperty(PASSWORD).getValue();

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            JedisPool jedisPool;
            if(password != null) {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000, password);
            } else {
                jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 1000);
            }
            subscriberJedis = jedisPool.getResource();
            RedisRes subscriber = new RedisRes(subscriberJedis,"onlyOne", session, REL_SUCCESS);
            subscriberJedis.subscribe(subscriber, channelNm);
            subscriberJedis.close();

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
}

class RedisRes extends JedisPubSub {
    private Jedis subscriber;
    private String name;
    private ProcessSession session;
    private Relationship REL_SUCCESS;
    public RedisRes(Jedis subscriber, String name, final ProcessSession session, Relationship REL_SUCCESS) {
        this.subscriber = subscriber;
        this.name = name;
        this.session = session;
        this.REL_SUCCESS = REL_SUCCESS;
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
        // Redis Client Close
        subscriber.close();
        this.unsubscribe();
    }
}
