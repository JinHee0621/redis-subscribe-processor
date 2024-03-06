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

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {
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


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("All created FlowFiles are routed to this relationship")
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
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
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
        try {
            String redisHost = context.getProperty(HOST_NUM).getValue();
            int redisPort = Integer.parseInt(context.getProperty(PORT).getValue());
            String channelNm = context.getProperty(CHANNEL).getValue();

            JedisPooled jedis = new JedisPooled(redisHost, redisPort);
            ExecutorService executor = Executors.newFixedThreadPool(1);

            // Redis Session Run
            executor.execute(() -> jedis.subscribe(new MyRedis("onlyOne", session, REL_SUCCESS), channelNm));

        } catch (Exception e) {
            return;
        }

    }
}

class MyRedis extends JedisPubSub {
    private String name;
    private ProcessSession session;
    private Relationship REL_SUCCESS;
    public MyRedis(String name, final ProcessSession session, Relationship REL_SUCCESS) {
        this.name = name;
        this.session = session;
        this.REL_SUCCESS = REL_SUCCESS;
    }

    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.printf(
                "name: %s method: %s channel: %s subscribedChannels: %d\n",
                name, "onSubscribe", channel, subscribedChannels);
        FlowFile flowFile = session.create();
        final String output = "name:"+ name + "method:" + "onUnsubscribe" + "channel: "+channel+" subscribedChannels: %d\n";
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream outputStream) throws IOException {
                IOUtils.write(output, outputStream, "UTF-8");
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.printf(
                "name: %s method: %s channel: %s subscribedChannels: %d\n",
                name, "onUnsubscribe", channel, subscribedChannels);
        FlowFile flowFile = session.create();
        final String output = "name:"+ name + "method:" + "onUnsubscribe" + "channel: "+channel+" subscribedChannels: %d\n";
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream outputStream) throws IOException {
                IOUtils.write(output, outputStream, "UTF-8");
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }

    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        System.out.printf(
                "name: %s method: %s patten: %s subscribedChannels: %d\n",
                name, "onPUnsubscribe", pattern, subscribedChannels);
        FlowFile flowFile = session.create();
        final String output = "name:"+ name + "method:" + "onUnsubscribe" + "pattern: "+pattern+" subscribedChannels: %d\n";
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream outputStream) throws IOException {
                IOUtils.write(output, outputStream, "UTF-8");
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }

    public void onPSubscribe(String pattern, int subscribedChannels) {
        System.out.printf(
                "name: %s method: %s patten: %s subscribedChannels: %d\n",
                name, "onPSubscribe", pattern, subscribedChannels);
        FlowFile flowFile = session.create();
        final String output = "name:"+ name + "method:" + "onUnsubscribe" + "pattern: "+pattern+" subscribedChannels: %d\n";
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream outputStream) throws IOException {
                IOUtils.write(output, outputStream, "UTF-8");
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }

    public void onPong(String message) {
        System.out.printf("name: %s method: %s message: %s\n", name, "onPong", message);
    }
}
