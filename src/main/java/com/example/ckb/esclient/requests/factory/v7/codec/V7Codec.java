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

package com.example.ckb.esclient.requests.factory.v7.codec;

import com.example.ckb.esclient.requests.IndexRequest;
import com.example.ckb.esclient.requests.UpdateRequest;
import com.example.ckb.esclient.requests.factory.Codec;
import com.example.ckb.esclient.response.Mappings;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.InputStream;

public final class V7Codec implements Codec {
    public static final Codec INSTANCE = new V7Codec();

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        // We added some serializers here and some in their item classes as annotation (e.g.
        // org.apache.skywalking.library.elasticsearch.requests.search.Sorts),
        // the basic idea is, if the item class is very basic and are the same serialization method
        // in both 6.x and 7.x, we set the serializer in their item class as annotation to make it
        // shared by 6.x and 7.x, without duplicating the serializer codes, otherwise, we add
        // serializers for each version explicitly in the object mapper.
        // The 2 methods to add serializers can be changed if some day the basic serializer cannot
        // be shared between newer versions of ElasticSearch or vice versa.
        .registerModule(
            new SimpleModule()
                .addSerializer(
                    IndexRequest.class,
                    new V7IndexRequestSerializer()
                )
                .addSerializer(
                    UpdateRequest.class,
                    new V7UpdateRequestSerializer()
                )
                .addDeserializer(
                    Mappings.class,
                    new V7MappingsDeserializer()
                )
        )
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public byte[] encode(final Object request) throws Exception {
        return MAPPER.writeValueAsBytes(request);
    }

    @Override
    public <T> T decode(final InputStream inputStream,
                        final TypeReference<T> type) throws Exception {
        return MAPPER.readValue(inputStream, type);
    }

    @Override
    public <T> T decode(final InputStream inputStream,
                        final Class<T> clazz) throws Exception {
        return MAPPER.readValue(inputStream, clazz);
    }
}
