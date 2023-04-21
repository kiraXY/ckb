
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

public final class V78Codec implements Codec {
    public static final Codec INSTANCE = new V78Codec();

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
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
