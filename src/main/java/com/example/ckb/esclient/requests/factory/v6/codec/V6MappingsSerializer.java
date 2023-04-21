package com.example.ckb.esclient.requests.factory.v6.codec;

import com.example.ckb.esclient.response.Mappings;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class V6MappingsSerializer extends JsonSerializer<Mappings> {

    @Override
    public void serialize(final Mappings value, final JsonGenerator gen,
                          final SerializerProvider serializers)
        throws IOException {
        gen.writeStartObject();
        {
            gen.writeFieldName(value.getType());
            gen.writeStartObject();
            {
                gen.writeObjectField("properties", value.getProperties());
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }
}
