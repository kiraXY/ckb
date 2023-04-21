package com.example.ckb.esclient.requests.factory.v6.codec;

import com.example.ckb.esclient.requests.IndexRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class V6IndexRequestSerializer extends JsonSerializer<IndexRequest> {
    @Override
    public void serialize(final IndexRequest value, final JsonGenerator gen,
                          final SerializerProvider provider) throws IOException {
        gen.setRootValueSeparator(new SerializedString("\n"));

        gen.writeStartObject();
        {
            gen.writeFieldName("index");
            gen.writeStartObject();
            {
                gen.writeStringField("_index", value.getIndex());
                gen.writeStringField("_type", value.getType());
                gen.writeStringField("_id", value.getId());
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();

        gen.writeObject(value.getDoc());
    }
}
