package com.example.ckb.esclient.requests.factory.v6.codec;

import com.example.ckb.esclient.requests.UpdateRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class V6UpdateRequestSerializer extends JsonSerializer<UpdateRequest> {
    @Override
    public void serialize(final UpdateRequest value, final JsonGenerator gen,
                          final SerializerProvider provider) throws IOException {
        gen.setRootValueSeparator(new SerializedString("\n"));

        gen.writeStartObject();
        {
            gen.writeFieldName("update");
            gen.writeStartObject();
            {
                gen.writeStringField("_index", value.getIndex());
                gen.writeStringField("_type", value.getType());
                gen.writeStringField("_id", value.getId());
            }
            gen.writeEndObject();
        }

        gen.writeEndObject();

        gen.writeStartObject();
        {
            gen.writeFieldName("doc");
            gen.writeObject(value.getDoc());
        }
        gen.writeEndObject();
    }
}
