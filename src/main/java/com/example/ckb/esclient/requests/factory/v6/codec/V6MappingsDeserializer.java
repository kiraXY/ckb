package com.example.ckb.esclient.requests.factory.v6.codec;

import com.example.ckb.esclient.response.Mappings;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

final class V6MappingsDeserializer extends JsonDeserializer<Mappings> {
    @Override
    @SuppressWarnings("unchecked")
    public Mappings deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException {

        final Map<String, Object> m =
            p.getCodec().readValue(p, new TypeReference<Map<String, Object>>() {
            });
        final Optional<Map.Entry<String, Object>> typeMapping =
            m.entrySet()
             .stream()
             .filter(it -> it.getValue() instanceof Map)
             .filter(it -> ((Map<String, Object>) it.getValue()).containsKey("properties"))
             .peek(it -> it.setValue(((Map<?, ?>) it.getValue()).get("properties")))
             .findFirst();

        final Optional<Mappings> result = typeMapping.map(it -> {
            final Mappings mappings = new Mappings();
            mappings.setType(it.getKey());
            mappings.setProperties((Map<String, Object>) it.getValue());
            return mappings;
        });
        return result.orElse(null);
    }
}
