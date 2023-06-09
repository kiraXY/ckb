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

package com.example.ckb.esclient.requests.search.aggregation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@JsonSerialize(using = TermsAggregation.Serializer.class)
public final class TermsAggregation extends Aggregation {
    private final String name;
    private final String field;
    private final BucketOrder order;
    private final Integer size;
    private final ImmutableMap<String, Aggregation> aggregations;

    @Override
    public String name() {
        return name;
    }

    static final class Serializer extends JsonSerializer<TermsAggregation> {
        @Override
        public void serialize(final TermsAggregation value, final JsonGenerator gen,
                              final SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            {
                gen.writeFieldName("terms");
                gen.writeStartObject();
                {
                    gen.writeStringField("field", value.getField());
                    if (value.getSize() != null) {
                        gen.writeNumberField("size", value.getSize());
                    }
                    if (value.getOrder() != null) {
                        writeOrder(value, gen);
                    }
                }
                gen.writeEndObject();

                if (value.getAggregations() != null && !value.getAggregations().isEmpty()) {
                    gen.writeObjectField("aggregations", value.getAggregations());
                }
            }
            gen.writeEndObject();
        }

        private void writeOrder(final TermsAggregation value,
                                final JsonGenerator gen) throws IOException {
            gen.writeFieldName("order");
            gen.writeStartObject();
            {
                gen.writeStringField(
                    value.getOrder().getPath(),
                    value.getOrder().isAsc() ? "asc" : "desc"
                );
            }
            gen.writeEndObject();
        }
    }
}
