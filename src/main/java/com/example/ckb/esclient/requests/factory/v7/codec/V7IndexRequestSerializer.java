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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class V7IndexRequestSerializer extends JsonSerializer<IndexRequest> {
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
                gen.writeStringField("_id", value.getId());
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();

        gen.writeObject(value.getDoc());
    }
}
