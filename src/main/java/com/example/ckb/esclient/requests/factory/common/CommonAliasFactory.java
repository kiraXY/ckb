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

package com.example.ckb.esclient.requests.factory.common;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.factory.AliasFactory;
import com.google.common.base.Strings;
import com.linecorp.armeria.common.HttpRequest;
import lombok.RequiredArgsConstructor;

import static com.google.common.base.Preconditions.checkArgument;

@RequiredArgsConstructor
public final class CommonAliasFactory implements AliasFactory {
    private final ElasticSearchVersion version;

    @Override
    public HttpRequest indices(String alias) {
        checkArgument(!Strings.isNullOrEmpty(alias), "alias cannot be null or empty");

        return HttpRequest.builder()
                          .get("/_alias/{name}")
                          .pathParam("name", alias)
                          .build();
    }
}
