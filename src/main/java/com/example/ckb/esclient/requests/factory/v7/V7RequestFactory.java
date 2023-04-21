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

package com.example.ckb.esclient.requests.factory.v7;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.factory.*;
import com.example.ckb.esclient.requests.factory.common.CommonAliasFactory;
import com.example.ckb.esclient.requests.factory.common.CommonBulkFactory;
import com.example.ckb.esclient.requests.factory.common.CommonDeleteFactory;
import com.example.ckb.esclient.requests.factory.common.CommonSearchFactory;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public final class V7RequestFactory implements RequestFactory {
    private final IndexFactory index;
    private final AliasFactory alias;
    private final DocumentFactory document;
    private final SearchFactory search;
    private final DeleteFactory delete;
    private final BulkFactory bulk;

    public V7RequestFactory(final ElasticSearchVersion version) {
        index = new V7IndexFactory(version);
        alias = new CommonAliasFactory(version);
        document = new V7DocumentFactory(version);
        search = new CommonSearchFactory(version);
        bulk = new CommonBulkFactory(version);
        delete = new CommonDeleteFactory(version);
    }
}
