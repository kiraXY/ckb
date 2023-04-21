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
public final class V78RequestFactory implements RequestFactory {
    private final IndexFactory index;
    private final AliasFactory alias;
    private final DocumentFactory document;
    private final SearchFactory search;
    private final DeleteFactory delete;
    private final BulkFactory bulk;

    public V78RequestFactory(final ElasticSearchVersion version) {
        index = new V7IndexFactory(version);
        alias = new CommonAliasFactory(version);
        document = new V7DocumentFactory(version);
        search = new CommonSearchFactory(version);
        bulk = new CommonBulkFactory(version);
        delete = new CommonDeleteFactory(version);
    }
}
