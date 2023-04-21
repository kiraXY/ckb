package com.example.ckb.esclient.requests.factory;

import com.example.ckb.esclient.requests.search.Delete;
import com.example.ckb.esclient.requests.search.SearchParams;
import com.linecorp.armeria.common.HttpRequest;

public interface DeleteFactory {
    HttpRequest delete(Delete delete, SearchParams params, String... index);
}
