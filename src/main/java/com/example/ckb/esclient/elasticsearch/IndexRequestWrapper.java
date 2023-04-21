package com.example.ckb.esclient.elasticsearch;

import com.example.ckb.esclient.requests.IndexRequest;
import lombok.Getter;

import java.util.Map;

@Getter
public class IndexRequestWrapper implements RequestWrapper {
    private final IndexRequest request;

    public IndexRequestWrapper(String index, String type, String id,
                               Map<String, ?> source) {
        request = IndexRequest.builder()
                              .index(index)
                              .type(type)
                              .id(id)
                              .doc(source)
                              .build();
    }
}
