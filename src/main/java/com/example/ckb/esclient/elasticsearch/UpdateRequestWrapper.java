package com.example.ckb.esclient.elasticsearch;

import com.example.ckb.esclient.requests.UpdateRequest;
import lombok.Getter;

import java.util.Map;

@Getter
public class UpdateRequestWrapper implements RequestWrapper {
    private final UpdateRequest request;

    public UpdateRequestWrapper(String index, String type, String id,
                                Map<String, Object> source) {
        request = UpdateRequest.builder()
                .index(index)
                .type(type)
                .id(id)
                .doc(source)
                .build();
    }
}
