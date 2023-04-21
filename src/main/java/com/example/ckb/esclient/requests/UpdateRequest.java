package com.example.ckb.esclient.requests;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@Builder
public final class UpdateRequest {
    private final String index;
    private final String type;
    private final String id;
    private final Map<String, Object> doc;
}
