package com.example.ckb.esclient.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
public final class Document {
    private boolean found;

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_source")
    private Map<String, Object> source;
}
