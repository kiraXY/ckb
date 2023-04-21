package com.example.ckb.esclient.response;

import lombok.Data;

import java.util.Map;

@Data
public final class Index {
    private Map<String, Object> settings;
    private Mappings mappings;
    private Map<String, Object> aliases;
}
