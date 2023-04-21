package com.example.ckb.esclient.response;

import lombok.Data;

@Data
public final class NodeInfo {
    @Data
    public static class Version {
        private String distribution = "ElasticSearch";
        private String number;
    }

    private Version version;
}
