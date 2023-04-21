package com.example.ckb.esclient.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public final class Mappings {
    @Getter
    @Setter
    @JsonIgnore
    private String type;

    @Getter
    @Setter
    private Map<String, Object> properties = new HashMap<>();
}
