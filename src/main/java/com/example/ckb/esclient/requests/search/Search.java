package com.example.ckb.esclient.requests.search;

import com.example.ckb.esclient.requests.search.aggregation.Aggregation;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class Search {
    private final Integer from;
    private final Integer size;
    private final Query query;
    private final Sorts sort;
    private final String[] _source;
    private final ImmutableMap<String, Aggregation> aggregations;

    public static SearchBuilder builder() {
        return new SearchBuilder();
    }
}
