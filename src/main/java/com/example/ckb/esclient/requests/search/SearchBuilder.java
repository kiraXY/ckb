package com.example.ckb.esclient.requests.search;

import com.example.ckb.esclient.requests.search.aggregation.Aggregation;
import com.example.ckb.esclient.requests.search.aggregation.AggregationBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class SearchBuilder {
    private Integer from;
    private Integer size;
    private String[] _source;
    private QueryBuilder queryBuilder;
    private ImmutableList.Builder<Sort> sort;
    private ImmutableMap.Builder<String, Aggregation> aggregations;

    SearchBuilder() {
    }

    public SearchBuilder from(Integer from) {
        requireNonNull(from, "from");
        checkArgument(from >= 0, "from must be >= 0, but was %s", from);
        this.from = from;
        return this;
    }

    //目前只写包括，未写不包括，待完善
    public SearchBuilder fetchSource(String[] include) {
        requireNonNull(include, "from");
//        checkArgument(from >= 0, "from must be >= 0, but was %s", from);
        this._source = include;
        return this;
    }

    public SearchBuilder size(Integer size) {
        requireNonNull(size, "size");
        checkArgument(size >= 0, "size must be positive, but was %s", size);
        this.size = size;
        return this;
    }

    public SearchBuilder sort(String by, Sort.Order order) {
        checkArgument(!Strings.isNullOrEmpty(by), "by cannot be blank");
        requireNonNull(order, "order");
        sort().add(new Sort(by, order));
        return this;
    }

    public SearchBuilder query(QueryBuilder queryBuilder) {
        checkState(this.queryBuilder == null, "queryBuilder is already set");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder");
        return this;
    }

    public SearchBuilder aggregation(Aggregation aggregation) {
        requireNonNull(aggregation, "aggregation");
        aggregations().put(aggregation.name(), aggregation);
        return this;
    }

    public SearchBuilder aggregation(AggregationBuilder builder) {
        requireNonNull(builder, "builder");
        return aggregation(builder.build());
    }

    public Search build() {
        final Sorts sorts;
        if (sort == null) {
            sorts = null;
        } else {
            sorts = new Sorts(sort.build());
        }

        final ImmutableMap<String, Aggregation> aggregations;
        if (this.aggregations == null) {
            aggregations = null;
        } else {
            aggregations = aggregations().build();
        }
        final Query query;
        if (queryBuilder != null) {
            query = queryBuilder.build();
        } else {
            query = null;
        }
        return new Search(
                from, size, query, sorts, _source, aggregations
        );
    }

    private ImmutableList.Builder<Sort> sort() {
        if (sort == null) {
            sort = ImmutableList.builder();
        }
        return sort;
    }

    private ImmutableMap.Builder<String, Aggregation> aggregations() {
        if (aggregations == null) {
            aggregations = ImmutableMap.builder();
        }
        return aggregations;
    }
}
