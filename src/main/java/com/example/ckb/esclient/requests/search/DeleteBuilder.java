package com.example.ckb.esclient.requests.search;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class DeleteBuilder {
    private QueryBuilder queryBuilder;
    DeleteBuilder() {
    }

    public DeleteBuilder query(QueryBuilder queryBuilder) {
        checkState(this.queryBuilder == null, "queryBuilder is already set");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder");
        return this;
    }

    public Delete build() {
        final Query query;
        if (queryBuilder != null) {
            query = queryBuilder.build();
        } else {
            query = null;
        }
        return new Delete(query);
    }

}
