package com.example.ckb.esclient.requests.search;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class Delete {
    private final Query query;
    public static DeleteBuilder builder() {
        return new DeleteBuilder();
    }
}
