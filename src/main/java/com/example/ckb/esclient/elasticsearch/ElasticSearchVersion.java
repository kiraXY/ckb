package com.example.ckb.esclient.elasticsearch;


import com.example.ckb.esclient.requests.factory.Codec;
import com.example.ckb.esclient.requests.factory.RequestFactory;
import com.example.ckb.esclient.requests.factory.v6.V6RequestFactory;
import com.example.ckb.esclient.requests.factory.v6.codec.V6Codec;
import com.example.ckb.esclient.requests.factory.v7.V78RequestFactory;
import com.example.ckb.esclient.requests.factory.v7.V7RequestFactory;
import com.example.ckb.esclient.requests.factory.v7.codec.V78Codec;
import com.example.ckb.esclient.requests.factory.v7.codec.V7Codec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ElasticSearchVersion {
    private final String distribution;
    private final int major;
    private final int minor;

    private final RequestFactory requestFactory;
    private final Codec codec;

    private ElasticSearchVersion(final String distribution, final int major, final int minor) {
        this.distribution = distribution;
        this.major = major;
        this.minor = minor;

        if (distribution.equalsIgnoreCase("OpenSearch")) {
            requestFactory = new V78RequestFactory(this);
            codec = V78Codec.INSTANCE;
            return;
        }

        if (distribution.equalsIgnoreCase("ElasticSearch")) {
            if (major == 6) { // 6.x
                requestFactory = new V6RequestFactory(this);
                codec = V6Codec.INSTANCE;
                return;
            }
            if (major == 7) {
                if (minor < 8) { // [7.0, 7.8)
                    requestFactory = new V7RequestFactory(this);
                    codec = V7Codec.INSTANCE;
                } else { // [7.8, 8.0)
                    requestFactory = new V78RequestFactory(this);
                    codec = V78Codec.INSTANCE;
                }
                return;
            }
            if (major == 8) {
                requestFactory = new V78RequestFactory(this);
                codec = V78Codec.INSTANCE;
                return;
            }
        }
        throw new UnsupportedOperationException("Unsupported version: " + this);
    }

    @Override
    public String toString() {
        return distribution + " " + major + "." + minor;
    }

    private static final Pattern REGEX = Pattern.compile("(\\d+)\\.(\\d+).*");

    public static ElasticSearchVersion of(String distribution, String version) {
        final Matcher matcher = REGEX.matcher(version);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse version: " + version);
        }
        final int major = Integer.parseInt(matcher.group(1));
        final int minor = Integer.parseInt(matcher.group(2));
        return new ElasticSearchVersion(distribution, major, minor);
    }

    public RequestFactory requestFactory() {
        return requestFactory;
    }

    public Codec codec() {
        return codec;
    }
}
