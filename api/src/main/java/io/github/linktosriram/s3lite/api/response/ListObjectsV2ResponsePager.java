package io.github.linktosriram.s3lite.api.response;

import io.github.linktosriram.s3lite.api.client.S3Client;
import io.github.linktosriram.s3lite.api.request.ListObjectsV2Request;

import java.util.*;

public class ListObjectsV2ResponsePager {
    private ListObjectsV2Response lastResponse;
    private final S3Client client;
    private final ListObjectsV2Request.Builder baseRequest;
    private final Queue<S3Object> contents = new ArrayDeque<>();
    private final Queue<CommonPrefix> commonPrefixes = new ArrayDeque<>();

    public ListObjectsV2ResponsePager(S3Client client, ListObjectsV2Request.Builder baseRequest) {
        this.client = client;
        this.baseRequest = baseRequest;
        this.lastResponse = client.listObjectsV2(baseRequest.build());
        contents.addAll(lastResponse.getContents());
        commonPrefixes.addAll(lastResponse.getCommonPrefixes());
    }

    public Iterator<S3Object> getContents() {
        return new Iterator<S3Object>() {
            @Override
            public boolean hasNext() {
                if (lastResponse.getNextContinuationToken() == null) {
                    return contents.isEmpty();
                } else {
                    return true;
                }
            }

            @Override
            public S3Object next() {
                S3Object next = contents.poll();
                if (next == null && lastResponse.getNextContinuationToken() != null) {
                    lastResponse = client.listObjectsV2(baseRequest.continuationToken(lastResponse.getContinuationToken()).build());
                    contents.addAll(lastResponse.getContents());
                    commonPrefixes.addAll(lastResponse.getCommonPrefixes());
                    return next();
                } else {
                    return next;
                }
            }
        };
    }

    public Iterator<CommonPrefix> getCommonPrefixes() {
        return new Iterator<CommonPrefix>() {
            @Override
            public boolean hasNext() {
                if (lastResponse.getNextContinuationToken() == null) {
                    return contents.isEmpty();
                } else {
                    return true;
                }
            }

            @Override
            public CommonPrefix next() {
                CommonPrefix next = commonPrefixes.poll();
                if (next == null && lastResponse.getNextContinuationToken() != null) {
                    lastResponse = client.listObjectsV2(baseRequest.continuationToken(lastResponse.getContinuationToken()).build());
                    contents.addAll(lastResponse.getContents());
                    commonPrefixes.addAll(lastResponse.getCommonPrefixes());
                    return next();
                } else {
                    return next;
                }
            }
        };
    }

    public ListObjectsV2Response getLastResponse() {
        return lastResponse;
    }
}
