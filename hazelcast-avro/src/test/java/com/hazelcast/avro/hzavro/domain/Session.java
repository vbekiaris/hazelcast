package com.hazelcast.avro.hzavro.domain;

public class Session {
    private long createdOn;
    private long expiresOn;

    public Session() {
    }

    public Session(long createdOn, long expiresOn) {
        this.createdOn = createdOn;
        this.expiresOn = expiresOn;
    }

    public long getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(long createdOn) {
        this.createdOn = createdOn;
    }

    public long getExpiresOn() {
        return expiresOn;
    }

    public void setExpiresOn(long expiresOn) {
        this.expiresOn = expiresOn;
    }
}
