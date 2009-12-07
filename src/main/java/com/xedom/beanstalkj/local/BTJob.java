package com.xedom.beanstalkj.local;

public class BTJob {

    private long id;

    private byte[] data;

    public BTJob(long id) {
        this.id = id;
    }

    public byte[] getData() {
        return data;
    }

    public long getId() {
        return id;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

}
