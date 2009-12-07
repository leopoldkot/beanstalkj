package com.xedom.beanstalkj.protocol.error;

import java.io.IOException;

@SuppressWarnings("serial")
public class UnknownCommandException extends IOException {

    private String receivedCommandLine;

    public UnknownCommandException(String msg, String receivedCommandLine) {
        super(msg);
        this.receivedCommandLine = receivedCommandLine;
    }

    public String getReceivedCommandLine() {
        return receivedCommandLine;
    }

}
