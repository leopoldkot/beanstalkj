package com.xedom.beanstalkj.protocol.error;

import java.io.IOException;

@SuppressWarnings("serial")
public class BadFormatException extends IOException {

    private String receivedCommandLine;

    public BadFormatException(String msg, String receivedCommandLine) {
        super(msg);
        this.receivedCommandLine = receivedCommandLine;
    }

    public String getReceivedCommandLine() {
        return receivedCommandLine;
    }

}
