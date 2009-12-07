package com.xedom.beanstalkj.protocol;

public class BeanstalkMessage {

    private String command;

    private String[] args;

    private int contentLength;

    private byte[] content;

    public BeanstalkMessage(String command, String[] args, int contentLength) {
        super();
        this.command = command;
        this.args = args;
        this.contentLength = contentLength;
    }

    public BeanstalkMessage(String commandName) {
        this(commandName, new String[] {}, -1);
    }

    public String getCommand() {
        return command;
    }

    public String[] getArgs() {
        return args;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public int getContentLength() {
        return contentLength;
    }

}
