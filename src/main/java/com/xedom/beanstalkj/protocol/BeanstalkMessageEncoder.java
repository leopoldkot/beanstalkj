package com.xedom.beanstalkj.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class BeanstalkMessageEncoder extends SimpleChannelHandler {

    private MessageParser parser = new MessageParser();

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {

        BeanstalkMessage msg = (BeanstalkMessage) e.getMessage();

        ChannelBuffer buf = ChannelBuffers.dynamicBuffer(e.getChannel().getConfig().getBufferFactory());

        String headerLine = compileLine(msg);

        // check ourselves
        parser.parseHeader(headerLine);

        // System.out.println("<<< " + headerLine);
        buf.writeBytes(headerLine.getBytes("ASCII"));
        buf.writeBytes(BeanstalkCodecUtil.CRLF);

        if (msg.getContentLength() >= 0) {
            buf.writeBytes(msg.getContent());
            buf.writeBytes(BeanstalkCodecUtil.CRLF);
        }

        Channels.write(ctx, e.getFuture(), buf);
    }

    private String compileLine(BeanstalkMessage msg) {
        String line = msg.getCommand();

        if (msg.getArgs() != null) {
            for (int i = 0; i < msg.getArgs().length; i++) {
                line += " " + msg.getArgs()[i];
            }
        }

        if (msg.getContentLength() >= 0) {
            line += " " + msg.getContentLength();
        }

        return line;
    }

}
