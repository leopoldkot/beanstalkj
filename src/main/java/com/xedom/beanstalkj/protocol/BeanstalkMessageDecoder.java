package com.xedom.beanstalkj.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

import com.xedom.beanstalkj.protocol.error.BadFormatException;
import com.xedom.beanstalkj.protocol.error.UnknownCommandException;

public class BeanstalkMessageDecoder extends
        ReplayingDecoder<BeanstalkMessageDecoder.State> {

    private final int maxHeaderSize;
    private final int maxBodySize;
    private BeanstalkMessage message;
    private ChannelBuffer content;
    private static MessageParser parser = new MessageParser();

    protected enum State {
        READ_HEADER, READ_FIXED_LENGTH_CONTENT, READ_CRLF_AFTER_CONTENT;
    }

    public BeanstalkMessageDecoder(int maxHeaderSize, int maxBodySize) {
        super(State.READ_HEADER, true);

        if (maxHeaderSize <= 0) {
            throw new IllegalArgumentException(
                    "maxHeaderSize must be a positive integer: "
                            + maxHeaderSize);
        }

        if (maxBodySize <= 0) {
            throw new IllegalArgumentException(
                    "maxBodySize must be a positive integer: " + maxBodySize);
        }

        this.maxHeaderSize = maxHeaderSize;
        this.maxBodySize = maxBodySize;
    }

    public BeanstalkMessageDecoder() {
        this(1024, 1024 * 10);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
            ChannelBuffer buffer, State state) throws Exception {

        switch (state) {
        case READ_HEADER: {
            String header = readLine(buffer, maxHeaderSize);
            // System.out.println(">>> " + header);

            try {
                message = parser.parseHeader(header);
            } catch (BadFormatException e) {
                return resetWithBadFormat(e);
            } catch (UnknownCommandException uce) {
                return resetWithUnknownCommand(uce);
            }

            if (message.getContentLength() >= 0) {
                checkpoint(State.READ_FIXED_LENGTH_CONTENT);
            } else {
                return reset();
            }
        }

        case READ_FIXED_LENGTH_CONTENT: {
            readFixedLengthContent(buffer);
            checkpoint(State.READ_CRLF_AFTER_CONTENT);
        }

        case READ_CRLF_AFTER_CONTENT: {
            boolean success = readCLRF(buffer);
            if (success) {
                return reset();
            } else {
                return resetWithExpectedCRLF();
            }
        }

        default: {
            throw new Error("Shouldn't reach here.");
        }

        }
    }

    private Object resetWithUnknownCommand(UnknownCommandException e) {
        System.out.println(e.getMessage() + " : " + e.getReceivedCommandLine());

        this.message = null;
        this.content = null;

        checkpoint(State.READ_HEADER);

        return BeanstalkProtocol.UNKNOWN_COMMAND_MSG;
    }

    private boolean readCLRF(ChannelBuffer buffer) {
        ChannelBuffer readBytes = buffer.readBytes(BeanstalkCodecUtil.CRLF.length);
        return (readBytes.getByte(0) == BeanstalkCodecUtil.CRLF[0] && readBytes.getByte(1) == BeanstalkCodecUtil.CRLF[1]);
    }

    private Object resetWithExpectedCRLF() {
        this.message = null;
        this.content = null;
        checkpoint(State.READ_HEADER);
        return BeanstalkProtocol.EXPECTED_CRLF_MSG;
    }

    private Object resetWithBadFormat(BadFormatException e) {
        System.out.println(e.getMessage() + " : " + e.getReceivedCommandLine());

        this.message = null;
        this.content = null;

        checkpoint(State.READ_HEADER);

        return BeanstalkProtocol.BAD_FORMAT_MSG;

    }

    private void readFixedLengthContent(ChannelBuffer buffer) {
        long length = message.getContentLength();
        assert length <= Integer.MAX_VALUE;

        if (content == null) {
            content = buffer.readBytes((int) length);
        } else {
            content.writeBytes(buffer.readBytes((int) length));
        }
    }

    private String readLine(ChannelBuffer buffer, int maxLineLength)
            throws TooLongFrameException {
        StringBuilder sb = new StringBuilder(64);
        int lineLength = 0;
        while (true) {
            byte nextByte = buffer.readByte();
            if (nextByte == BeanstalkCodecUtil.CR) {
                nextByte = buffer.readByte();
                if (nextByte == BeanstalkCodecUtil.LF) {
                    return sb.toString();
                }
            } else if (nextByte == BeanstalkCodecUtil.LF) {
                return sb.toString();
            } else {
                if (lineLength >= maxLineLength) {
                    throw new TooLongFrameException(
                            "An HTTP line is larger than " + maxLineLength
                                    + " bytes.");
                }
                lineLength++;
                sb.append((char) nextByte);
            }
        }
    }

    private Object reset() {
        BeanstalkMessage message = this.message;
        ChannelBuffer content = this.content;

        if (content != null) {
            byte[] dst = new byte[message.getContentLength()];
            content.readBytes(dst);
            message.setContent(dst);
            this.content = null;
        }
        this.message = null;

        checkpoint(State.READ_HEADER);
        return message;
    }
}
