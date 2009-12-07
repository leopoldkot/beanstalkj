package com.xedom.beanstalkj.protocol.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTServer;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

public class ReserveWithTimeoutCommandHandlerTest {

    private BTClient client;

    private CommandHandler handler = new ReserveWithTimeoutCommandHandler();

    @Before
    public void setUp() {
        BTServer server = new BTServer();
        client = new BTClient(server);
        server.connect(client);
    }

    @Test
    public void testSuccess() {

        byte[] bytes = "test data".getBytes();
        long id = client.put(10000, 0, 10, bytes);

        String command = "reserve-with-timeout";
        BeanstalkMessage msg = new BeanstalkMessage(command,
                new String[] { "10" }, -1);
        try {
            BeanstalkMessage resp = handler.handleCommand(client, msg);

            assertEquals("RESERVED", resp.getCommand());
            assertEquals(1, resp.getArgs().length);
            assertEquals(id + "", resp.getArgs()[0]);
            assertEquals(bytes.length, resp.getContentLength());
            Assert.assertArrayEquals(bytes, resp.getContent());

        } catch (BadFormatException e) {
            fail("Unexpected exception");
        }

    }

    @Test
    public void testTimedOut() {

        String command = "reserve-with-timeout";
        BeanstalkMessage msg = new BeanstalkMessage(command,
                new String[] { "1" }, -1);
        try {
            BeanstalkMessage resp = handler.handleCommand(client, msg);

            assertEquals("TIMED_OUT", resp.getCommand());
            assertNull(resp.getArgs());
            assertEquals(-1, resp.getContentLength());
        } catch (BadFormatException e) {
            fail("Unexpected exception");
        }

    }

}
