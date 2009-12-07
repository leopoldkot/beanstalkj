package com.xedom.beanstalkj.protocol.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTServer;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

public class WatchCommandHandlerTest {

    private BTClient client;

    private CommandHandler handler = new WatchCommandHandler();

    @Before
    public void setUp() {
        BTServer server = new BTServer();
        client = new BTClient(server);
        server.connect(client);
    }

    @Test
    public void testCommand() {

        String command = "watch";
        String[] args = new String[] { "tube1" };
        BeanstalkMessage msg = new BeanstalkMessage(command, args, -1);
        try {
            BeanstalkMessage resp = handler.handleCommand(client, msg);

            assertEquals("WATCHING", resp.getCommand());
            assertEquals(1, resp.getArgs().length);
            assertEquals("2", resp.getArgs()[0]);
            assertEquals(-1, resp.getContentLength());
            assertEquals(null, resp.getContent());

        } catch (BadFormatException e) {
            fail("Unexpected exception");
        }

    }

    // @Test
    // public void testInvalidTubeName() {
    //
    // // checkInvalidTubeName("-tube1");
    // checkInvalidTubeName("tube*");
    // }
    //
    // @Test
    // public void testR() {
    // System.out.println("tube*".matches("^[a-zA-Z0-9-+/.;$()]+$"));
    // }

    private void checkInvalidTubeName(String name) {
        String command = "watch";
        String[] args = new String[] { name };
        BeanstalkMessage msg = new BeanstalkMessage(command, args, -1);
        try {
            BeanstalkMessage resp = handler.handleCommand(client, msg);
            assertEquals("BAD_FORMAT", resp.getCommand());
        } catch (BadFormatException e) {
            // expected
        }
    }

}
