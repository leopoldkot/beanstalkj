package com.xedom.beanstalkj.protocol.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTServer;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

public class ListTubesWatchedCommandHandlerTest {

    private BTClient client;

    private CommandHandler handler = new ListTubesWatchedCommandHandler();

    @Before
    public void setUp() {
        BTServer server = new BTServer();
        client = new BTClient(server);
        server.connect(client);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCommand() throws Exception {

        client.watch("aaa");
        client.watch("bbb");

        String command = "list-tubes-watched";
        String[] args = new String[] {};
        BeanstalkMessage msg = new BeanstalkMessage(command, null, -1);
        try {
            BeanstalkMessage resp = handler.handleCommand(client, msg);

            String stats = new String(resp.getContent(), "UTF-8");
            System.out.println(stats);

            Yaml yaml = new Yaml();
            List<String> list = (List<String>) yaml.load(stats);

            assertEquals(3, list.size());

            assertTrue(list.contains("default"));
            assertTrue(list.contains("aaa"));
            assertTrue(list.contains("bbb"));

            assertNull(resp.getArgs());
            assertTrue(resp.getContentLength() > 0);
            assertNotNull(resp.getContent());

        } catch (BadFormatException e) {
            fail("Unexpected exception");
        }

    }
}
