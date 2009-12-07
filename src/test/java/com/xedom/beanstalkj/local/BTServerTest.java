package com.xedom.beanstalkj.local;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class BTServerTest {

    private BTServer server;

    @Before
    public void setUp() {
        server = new BTServer();
    }

    @Test
    public void testConnect() {

        BTClient client = new BTClient(server);
        server.connect(client);

        String listTubeUsed = server.listTubeUsed(client);
        assertEquals("default", listTubeUsed);

    }

    @Test
    public void testTubesCreation() {

        BTClient client = new BTClient(server);
        server.connect(client);

        String listTubeUsed = server.listTubeUsed(client);
        assertEquals("default", listTubeUsed);

        server.useTube(client, "tube1");
        listTubeUsed = server.listTubeUsed(client);
        assertEquals("tube1", listTubeUsed);

        List<String> listTubesWatched = server.listTubesWatched(client);
        assertEquals(1, listTubesWatched.size());
        assertEquals("default", listTubesWatched.iterator().next());

    }

    @Test
    public void testMultipleClients() throws Exception {
        BTClient client1 = new BTClient(server);
        server.connect(client1);
        BTClient client2 = new BTClient(server);
        server.connect(client2);

        // client1.

    }

    @Test
    public void testPut() throws Exception {

        BTClient client = new BTClient(server);
        server.connect(client);

        byte[] data = new byte[1024];
        client.put(1000, 0, 0, data);
        long t = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            long put = client.put(1000, 0, 0, data);
            // System.out.println(i + " id " + put);
        }
        System.out.println(System.currentTimeMillis() - t + "ms");

    }
}
