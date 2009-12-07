package com.xedom.beanstalkj.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BTClientTest {

    private BTClient client;

    @Before
    public void setUp() {
        BTServer server = new BTServer();
        client = new BTClient(server);
        server.connect(client);
    }

    @Test
    public void testListTubesWatched() throws Exception {
        List<String> listTubesWatched = client.listTubesWatched();

        assertEquals(1, listTubesWatched.size());
        assertEquals("default", listTubesWatched.iterator().next());

        int watch = client.watch("tube1");
        assertEquals(2, watch);
        assertEquals(2, client.listTubesWatched().size());

    }

    @Test
    public void testPut() throws Exception {

        long id = client.put(0, 0, 1, "test data".getBytes());
        System.out.println(id);

        BTJob job = client.peek(id);
        assertEquals(id, job.getId());

        assertEquals("test data".getBytes().length, job.getData().length);
        Assert.assertArrayEquals("test data".getBytes(), job.getData());

    }

    @Test
    public void testPutDelay() throws Exception {

        long id = client.put(0, 1, 1, "test data".getBytes());
        System.out.println(id);

        BTJob job = client.peekDelayed();
        assertEquals(id, job.getId());

        Thread.sleep(910);
        assertNotNull(client.peekDelayed());
        assertNull(client.peekReady());
        Thread.sleep(100);
        assertNull(client.peekDelayed());
        assertNotNull(client.peekReady());

        assertEquals(id, client.peekReady().getId());
    }

    @Test
    public void testPutPriority() throws Exception {

        long id1 = client.put(20000, 0, 1, "test data".getBytes());
        long id2 = client.put(10000, 0, 1, "test data".getBytes());

        BTJob job2 = client.peekReady();
        assertEquals(id2, job2.getId());

        long id3 = client.put(5000, 0, 1, "test data".getBytes());
        BTJob job3 = client.peekReady();
        assertEquals(id3, job3.getId());

    }

    @Test
    public void testBury() throws Exception {
        long id1 = client.put(20000, 0, 1, "test data".getBytes());
        long id2 = client.put(5000, 0, 1, "test data".getBytes());
        long id3 = client.put(10000, 0, 1, "test data".getBytes());

        BTJob job2 = client.reserve();
        assertEquals(id2, job2.getId());

        client.bury(id2, 3000);
        assertEquals(id2, client.peekBuried().getId());

        BTJob job3 = client.reserve();
        assertEquals(id3, job3.getId());
        client.bury(id3, 7000);

        assertEquals(id2, client.peekBuried().getId());

        int kick = client.kick(1);
        assertEquals(1, kick);

        assertEquals(id3, client.peekBuried().getId());

        kick = client.kick(10);
        assertEquals(1, kick);

        assertNull(client.peekBuried());
    }

    @Test
    public void testRelease() throws Exception {

    }

}
