package com.xedom.beanstalkj.local;

import java.util.List;
import java.util.Map;

import com.xedom.beanstalkj.protocol.error.DeadlineSoonException;

public class BTClient {

    private BTServer server;

    public BTClient(BTServer server) {
        this.server = server;
    }

    public boolean bury(long jobId, long priority) {
        return server.bury(this, jobId, priority);
    }

    public boolean delete(long jobId) {
        return server.delete(jobId);
    }

    public String getClientVersion() {
        return server.getClientVersion();
    }

    public int ignore(String tubeName) {
        return server.ignore(this, tubeName);
    }

    public int kick(int count) {
        return server.kick(this, count);
    }

    public String listTubeUsed() {
        return server.listTubeUsed(this);
    }

    public List<String> listTubes() {
        return server.listTubes();
    }

    public List<String> listTubesWatched() {
        return server.listTubesWatched(this);
    }

    public BTJob peek(long jobId) {
        return server.peek(jobId);
    }

    public BTJob peekBuried() {
        return server.peekBuried(this);
    }

    public BTJob peekDelayed() {
        return server.peekDelayed(this);
    }

    public BTJob peekReady() {
        return server.peekReady(this);
    }

    public long put(long priority, int delaySeconds, int timeToRun, byte[] data) {
        return server.put(this, priority, delaySeconds, timeToRun, data);
    }

    public boolean release(long jobId, long priority, int delaySeconds) {
        return server.release(jobId, priority, delaySeconds);
    }

    public BTJob reserve(Integer timeoutSeconds) throws DeadlineSoonException {
        return server.reserve(this, timeoutSeconds);
    }

    public BTJob reserve() throws DeadlineSoonException {
        return server.reserve(this);
    }

    public Map<String, String> stats() {
        return server.stats();
    }

    public Map<String, String> statsJob(long jobId) {
        return server.statsJob(jobId);
    }

    public Map<String, String> statsTube(String tubeName) {
        return server.statsTube(tubeName);
    }

    public boolean touch(long jobId) {
        return server.touch(this, jobId);
    }

    public void useTube(String tubeName) {
        server.useTube(this, tubeName);
    }

    public int watch(String tubeName) {
        return server.watch(this, tubeName);
    }

}
