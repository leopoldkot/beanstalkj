package com.xedom.beanstalkj.local;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BTTube {

    private static final int INITIAL_CAPACITY = 100;

    private static final Log log = LogFactory.getLog(BTTube.class);

    private final ConcurrentMap<Long, BTJob> jobs = new ConcurrentHashMap<Long, BTJob>();

    private final ConcurrentMap<Long, Long> priorities = new ConcurrentHashMap<Long, Long>();

    private final Comparator<Long> jobsComparator = new JobsPriorityComparator(
            priorities);

    private final PriorityBlockingQueue<Long> ready = new PriorityBlockingQueue<Long>(
            INITIAL_CAPACITY, jobsComparator);

    private final PriorityBlockingQueue<Long> buried = new PriorityBlockingQueue<Long>(
            INITIAL_CAPACITY, jobsComparator);

    private final PriorityBlockingQueue<Long> delayed = new PriorityBlockingQueue<Long>(
            INITIAL_CAPACITY, jobsComparator);

    private final ConcurrentMap<Long, BTJobState> state = new ConcurrentHashMap<Long, BTJobState>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private String name;

    public BTTube(String name) {
        super();
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BTJob reserve(Integer timeoutSeconds) {

        try {
            Long id = ready.poll(timeoutSeconds, TimeUnit.MILLISECONDS);

            if (id != null) {
                state.put(id, BTJobState.RESERVED);
                return jobs.get(id);
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public boolean delete(long jobId) {

        if (isDelayed(jobId) || isBuried(jobId) || isReserved(jobId)) {
            jobs.remove(jobId);
            state.remove(jobId);

            return true;
        } else {
            // throw new IllegalStateException("Can't delete non RESERVED job:"
            // + jobId + ", " + state.get(jobId));
            return false;
        }

    }

    public boolean bury(long jobId, long priority) {
        if (isReserved(jobId)) {
            state.put(jobId, BTJobState.BURIED);
            ready.remove(jobId);
            buried.add(jobId);
            log.debug("Job " + jobId + " is buried");
            return true;
        } else {
            log.debug("Job " + jobId
                    + " can't be buried, because it is not reserved");
            return false;
        }
    }

    private boolean isReserved(long jobId) {
        return BTJobState.RESERVED.equals(state.get(jobId));
    }

    private boolean isDelayed(long jobId) {
        return BTJobState.DELAYED.equals(state.get(jobId));
    }

    private boolean isBuried(long jobId) {
        return BTJobState.BURIED.equals(state.get(jobId));
    }

    public int kick(int count) {
        return buried.drainTo(ready, count);
    }

    public BTJob peek(long jobId) {
        return jobs.get(jobId);
    }

    public BTJob peekBuried() {
        Long id = buried.peek();

        if (id != null) {
            return jobs.get(id);
        } else
            return null;
    }

    public BTJob peekDelayed() {
        Long id = delayed.peek();

        if (id != null) {
            return jobs.get(id);
        } else
            return null;
    }

    public BTJob peekReady() {

        Long id = ready.peek();
        if (id != null) {
            return jobs.get(id);
        } else
            return null;
    }

    public void put(BTJob job, long priority, int delaySeconds, int timeToRun) {
        jobs.put(job.getId(), job);
        priorities.put(job.getId(), priority);
        if (delaySeconds == 0) {
            state.put(job.getId(), BTJobState.READY);
            ready.add(job.getId());
        } else {
            state.put(job.getId(), BTJobState.DELAYED);
            delayed.put(job.getId());

            Runnable command = moveToReadyTask(job.getId());
            scheduler.schedule(command, delaySeconds, TimeUnit.SECONDS);
        }
    }

    private Runnable moveToReadyTask(final long jobId) {
        return new Runnable() {
            @Override
            public void run() {
                delayed.remove(jobId);
                ready.add(jobId);
                state.put(jobId, BTJobState.READY);
            }
        };
    }

    public boolean release(long jobId, long priority, int delaySeconds) {

        if (!exists(jobId) || !isReserved(jobId)) {
            return false;
        }

        priorities.put(jobId, priority);

        if (delaySeconds == 0) {
            state.put(jobId, BTJobState.READY);
            ready.add(jobId);
        } else {
            state.put(jobId, BTJobState.DELAYED);

            Runnable command = moveToReadyTask(jobId);
            scheduler.schedule(command, delaySeconds, TimeUnit.SECONDS);
        }

        return true;
    }

    private boolean exists(long jobId) {
        return jobs.containsKey(jobId);
    }

    public boolean touch(long jobId) {
        return false;
    }

    public BTJob reserve() {
        return reserve(Integer.MAX_VALUE);
    }

    static class JobsPriorityComparator implements Comparator<Long> {

        private final Map<Long, Long> priorities;

        public JobsPriorityComparator(Map<Long, Long> priorities) {
            this.priorities = priorities;
        }

        @Override
        public int compare(Long jobId1, Long jobId2) {

            // System.out.println("Compare jobs " + jobId1 + "["
            // + priorities.get(jobId1) + "] vs " + jobId2 + "["
            // + priorities.get(jobId2) + "]");

            int cmp = (int) (priorities.get(jobId1) - priorities.get(jobId2));

            if (cmp == 0) {
                cmp = (int) (jobId1 - jobId2);
            }

            return cmp;
        }

    }

}
