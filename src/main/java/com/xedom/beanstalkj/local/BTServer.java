package com.xedom.beanstalkj.local;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.xedom.beanstalkj.local.comparator.ReservedJobsComparator;
import com.xedom.beanstalkj.protocol.error.DeadlineSoonException;

public class BTServer {

    private static final long ONE_SECOND_IN_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private static final String DEFAULT_TUBE = "default";

    private static final int INITIAL_CAPACITY = 10;

    private ConcurrentMap<String, BTTube> tubes = new ConcurrentHashMap<String, BTTube>();

    private ConcurrentMap<BTClient, BTTube> currentTube = new ConcurrentHashMap<BTClient, BTTube>();

    private ConcurrentMap<BTClient, Set<String>> watchList = new ConcurrentHashMap<BTClient, Set<String>>();

    private ConcurrentMap<Long, BTTube> job2tube = new ConcurrentHashMap<Long, BTTube>();

    // in seconds
    private final ConcurrentMap<Long, Integer> ttrs = new ConcurrentHashMap<Long, Integer>();

    // in nanos
    private final ConcurrentMap<Long, Long> reservedTime = new ConcurrentHashMap<Long, Long>();

    // queue of reserved job ids
    private ConcurrentMap<BTClient, PriorityBlockingQueue<Long>> clientReservedJobs = new ConcurrentHashMap<BTClient, PriorityBlockingQueue<Long>>();

    private Comparator<Long> reservedJobsComparator = new ReservedJobsComparator(
            ttrs, reservedTime);

    static AtomicInteger nextId = new AtomicInteger(1);

    public BTServer() {
        tubes.put(DEFAULT_TUBE, new BTTube(DEFAULT_TUBE));
    }

    public void connect(BTClient client) {
        currentTube.put(client, tubes.get(DEFAULT_TUBE));
        HashSet<String> watchedTubeNames = new HashSet<String>();
        watchedTubeNames.add(DEFAULT_TUBE);
        watchList.put(client, watchedTubeNames);

        clientReservedJobs.put(client, new PriorityBlockingQueue<Long>(
                INITIAL_CAPACITY, reservedJobsComparator));

    }

    public boolean bury(BTClient client, long jobId, long priority) {
        return currentTube(client).bury(jobId, priority);
    }

    private BTTube currentTube(BTClient client) {
        return currentTube.get(client);
    }

    public boolean delete(long jobId) {

        BTTube tube = findTubeByJobId(jobId);

        if (tube != null) {
            return tube.delete(jobId);
        } else {
            return false;
        }

    }

    private BTTube findTubeByJobId(long jobId) {
        return job2tube.get(jobId);
    }

    public String getClientVersion() {
        return getClass().getName();
    }

    public int ignore(BTClient client, String tubeName) {
        watchList.get(client).remove(tubeName);
        return watchList.get(client).size();
    }

    public int kick(BTClient client, int count) {
        return currentTube(client).kick(count);
    }

    public String listTubeUsed(BTClient client) {
        return currentTube(client).getName();
    }

    public List<String> listTubes() {
        return new ArrayList<String>(tubes.keySet());
    }

    public List<String> listTubesWatched(BTClient client) {
        return new ArrayList<String>(watchList.get(client));
    }

    public BTJob peek(long jobId) {
        BTTube tube = findTubeByJobId(jobId);

        if (tube != null) {
            return tube.peek(jobId);
        } else {
            return null;
        }

    }

    public BTJob peekBuried(BTClient client) {
        return currentTube(client).peekBuried();
    }

    public BTJob peekDelayed(BTClient client) {
        return currentTube(client).peekDelayed();
    }

    public BTJob peekReady(BTClient client) {
        return currentTube(client).peekReady();
    }

    public long put(BTClient client, long priority, int delaySeconds,
            int timeToRun, byte[] data) {

        BTJob job = new BTJob(nextId.getAndIncrement());

        job2tube.put(job.getId(), currentTube(client));

        job.setData(data);

        currentTube(client).put(job, priority, delaySeconds, timeToRun);

        if (timeToRun < 1) {
            timeToRun = 1;
        }
        ttrs.put(job.getId(), timeToRun);

        return job.getId();
    }

    public boolean release(long jobId, long priority, int delaySeconds) {

        BTTube tube = findTubeByJobId(jobId);

        if (tube != null) {
            return tube.release(jobId, priority, delaySeconds);
        } else {
            return false;
        }

    }

    public BTJob reserve(BTClient client, Integer timeoutSeconds)
            throws DeadlineSoonException {
        Set<String> tubeNames = watchList.get(client);

        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        BTJob job = null;
        long timeToLive = Long.MAX_VALUE;

        PriorityBlockingQueue<Long> reservedJobs = clientReservedJobs.get(client);
        while (true) {

            for (String tubeName : tubeNames) {
                BTTube tube = tubes.get(tubeName);

                job = tube.reserve(10);

                if (job != null) {
                    reservedTime.put(job.getId(), System.nanoTime());
                    reservedJobs.offer(job.getId());
                    return job;
                } else {
                    checkDeadlineSoon(reservedJobs);
                }

                timeToLive = deadline - System.nanoTime();
                if (timeToLive <= 0) {
                    return null;
                }
            }

        }

    }

    private void checkDeadlineSoon(PriorityBlockingQueue<Long> reservedJobs)
            throws DeadlineSoonException {

        System.out.println("Checking for deadline");

        Long jobId = null;
        while ((jobId = reservedJobs.peek()) != null && isJobOutdated(jobId)) {
            reservedJobs.remove(jobId);
            System.out.println("Deleted outdated job " + jobId);
        }

        Long soonestDeadlineJobId = reservedJobs.peek();

        if (soonestDeadlineJobId != null) {
            long remainingTimeInNanos = getDeadlineInNanos(soonestDeadlineJobId)
                    - System.nanoTime();
            System.out.println("soonestDeadline jobId " + soonestDeadlineJobId
                    + " now " + System.nanoTime()
                    + ", remainingTimeInNanos  = " + remainingTimeInNanos
                    + " 1s = " + ONE_SECOND_IN_NANOS);

            if (remainingTimeInNanos > 0) {
                if (remainingTimeInNanos <= ONE_SECOND_IN_NANOS) {
                    throw new DeadlineSoonException();
                }
            } else {
                reservedJobs.remove(soonestDeadlineJobId);
            }
        }
    }

    private long getDeadlineInNanos(Long jobId) {
        return reservedTime.get(jobId)
                + TimeUnit.SECONDS.toNanos(ttrs.get(jobId));
    }

    private boolean isJobOutdated(Long jobId) {
        return getDeadlineInNanos(jobId) < System.nanoTime();
    }

    public Map<String, String> stats() {
        return new HashMap<String, String>();
    }

    public Map<String, String> statsJob(long jobId) {
        return new HashMap<String, String>();
    }

    public Map<String, String> statsTube(String tubeName) {
        return new HashMap<String, String>();
    }

    public boolean touch(BTClient client, long jobId) {

        PriorityBlockingQueue<Long> reservedJobs = clientReservedJobs.get(client);

        if (reservedJobs.remove(jobId)) {
            reservedTime.put(jobId, System.nanoTime());
            reservedJobs.offer(jobId);
            return true;
        } else {
            return false;
        }
    }

    public void useTube(BTClient client, String tubeName) {

        if (!tubes.containsKey(tubeName)) {
            tubes.putIfAbsent(tubeName, new BTTube(tubeName));
        }

        currentTube.put(client, tubes.get(tubeName));
    }

    public int watch(BTClient client, String tubeName) {

        if (!tubes.containsKey(tubeName)) {
            tubes.putIfAbsent(tubeName, new BTTube(tubeName));
        }

        Set<String> watchedTubes = watchList.get(client);

        if (watchedTubes == null) {
            watchedTubes = new HashSet<String>();
            watchList.putIfAbsent(client, watchedTubes);
        }

        watchedTubes.add(tubeName);

        return watchList.get(client).size();
    }

    public BTJob reserve(BTClient client) throws DeadlineSoonException {
        Set<String> tubeNames = watchList.get(client);

        BTJob job = null;
        PriorityBlockingQueue<Long> reservedJobs = clientReservedJobs.get(client);
        while (true) {

            for (String tubeName : tubeNames) {
                BTTube tube = tubes.get(tubeName);

                job = tube.reserve(10);

                if (job != null) {
                    reservedTime.put(job.getId(), System.nanoTime());
                    reservedJobs.offer(job.getId());
                    return job;
                } else {
                    checkDeadlineSoon(reservedJobs);
                }
            }

        }
    }
}
