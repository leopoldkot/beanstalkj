package com.xedom.beanstalkj.local.comparator;

import java.util.Comparator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class ReservedJobsComparator implements Comparator<Long> {

    private final ConcurrentMap<Long, Integer> ttrs;
    private final ConcurrentMap<Long, Long> reservedTime;

    public ReservedJobsComparator(ConcurrentMap<Long, Integer> ttrs,
            ConcurrentMap<Long, Long> reservedTime) {
        this.ttrs = ttrs;
        this.reservedTime = reservedTime;

    }

    @Override
    public int compare(Long jobId1, Long jobId2) {
        
        long deadline1 = reservedTime.get(jobId1) + TimeUnit.SECONDS.toNanos(ttrs.get(jobId1));
        long deadline2 = reservedTime.get(jobId2) + TimeUnit.SECONDS.toNanos(ttrs.get(jobId2));

        System.out.println("Compare reserved jobs " + jobId1 + "[" + deadline1
                + "] vs " + jobId2 + "[" + deadline2 + "]");

        int cmp = (int) (deadline1 - deadline2);

        return cmp;
    }

}
