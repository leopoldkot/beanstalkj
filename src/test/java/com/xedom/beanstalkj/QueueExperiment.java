package com.xedom.beanstalkj;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QueueExperiment {

    private List<ETube> tubes = new ArrayList<ETube>();

    private List<EConsumer> consumers = new ArrayList<EConsumer>();

    private EProducer producer;

    public QueueExperiment() {
        for (int i = 0; i < 5; i++) {
            tubes.add(new ETube("Q" + i));
        }

        producer = new EProducer(tubes);

        EConsumer cons1 = new EConsumer("cons1");
        cons1.add(tubes.get(0));
        cons1.add(tubes.get(1));
        cons1.add(tubes.get(2));

        EConsumer cons2 = new EConsumer("cons2");
        cons2.add(tubes.get(1));
        cons2.add(tubes.get(2));
        cons2.add(tubes.get(3));

        EConsumer cons3 = new EConsumer("cons3");
        cons3.add(tubes.get(4));

        consumers.add(cons1);
        consumers.add(cons2);
        consumers.add(cons3);
    }

    public void startAll() {
        Thread producerThread = new Thread(producer);
        producerThread.setDaemon(true);
        producerThread.start();

        for (EConsumer cons : consumers) {
            EConsumeWorker worker = new EConsumeWorker(cons);
            Thread consumerThread = new Thread(worker);
            consumerThread.setDaemon(true);
            consumerThread.start();
        }

    }

    public void printStats() {

        for (ETube tube : tubes) {
            tube.printStats();
        }

    }

    public static void main(String[] args) throws InterruptedException {
        QueueExperiment ex = new QueueExperiment();

        ex.startAll();

        Thread.sleep(10000);

        ex.printStats();
    }

    static class EConsumer {

        private String name;

        private Set<ETube> watchSet = new HashSet<ETube>();

        public EConsumer(String name) {
            this.name = name;
        }

        public void add(ETube tube) {
            watchSet.add(tube);
        }

        public void remove(ETube tube) {
            watchSet.remove(tube);
        }

        public Long consume() throws InterruptedException, ExecutionException {

            Long o = null;
            while (o == null) {
                for (ETube tube : watchSet) {
                    o = tube.reserveNoWait();

                    if (o != null) {
                        break;
                    }
                }
            }

            return o;
        }

        public Long consume(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException {

            long deadline = System.nanoTime() + unit.toNanos(timeout);

            Long o = null;
            long timeToLive = Long.MAX_VALUE;
            while (o == null) {
                for (ETube tube : watchSet) {
                    o = tube.reserveNoWait();

                    if (o != null) {
                        System.out.println("Consumer " + name + " consumed "
                                + o + " from tube " + tube.name);
                        return o;
                    }

                    timeToLive = deadline - System.nanoTime();
                    if (timeToLive <= 0) {
                        throw new InterruptedException("Wait time exceeded "
                                + timeToLive);
                    }
                }
            }
            return o;

        }
    }

    public static class EOffer {

        private AtomicBoolean taken = new AtomicBoolean(false);

        private AtomicBoolean approved = new AtomicBoolean(false);

        private AtomicBoolean rejected = new AtomicBoolean(false);

        private AtomicReference<Long> offeredValue = new AtomicReference<Long>(
                null);

        public Long take() {
            if (taken.compareAndSet(false, true)) {
                return offeredValue.get();
            }

            return null;
        }

        public void untake() {
            if (!taken.compareAndSet(true, false)) {
                throw new IllegalStateException("Can't untake not taken before");
            }
        }

        public void approve() {
            if (!approved.compareAndSet(false, true)) {
                throw new IllegalStateException("Can't approve more than once");
            }
        }

        public void reject() {
            if (!rejected.compareAndSet(false, true)) {
                throw new IllegalStateException("Can't approve more than once");
            }
        }
    }

    public static class EConsumeWorker implements Runnable {

        private EConsumer cons;

        public EConsumeWorker(EConsumer cons) {
            super();
            this.cons = cons;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Long consume = cons.consume(10, TimeUnit.MILLISECONDS);
                    System.out.println("Consumed " + consume);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    static class ETube {

        private String name;

        private BlockingQueue<Long> queue = new PriorityBlockingQueue<Long>();

        public ETube(String name) {
            this.name = name;
        }

        public void printStats() {
            System.out.println("Queue " + name + " size: " + queue.size());

        }

        public Long reserveNoWait() {
            return queue.poll();
        }

        public Long reserve() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        public void put(Long v) throws InterruptedException {
            queue.put(v);
        }

        public String getName() {
            return name;
        }

    }

    static class EProducer implements Runnable {

        private Random rnd = new Random();

        private List<ETube> tubes;

        public EProducer(List<ETube> tubes) {
            super();
            this.tubes = tubes;
        }

        @Override
        public void run() {

            while (true) {
                int i = (int) (Math.random() * tubes.size());

                Long val = rnd.nextLong();
                try {
                    tubes.get(i).put(val);

                    Thread.sleep(1);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

        }
    }

}
