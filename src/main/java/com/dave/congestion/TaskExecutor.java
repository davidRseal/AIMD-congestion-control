package com.dave.congestion;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor<T> {
    private final List<Callable<T>> tasks;
    private final List<T> taskResponses = new ArrayList<>();
    private final AtomicInteger numFinished = new AtomicInteger(0);
    private final AtomicInteger numErroredWithoutRetry = new AtomicInteger(0);
    private final List<Class<? extends Exception>> retryableExceptions;
    private final List<Thread> consumerThreads = new ArrayList<>();
    private int initialCount;
    private final int numOriginalTasks;
    private int throttleThreshold;
    private final AtomicInteger numErroredSinceLastThrottle = new AtomicInteger(0);
    private final AtomicInteger numSentInCurrentWindow = new AtomicInteger(0);
    private final AtomicInteger numConsumersTarget = new AtomicInteger();
    private final AtomicInteger numRequestsSent = new AtomicInteger(0);
    private final AtomicInteger numTimesThrottled = new AtomicInteger(0);
    private int increaseNum;
    private double decreaseCoefficient;

    public TaskExecutor(List<Callable<T>> tasks, List<Class<? extends Exception>> retryableExceptions) {
        this.tasks = tasks;
        this.numOriginalTasks = tasks.size();
        this.retryableExceptions = retryableExceptions;

        this.initialCount = 2;
        this.numConsumersTarget.set(initialCount);
        this.throttleThreshold = 1;
        this.increaseNum = 1;
        this.decreaseCoefficient = 0.5;
    }

    public List<T> execute() throws InterruptedException {
        for (int i = 0; i < initialCount; i++) {
            Thread newThread = new Thread(new Consumer<>(this), "Thread-" + consumerThreads.size());
            consumerThreads.add(newThread);
            newThread.start();
        }

        while (!isFinished()) {
            Thread.sleep(1);
        }
        for (Thread thread : consumerThreads) {
            thread.interrupt();
        }

        return taskResponses;
    }

    private void addConsumer() {
        for (int i = 0; i < increaseNum; i++) {
            numConsumersTarget.getAndIncrement();
            Thread newThread = new Thread(new Consumer<>(this));
            consumerThreads.add(newThread);
            newThread.start();
        }
    }

    private void throttleConsumers() {
        int target = numConsumersTarget.get();
        int newTarget = (int) (target * decreaseCoefficient);
        // throttle only if there will be at least 1 consumer and if it's not already throttling
        if (newTarget >= 1 && !shouldThrottle()) {
            numConsumersTarget.set(newTarget);
            numTimesThrottled.getAndIncrement();
        }
    }

    protected void requeueTask(Callable<T> task) {
        numErroredSinceLastThrottle.getAndIncrement();
        if (numErroredSinceLastThrottle.get() >= throttleThreshold) {
            numErroredSinceLastThrottle.set(0);
            throttleConsumers();
        }
        tasks.add(task);
    }

    protected Callable<T> consumeTask() {
        if (tasks.size() > 0) {
            if (shouldThrottle()) {
                // remove this thread from consumerThreads, then return null so this thread returns
                consumerThreads.remove(Thread.currentThread());
                return null;
            }
            return tasks.remove(0); // consume a new task
        }
        return null; // no more tasks to consume; destroy this thread
    }

    protected boolean isFinished() {
        return numFinished.get() == numOriginalTasks;
    }

    protected void addTaskResponse(T response) {
//        System.out.println(Instant.now().toEpochMilli() + ", " + consumerThreads.size());
        taskResponses.add(response);
        numFinished.getAndIncrement();
        numSentInCurrentWindow.getAndIncrement();
        if (shouldAddConsumer()) {
            numSentInCurrentWindow.set(0);
            addConsumer();
        }
    }

    protected void incrementNumErroredWithoutRetry() {
        numFinished.getAndIncrement();
        numErroredWithoutRetry.getAndIncrement();
    }

    protected boolean isRetryableException(Class<? extends Exception> exception) {
        return retryableExceptions.contains(exception);
    }

    protected void incrementNumRequestsSent() {
        numRequestsSent.getAndIncrement();
    }

    private boolean shouldThrottle() {
        return consumerThreads.size() > numConsumersTarget.get();
    }

    private boolean shouldAddConsumer() {
        return numSentInCurrentWindow.get() >= numConsumersTarget.get();
    }

    // Builder functions
    public TaskExecutor<T> initialCount(int initialCount) {
        this.numConsumersTarget.set(initialCount);
        this.initialCount = initialCount;
        return this;
    }

    public TaskExecutor<T> throttleThreshold(int throttleThreshold) {
        this.throttleThreshold = throttleThreshold;
        return this;
    }

    public TaskExecutor<T> increaseNum(int increaseNum) {
        this.increaseNum = increaseNum;
        return this;
    }

    public TaskExecutor<T> decreaseCoefficient(double decreaseCoefficient) {
        this.decreaseCoefficient = decreaseCoefficient;
        return this;
    }

    public int getNumFinished() {
        return numFinished.get();
    }

    public int getNumErroredWithoutRetry() {
        return numErroredWithoutRetry.get();
    }

    public int getNumRequestsSent() {
        return numRequestsSent.get();
    }

    public int getNumTimesThrottled() {
        return numTimesThrottled.get();
    }
}
