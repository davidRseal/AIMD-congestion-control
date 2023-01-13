package com.dave.congestion;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: There are definitely stability issues.
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
    private final AtomicBoolean throttleMode = new AtomicBoolean(false);
    private final AtomicBoolean throttleRecoveryMode = new AtomicBoolean(true);
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
        initializeThreads(initialCount);

        while (!isFinished()) {
            if (throttleRecoveryMode.get()) {
                initializeThreads(numConsumersTarget.get());
                throttleRecoveryMode.set(false);
            }
            Thread.sleep(1);
        }
        cleanUpThreads();

        return taskResponses;
    }

    private void initializeThreads(int numThreads) {
        for (int i = 0; i < numThreads; i++) {
            Thread newThread = new Thread(new Consumer<>(this), "Thread-" + consumerThreads.size());
            consumerThreads.add(newThread);
            newThread.start();
        }
    }

    private void cleanUpThreads() {
        for (Thread thread : consumerThreads) {
            thread.interrupt();
        }
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
        throttleMode.set(true);
        int currentTarget = numConsumersTarget.get();
        int newTarget = (int) (currentTarget * decreaseCoefficient);
        // throttle only if there will be at least 1 consumer
        if (newTarget >= 1) {
            numConsumersTarget.set(newTarget);
            numTimesThrottled.getAndIncrement();
        }
    }

    protected void requeueTask(Callable<T> task) {
        tasks.add(task);

        if (shouldThrottle()) {
            return;
        }

        numErroredSinceLastThrottle.getAndIncrement();
        if (numErroredSinceLastThrottle.get() >= throttleThreshold) {
            numErroredSinceLastThrottle.set(0);
            throttleConsumers();
        }
    }

    protected Callable<T> consumeTask() {
        if (tasks.size() > 0) {
            if (shouldThrottle()) {

                // If there's only 1 left and it's about to be deleted it's time to enter recovery mode
                if (consumerThreads.size() == 1) {
                    throttleMode.set(false);
                    throttleRecoveryMode.set(true);
                }

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
        return throttleMode.get();
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
