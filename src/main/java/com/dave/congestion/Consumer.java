package com.dave.congestion;

import java.util.concurrent.Callable;

public class Consumer<T> implements Runnable {

    final TaskExecutor<T> parent;

    public Consumer(TaskExecutor<T> parent) {
        this.parent = parent;
    }

    @Override
    public void run() {
        Callable<T> currentTask;
        while(true) {
            synchronized (parent) {
                currentTask = parent.consumeTask();
                if (currentTask == null) {
                    return;
                }
                parent.incrementNumRequestsSent();
            }
            try {
                T response = currentTask.call();
                synchronized (parent) {
                    parent.addTaskResponse(response);
                }
            } catch (Exception e) {
                synchronized (parent) {
                    if (parent.isRetryableException(e.getClass())) {
                        parent.requeueTask(currentTask);
                    } else {
                        parent.incrementNumErroredWithoutRetry();
                    }
                }
            }
        }
    }
}
