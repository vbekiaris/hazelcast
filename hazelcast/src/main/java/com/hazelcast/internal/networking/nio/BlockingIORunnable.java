/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class BlockingIORunnable implements Runnable, OperationHostileThread {

    // WARNING: This value has significant effect on idle CPU usage!
    private static final int SELECT_WAIT_TIME_MILLIS
            = Integer.getInteger("hazelcast.io.select.wait.time.millis", 5000);
    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;
    // When we detect Selector.select returning prematurely
    // for more than SELECT_IDLE_COUNT_THRESHOLD then we rebuild the selector
    private static final int SELECT_IDLE_COUNT_THRESHOLD = 10;
    // for tests only
    private static final Random RANDOM = new Random();
    // when testing, we simulate the selector bug randomly with one out of TEST_SELECTOR_BUG_PROBABILITY
    private static final int TEST_SELECTOR_BUG_PROBABILITY = Integer.parseInt(
            System.getProperty("hazelcast.io.selector.bug.probability", "16"));

    @SuppressWarnings("checkstyle:visibilitymodifier")
    // this field is set during construction and is meant for the probes so that the NioPipeline can
    // indicate which thread they are currently bound to.
    @Probe(name = "ioThreadId", level = ProbeLevel.INFO)
    public int id;

    @Probe(level = INFO)
    volatile long bytesTransceived;
    @Probe(level = INFO)
    volatile long framesTransceived;
    @Probe(level = INFO)
    volatile long priorityFramesTransceived;
    @Probe(level = INFO)
    volatile long processCount;

    @Probe(name = "taskQueueSize")
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
    @Probe
    private final SwCounter eventCount = newSwCounter();
    @Probe
    private final SwCounter selectorIOExceptionCount = newSwCounter();
    @Probe
    private final SwCounter completedTaskCount = newSwCounter();
    // count number of times the selector was rebuilt (if selectWorkaround is enabled)
    @Probe
    private final SwCounter selectorRebuildCount = newSwCounter();

    private final ILogger logger;

    private final ChannelErrorHandler errorHandler;

    // last time select unblocked with some keys selected
    private volatile long lastSelectTimeMs;

    private volatile boolean stop;

    // set to true while testing
    private boolean selectorWorkaroundTest;

    private final String threadName;

    private final NioPipeline pipeline;

    public BlockingIORunnable(String threadName,
                              ILogger logger,
                              NioPipeline pipeline,
                              ChannelErrorHandler errorHandler) {
        this.threadName = threadName;
        this.logger = logger;
        this.pipeline = pipeline;
        this.errorHandler = errorHandler;
    }

    void setSelectorWorkaroundTest(boolean selectorWorkaroundTest) {
        this.selectorWorkaroundTest = selectorWorkaroundTest;
    }

    public long bytesTransceived() {
        return bytesTransceived;
    }

    public long framesTransceived() {
        return framesTransceived;
    }

    public long priorityFramesTransceived() {
        return priorityFramesTransceived;
    }

    public long handleCount() {
        return processCount;
    }

    public long eventCount() {
        return eventCount.get();
    }

    public long completedTaskCount() {
        return completedTaskCount.get();
    }

    /**
     * Returns the total number of selection-key events that have been processed by this thread.
     *
     * @return total number of selection-key events.
     */
    public long getEventCount() {
        return eventCount.get();
    }

    /**
     * A probe that measure how long this NioThread has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    /**
     * Adds a task to this NioThread without notifying the thread.
     *
     * @param task the task to add
     * @throws NullPointerException if task is null
     */
    public void addTask(Runnable task) {
        taskQueue.add(task);
    }

    /**
     * Adds a task to be executed by the NioThread and wakes up the selector so that it will
     * eventually pick up the task.
     *
     * @param task the task to add.
     * @throws NullPointerException if task is null
     */
    public void addTaskAndWakeup(Runnable task) {
        taskQueue.add(task);
    }

    @Override
    public void run() {
        logger.info("Starting NioThread " + id + " on " + Thread.currentThread());
        // This outer loop is a bit complex but it takes care of a lot of stuff:
        // * it calls runSelectNowLoop or runSelectLoop based on selectNow enabled or not.
        // * handles backoff and retrying in case if io exception is thrown
        // * it takes care of other exception handling.
        //
        // The idea about this approach is that the runSelectNowLoop and runSelectLoop are
        // as clean as possible and don't contain any logic that isn't happening on the happy-path.
        try {
            for (; ; ) {
                try {
                    while (!stop) {
                        processTaskQueue();
                        pipeline.process();
                    }
                    // break the for loop; we are done
                    break;
                } catch (EOFException e) {
                    shutdown();
                } catch (IOException nonFatalException) {
                    selectorIOExceptionCount.inc();
                    logger.warning(threadName + " " + nonFatalException.toString(), nonFatalException);
                    coolDown();
                }
            }
        } catch (Throwable e) {
            errorHandler.onError(null, e);
        }

        logger.finest(threadName + " finished");
    }

    /**
     * When an IOException happened, the loop is going to be retried but we need to wait a bit
     * before retrying. If we don't wait, it can be that a subsequent call will run into an IOException
     * immediately. This can lead to a very hot loop and we don't want that. A similar approach is used
     * in Netty
     */
    private void coolDown() {
        try {
            Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
        } catch (InterruptedException i) {
            // if the thread is interrupted, we just restore the interrupt flag and let one of the loops deal with it
            Thread.currentThread().interrupt();
        }
    }

    private boolean processTaskQueue() {
        boolean tasksProcessed = false;
        while (!stop) {
            Runnable task = taskQueue.poll();
            if (task == null) {
                break;
            }
            task.run();
            completedTaskCount.inc();
            tasksProcessed = true;
        }
        return tasksProcessed;
    }

    public void shutdown() {
        pipeline.started = false;
        stop = true;
        taskQueue.clear();
        Thread.currentThread().interrupt();
    }

    @Override
    public String toString() {
        return threadName;
    }
}
