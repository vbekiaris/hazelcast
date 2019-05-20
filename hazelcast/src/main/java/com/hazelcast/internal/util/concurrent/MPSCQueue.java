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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.util.concurrent.IdleStrategy;
import org.jctools.queues.MpscArrayQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Multi producer single consumer queue. This queue has a configurable {@link IdleStrategy} so if there is nothing to take,
 * the thread can idle and eventually can do the more expensive blocking. The blocking is especially a concern for the putting
 * thread, because it needs to notify the blocked thread.
 * <p>
 * This MPSCQueue is based on 2 stacks; so the items are put in a reverse order by the putting thread, and by the taking thread
 * they are reversed in order again so that the original ordering is restored. Using this approach, if there are multiple items
 * on the stack, the owning thread can take them all using a single CAS. Once this is done, the owning thread can process them
 * one by one and doesn't need to contend with the putting threads; reducing contention.
 *
 * @param <E> the type of elements held in this collection
 */
public final class MPSCQueue<E> implements BlockingQueue<E> {

    private Thread consumerThread;
    private final MpscArrayQueue<E> queue;

    /**
     * Creates a new {@link MPSCQueue} with the provided {@link IdleStrategy} and consumer thread.
     *
     * @param consumerThread the Thread that consumes the items.
     * @param idleStrategy   the idleStrategy. If null, this consumer will block if the queue is empty.
     * @throws NullPointerException when consumerThread is null.
     */
    public MPSCQueue(Thread consumerThread, IdleStrategy idleStrategy) {
        this.consumerThread = checkNotNull(consumerThread, "consumerThread can't be null");
        this.queue = new MpscArrayQueue<>(100_000);
    }

    /**
     * Creates a new {@link MPSCQueue} with the provided {@link IdleStrategy}.
     *
     * @param idleStrategy the idleStrategy. If null, the consumer will block.
     */
    public MPSCQueue(IdleStrategy idleStrategy) {
        this.queue = new MpscArrayQueue<>(100_000);
    }

    /**
     * Sets the consumer thread.
     * <p>
     * The consumer thread is needed for blocking, so that an offering known which thread
     * to wakeup. There can only be a single consumerThread and this method should be called
     * before the queue is safely published. It will not provide a happens before relation on
     * its own.
     *
     * @param consumerThread the consumer thread.
     * @throws NullPointerException when consumerThread null.
     */
    public void setConsumerThread(Thread consumerThread) {
        this.consumerThread = checkNotNull(consumerThread, "consumerThread can't be null");
    }

    /**
     * {@inheritDoc}.
     * <p>
     * This call is threadsafe; but it will only remove the items that are on the put-stack.
     */
    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public boolean offer(E item) {
        return queue.offer(item);
    }

    @Override
    public E peek() {
        return queue.peek();
    }

    @Override
    public E take()
            throws InterruptedException {
        E item = queue.poll();
        if (item != null) {
            return item;
        }

        while (item == null) {
            if (consumerThread.isInterrupted()) {
                throw new InterruptedException();
            }
            item = queue.poll();
        }

        return item;
    }

    @Override
    public E poll() {
        return queue.poll();
    }

    /**
     * {@inheritDoc}.
     * <p>
     * Best effort implementation.
     */
    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void put(E e) {
        queue.offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        queue.offer(e);
        return true;
    }

    @Override
    public boolean add(E e) {
        return queue.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return queue.remove(o);
    }

    @Override
    public boolean contains(Object o) {
        return queue.contains(o);
    }

    @Override
    public E remove() {
        return queue.remove();
    }

    @Override
    public E element() {
        return queue.element();
    }

    @Override
    public Object[] toArray() {
        return queue.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return queue.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queue.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return queue.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queue.retainAll(c);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

}
