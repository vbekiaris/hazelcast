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

package com.hazelcast.internal.util.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DefaultStampedLock implements StampedLock {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    @Override
    public long readLock() {
        readLock.lock();
        return 0;
    }

    @Override
    public long writeLock() {
        writeLock.lock();
        return 0;
    }

    @Override
    public void unlockRead(long stamp) {
        readLock.unlock();
    }

    @Override
    public void unlockWrite(long stamp) {
        writeLock.unlock();
    }

    @Override
    public long tryOptimisticRead() {
        return 0;
    }

    @Override
    public boolean validate(long stamp) {
        return false;
    }
}
