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

import com.hazelcast.internal.RequiresJdk8;

@RequiresJdk8
public class Jdk8StampedLock implements StampedLock {

    private final java.util.concurrent.locks.StampedLock stampedLock = new java.util.concurrent.locks.StampedLock();

    @Override
    public long readLock() {
        return stampedLock.readLock();
    }

    @Override
    public long writeLock() {
        return stampedLock.writeLock();
    }

    @Override
    public void unlockRead(long stamp) {
        stampedLock.unlockRead(stamp);
    }

    @Override
    public void unlockWrite(long stamp) {
        stampedLock.unlockWrite(stamp);
    }

    @Override
    public long tryOptimisticRead() {
        return stampedLock.tryOptimisticRead();
    }

    @Override
    public boolean validate(long stamp) {
        return stampedLock.validate(stamp);
    }
}
