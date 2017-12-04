/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

public interface LocalRecordStoreStats {

    /**
     * Returns the number of hits (reads) of the locally owned entries of this partition,
     * since this record store was created.
     *
     * @return number of hits (reads) of the locally owned entries of this partition.
     */
    long getHits();

    /**
     * Returns the number of misses (reads without a mapping) on keys owned by
     * this record store since it was created. Reading a key without a mapping
     * will cause a miss even when that key's value is loaded from a configured
     * {@link com.hazelcast.core.MapLoader} and returned from the call.
     *
     * The number of misses is not migrated, so when a partition has a new owner member,
     * the respective {@link com.hazelcast.map.impl.recordstore.RecordStore}'s
     * {@code misses} statistics will be reset to 0.
     *
     * @return number of misses of the locally owned keys of this partition.
     */
    long getMisses();

    /**
     * Returns the last access (read) time of the locally owned entries of this partition.
     *
     * @return last access (read) time of the locally owned entries of this partition.
     */
    long getLastAccessTime();

    /**
     * Returns the last update time of the locally owned entries of this partition.
     *
     * @return last update time of the locally owned entries of this partition.
     */
    long getLastUpdateTime();

    /**
     * Increases the number of hits of the locally owned entries of this partition.
     */
    void increaseHits();

    /**
     * Increases the number of hits of the locally owned entries of this partition.
     */
    void increaseHits(long hits);

    /**
     * Decreases the number of hits of the locally owned entries of this partition.
     */
    void decreaseHits(long hits);

    /**
     * Increases the number of misses of the locally owned keys of this partition.
     */
    void increaseMisses();

    /**
     * Increases the number of misses of the locally owned keys of this partition.
     */
    void increaseMisses(long misses);

    /**
     * Increases the number of hits of the locally owned entries of this partition.
     */
    void setLastAccessTime(long time);

    /**
     * Increases the number of hits of the locally owned entries of this partition.
     */
    void setLastUpdateTime(long time);
}
