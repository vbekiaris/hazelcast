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

package test;

import com.hazelcast.core.MapStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryMapStoreSleepAndCount implements MapStore<String, String> {

    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<String, String>();

    private final int msPerLoad;

    private static final AtomicInteger countLoadedKeys = new AtomicInteger(0);

    // ----------------------------------------------------------- construction

    public InMemoryMapStoreSleepAndCount(int msPerLoad) {
        this.msPerLoad = msPerLoad;
    }

    public void preload(int size) {
        for (int i = 0; i < size; i++) {
            store.put("k" + i, "v" + i);
        }
    }

    // ---------------------------------------------------------------- getters

    public int getCountLoadedKeys() {
        return countLoadedKeys.get();
    }

    // ----------------------------------------------------- MapStore interface

    @Override
    public String load(String key) {
        if (msPerLoad > 0) {
            try {
                Thread.sleep(msPerLoad);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        countLoadedKeys.incrementAndGet();
        return store.get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        List<String> keysList = new ArrayList<String>(keys);
        Collections.sort(keysList);

        Map<String, String> result = new HashMap<String, String>();
        for (String key : keys) {

            String value = store.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }

        if (msPerLoad > 0) {
            try {
                Thread.sleep(msPerLoad);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        countLoadedKeys.addAndGet(keys.size());
        return result;
    }

    @Override
    public Set<String> loadAllKeys() {
        Set<String> result = new HashSet<String>(store.keySet());
        List<String> resultList = new ArrayList<String>(result);
        Collections.sort(resultList);
        return result;
    }

    @Override
    public void store(String key, String value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<String, String> map) {
        store.putAll(map);
    }

    @Override
    public void delete(String key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        List<String> keysList = new ArrayList<String>(keys);
        Collections.sort(keysList);
        for (String key : keys) {
            store.remove(key);
        }
    }

}
