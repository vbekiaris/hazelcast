/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationPublisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanUtilTest {

    private WanReplicationPublisher publisher1;
    private WanReplicationPublisher publisher2;

    @Before
    public void setup() {
        Set<String> protocols1 = new HashSet<String>(asList(new String[] {"1", "2"}));
        Set<String> protocols2 = new HashSet<String>(asList(new String[] {"2", "3"}));

        publisher1 = Mockito.mock(WanReplicationPublisher.class);
        Mockito.when(publisher1.getSupportedProtocols()).thenReturn(protocols1);

        publisher2 = Mockito.mock(WanReplicationPublisher.class);
        Mockito.when(publisher2.getSupportedProtocols()).thenReturn(protocols2);
    }

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(WanUtil.class);
    }

    @Test
    public void testAllSupportedProtocols() {
        Set<String> allProtocols = WanUtil.allSupportedProtocols(new HashSet(asList(
            publisher1, publisher2
        )));
        assertEquals(3, allProtocols.size());
        assertTrue(allProtocols.contains("1"));
        assertTrue(allProtocols.contains("2"));
        assertTrue(allProtocols.contains("3"));
    }

}
