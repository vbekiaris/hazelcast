/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.test;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.starter.Downloader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({NightlyTest.class, ParallelTest.class})
public class DownloaderTest {

    private static final String HAZELCAST_3_6_MEMBER_HASH = "89563f7dab02bd5f592082697c24d167";
    private static final String HAZELCAST_3_6_CLIENT_HASH = "fd6022e35908b42d24fe10a9c9fdaad5";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void downloadVersion() throws IOException {
        File[] files = Downloader.downloadVersion("3.6", folder.getRoot());
        HashFunction md5Hash = Hashing.md5();

        byte[] memberBytes = Files.toByteArray(files[0]);
        HashCode memberHash = md5Hash.hashBytes(memberBytes);
        assertEquals(HAZELCAST_3_6_MEMBER_HASH, memberHash.toString());

        byte[] clientBytes = Files.toByteArray(files[1]);
        HashCode clientHash = md5Hash.hashBytes(clientBytes);
        assertEquals(HAZELCAST_3_6_CLIENT_HASH, clientHash.toString());
    }

}