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

package com.hazelcast.test.starter;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.apache.http.HttpStatus.SC_OK;

public class Downloader {
    private static final String MEMBER_URL = "https://repo1.maven.org/maven2/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s.jar";
    private static final String MEMBER_TESTS_URL =
            "https://repo1.maven.org/maven2/com/hazelcast/hazelcast/%1$s/hazelcast-%1$s-tests.jar";
    private static final String CLIENT_URL =
            "https://repo1.maven.org/maven2/com/hazelcast/hazelcast-client/%1$s/hazelcast-client-%1$s.jar";
    private static final String CLIENT_TESTS_URL =
            "https://repo1.maven.org/maven2/com/hazelcast/hazelcast-client/%1$s/hazelcast-client-%1$s-tests.jar";

    public static File[] downloadVersion(String version, File target) {
        File[] files = new File[2];
        files[0] = downloadMember(version, target);
        files[1] = downloadClient(version, target);
        return files;
    }

    public static File[] downloadVersionWithTests(String version, File target) {
        File[] files = new File[4];
        files[0] = downloadMember(version, target);
        files[1] = downloadClient(version, target);
        files[2] = downloadMemberTests(version, target);
        files[3] = downloadClientTests(version, target);
        return files;
    }

    private static File downloadClient(String version, File target) {
        String url = constructUrlForClient(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static File downloadMember(String version, File target) {
        String url = constructUrlForMember(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static File downloadMemberTests(String version, File target) {
        String url = constructUrlForMemberTests(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static File downloadClientTests(String version, File target) {
        String url = constructUrlForClientTests(version);
        String filename = extractFilenameFromUrl(url);
        return downloadFile(url, target, filename);
    }

    private static String extractFilenameFromUrl(String url) {
        int lastIndexOf = url.lastIndexOf('/');
        return url.substring(lastIndexOf);
    }

    private static File downloadFile(String url, File targetDirectory, String filename) {
        CloseableHttpClient client = HttpClients.createDefault();
        File targetFile = new File(targetDirectory, filename);
        if (targetFile.isFile() && targetFile.exists()) {
            return targetFile;
        }
        HttpGet request = new HttpGet(url);
        try {
            CloseableHttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw new GuardianException("Cannot download file from " + url + ", http response code: "
                                + response.getStatusLine().getStatusCode());
            }
            HttpEntity entity = response.getEntity();
            FileOutputStream  fos = new FileOutputStream(targetFile);
            entity.writeTo(fos);
            return targetFile;
        } catch (IOException e) {
            throw Utils.rethrow(e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private static String constructUrlForClient(String version) {
        return String.format(CLIENT_URL, version);
    }

    private static String constructUrlForMember(String version) {
        return String.format(MEMBER_URL, version);
    }

    private static String constructUrlForClientTests(String version) {
        return String.format(CLIENT_TESTS_URL, version);
    }

    private static String constructUrlForMemberTests(String version) {
        return String.format(MEMBER_TESTS_URL, version);
    }
}
