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

import com.hazelcast.core.HazelcastInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;


public class HazelcastProxyFactory {
    public static HazelcastInstance proxy(Object hazelcastDelegate) {
        Class<HazelcastInstance> expectedInterface = HazelcastInstance.class;
        ClassLoader targetClassloader = HazelcastProxyFactory.class.getClassLoader();
        HazelcastInstance proxy = generateProxyForInterface(hazelcastDelegate, targetClassloader, expectedInterface);
        return proxy;
    }

    public static <T> T generateProxyForInterface(Object delegate, ClassLoader proxyTargetClassloader, Class<?>...expectedInterfaces) {
        if (!checkImplementInterfaces(delegate, expectedInterfaces)) {
            throw new GuardianException("Cannot create proxy for class " + delegate);
        }
        InvocationHandler myInvocationHandler = new ProxyInvocationHandler(delegate);
        return (T) Proxy.newProxyInstance(proxyTargetClassloader, expectedInterfaces, myInvocationHandler);
    }

    private static boolean checkImplementInterfaces(Object o, Class<?>...ifaces) {
//        for (Class<?> iface : ifaces) {
//            if (!checkImplementInterface(o, iface)) {
//                return false;
//            }
//        }
        return true;
    }

    private static boolean checkImplementInterface(Object o, Class<?> iface) {
        Class<?>[] interfaces = o.getClass().getInterfaces();
        for (Class<?> current : interfaces) {
            if (current.getName().equals(iface.getName())) {
                return true;
            }
        }
        return false;
    }

}
