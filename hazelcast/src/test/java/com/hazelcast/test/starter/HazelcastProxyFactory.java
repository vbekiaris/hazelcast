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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default.IMITATE_SUPER_CLASS;

public class HazelcastProxyFactory {

    private static final Set<String> JDK_PROXYING_BLACKLIST;
    // <Class toProxy, ClassLoader targetClassLoader> -> Class<?> proxy mapping for subclass proxies
    // java.lang.reflect.Proxy already maintains its own cache
    private static final ConcurrentMap<ProxySource, Class<?>> PROXIES = new ConcurrentHashMap<ProxySource, Class<?>>();

    private static final String CLASS_NAME_ENTRY_EVENT = "com.hazelcast.core.EntryEvent";
    private static final String CLASS_NAME_LIFECYCLE_EVENT = "com.hazelcast.core.LifecycleEvent";
    private static final String CLASS_NAME_DATA_AWARE_ENTRY_EVENT = "com.hazelcast.map.impl.DataAwareEntryEvent";
    private static final String CLASS_NAME_DATA_HZ_INSTANCE_PROXY = "com.hazelcast.instance.HazelcastInstanceProxy";
    private static final String CLASS_NAME_DATA_HZ_INSTANCE_IMPL = "com.hazelcast.instance.HazelcastInstanceImpl";

    static {
        HashSet<String> classNames = new HashSet<String>();
        classNames.add(CLASS_NAME_ENTRY_EVENT);
        classNames.add(CLASS_NAME_LIFECYCLE_EVENT);
        classNames.add(CLASS_NAME_DATA_AWARE_ENTRY_EVENT);
        classNames.add(CLASS_NAME_DATA_HZ_INSTANCE_PROXY);
        classNames.add(CLASS_NAME_DATA_HZ_INSTANCE_IMPL);
        JDK_PROXYING_BLACKLIST = classNames;
    }

    /**
     * This is the main entry point to obtain proxies for a target class loader.
     * Create an Object valid for the Hazelcast version started with {@code targetClassLoader} that proxies
     * the given {@code arg} which is valid in the current Hazelcast version.
     * @param targetClassLoader
     * @param arg
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Object proxyObjectForStarter(ClassLoader targetClassLoader, Object arg)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {

        Class<?>[] ifaces = getAllInterfacesIncludingSelf(arg.getClass());
        Class<?>[] delegateIfaces = new Class<?>[ifaces.length];
        Object newArg;
        if (shouldSubclassWithProxy(arg.getClass(), ifaces)) {
            // proxy class via subclassing the existing class implementation in the target targetClassLoader
            Class<?> delegateClass = targetClassLoader.loadClass(arg.getClass().getName());
            newArg = proxyWithSubclass(targetClassLoader, arg, delegateClass);
        } else {
            for (int j = 0; j < ifaces.length; j++) {
                Class<?> clazz = ifaces[j];
                Class<?> delegateInterface = targetClassLoader.loadClass(clazz.getName());
                delegateIfaces[j] = delegateInterface;
            }
            newArg = generateProxyForInterface(arg, targetClassLoader, delegateIfaces);
        }
        return newArg;
    }

    /**
     * Convenience method to proxy an array of objects to be passed as arguments to a method on a class that is
     * loaded by {@code targetClassLoader}
     * @param args
     * @param targetClassLoader
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static Object[] proxyArgumentsIfNeeded(Object[] args, ClassLoader targetClassLoader)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException,
            NoSuchMethodException, InvocationTargetException {
        if (args == null) {
            return null;
        }

        Object[] newArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null || arg.getClass().getClassLoader() == String.class.getClassLoader()) {
                newArgs[i] = arg;
            } else {
                newArgs[i] = proxyObjectForStarter(targetClassLoader, arg);
            }
        }
        return newArgs;
    }

    /**
     * Generate a JDK dynamic proxy implementing the expected interfaces.
     * @param delegate
     * @param proxyTargetClassloader
     * @param expectedInterfaces
     * @param <T>
     * @return
     */
    private static <T> T generateProxyForInterface(Object delegate, ClassLoader proxyTargetClassloader, Class<?>...expectedInterfaces) {
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

    /**
     *
     * @param targetClassLoader
     * @param arg
     * @param delegateClass
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static Object proxyWithSubclass(ClassLoader targetClassLoader, Object arg, Class<?> delegateClass)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException,
            InvocationTargetException, ClassNotFoundException {

        Class<?> targetClass;
        ProxySource proxySource = ProxySource.of(arg.getClass(), targetClassLoader);
        // todo: use own ConcurrentRefHashMap with construct-if-absent
        if (PROXIES.containsKey(proxySource)) {
            targetClass = PROXIES.get(proxySource);
        } else {
            targetClass = new ByteBuddy().subclass(delegateClass, IMITATE_SUPER_CLASS)
                                         .method(ElementMatchers.isDeclaredBy(delegateClass))
                                         .intercept(InvocationHandlerAdapter.of(new ProxyInvocationHandler(arg)))
                                         .make()
                                         .load(targetClassLoader)
                                         .getLoaded();
            PROXIES.put(proxySource, targetClass);
        }

        return constructSubclass(targetClassLoader, targetClass, arg);
    }

    /**
     * Check whether given {@code delegateClass} should be proxied by subclassing or dynamic JDK proxy.
     * @param delegateClass  class of object to be proxied
     * @param ifaces         interfaces implemented by delegateClass
     * @return
     */
    private static boolean shouldSubclassWithProxy(Class<?> delegateClass, Class<?>[] ifaces) {
        String className = delegateClass.getName();
        if (JDK_PROXYING_BLACKLIST.contains(className)) {
            return true;
        }

        if (ifaces.length == 0) {
            return true;
        }

        return false;
    }

    private static Object constructSubclass(ClassLoader starterClassLoader, Class<?> subclass, Object delegate)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException,
            NoSuchMethodException, InvocationTargetException {
        String className = delegate.getClass().getName();
        if (className.equals(CLASS_NAME_DATA_AWARE_ENTRY_EVENT)) {
            return proxyDataAwareEntryEvent(starterClassLoader, subclass, delegate);
        } else if (className.equals(CLASS_NAME_LIFECYCLE_EVENT)) {
            return proxyLifecycleEvent(starterClassLoader, subclass, delegate);
        } else if (className.equals(CLASS_NAME_DATA_HZ_INSTANCE_PROXY)) {
            return proxyHazelcastInstanceProxy(starterClassLoader, subclass, delegate);
        } else if (className.equals(CLASS_NAME_DATA_HZ_INSTANCE_IMPL)) {
            return proxyHazelcastInstanceImpl(starterClassLoader, subclass, delegate);
        } else {
            throw new UnsupportedOperationException("Cannot proxy " + delegate.getClass());
        }
    }

    private static Object proxyDataAwareEntryEvent(ClassLoader starterClassLoader, Class<?> subclass,
                                                   Object delegate)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        // locate required classes on target class loader
        Class<?> dataClass = starterClassLoader.loadClass("com.hazelcast.nio.serialization.Data");
        Class<?> memberClass = starterClassLoader.loadClass("com.hazelcast.core.Member");
        Class<?> serServiceClass = starterClassLoader.loadClass("com.hazelcast.spi.serialization.SerializationService");
        Constructor<?> constructor = subclass.getConstructor(memberClass, Integer.TYPE, String.class, dataClass,
                dataClass, dataClass, dataClass, serServiceClass);

        Object serializationService = getFieldValueReflectively(delegate, "serializationService");
        Object source = getFieldValueReflectively(delegate, "source");
        Object member = getFieldValueReflectively(delegate, "member");
        Object entryEventType = getFieldValueReflectively(delegate, "entryEventType");
        Integer eventTypeId = (Integer) entryEventType.getClass().getMethod("getType").invoke(entryEventType);
        Object dataKey = getFieldValueReflectively(delegate, "dataKey");
        Object dataNewValue = getFieldValueReflectively(delegate, "dataNewValue");
        Object dataOldValue = getFieldValueReflectively(delegate, "dataOldValue");
        Object dataMergingValue = getFieldValueReflectively(delegate, "dataMergingValue");

        Object[] args = new Object[] {member, eventTypeId.intValue(), source,
                                      dataKey, dataNewValue,
                                      dataOldValue, dataMergingValue,
                                      serializationService};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }

    private static Object proxyLifecycleEvent(ClassLoader starterClassLoader, Class<?> subclass,
                                            Object delegate)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        // locate required classes on target class loader
        Class<?> stateClass = starterClassLoader.loadClass("com.hazelcast.core.LifecycleEvent$LifecycleState");
        Constructor<?> constructor = subclass.getConstructor(stateClass);

        Object state = getFieldValueReflectively(delegate, "state");
        Object[] args = new Object[] {state};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }

    private static Object proxyHazelcastInstanceProxy(ClassLoader starterClassLoader, Class<?> subclass,
                                                      Object delegate)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        // locate required classes on target class loader
        Class<?> hzInstanceImplClass = starterClassLoader.loadClass("com.hazelcast.instance.HazelcastInstanceImpl");
        Constructor<?> constructor = subclass.getDeclaredConstructor(hzInstanceImplClass);
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }

        Object hzInstanceImpl = getFieldValueReflectively(delegate, "original");
        Object[] args = new Object[] {hzInstanceImpl};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }

    private static Object proxyHazelcastInstanceImpl(ClassLoader starterClassLoader, Class<?> subclass,
                                                      Object delegate)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {

        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return all interfaces implemented by {@code type}, along with {@code type} itself if it is an interface
     * @param type
     * @return
     */
    private static Class<?>[] getAllInterfacesIncludingSelf(Class<?> type) {
        Set<Class<?>> interfaces = new HashSet<Class<?>>();
        interfaces.addAll(Arrays.asList(getAllInterfaces(type)));
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (type.isInterface()) {
            interfaces.add(type);
        }
        return interfaces.toArray(new Class<?>[0]);
    }

    // copied over from upstream/master/ClassLoaderUtil
    private static Class<?>[] getAllInterfaces(Class<?> clazz) {
        Collection<Class<?>> interfaces = new HashSet<Class<?>>();
        addOwnInterfaces(clazz, interfaces);
        addInterfacesOfSuperclasses(clazz, interfaces);
        return interfaces.toArray(new Class<?>[0]);
    }

    private static void addOwnInterfaces(Class<?> clazz, Collection<Class<?>> allInterfaces) {
        Class<?>[] interfaces = clazz.getInterfaces();
        Collections.addAll(allInterfaces, interfaces);
        for (Class cl : interfaces) {
            addOwnInterfaces(cl, allInterfaces);
        }
    }

    private static void addInterfacesOfSuperclasses(Class<?> clazz, Collection<Class<?>> interfaces) {
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
            addOwnInterfaces(superClass, interfaces);
            superClass = superClass.getSuperclass();
        }
    }

    private static Object getFieldValueReflectively(Object arg, String fieldName)
            throws IllegalAccessException {
        checkNotNull(arg, "Argument cannot be null");
        checkHasText(fieldName, "Field name cannot be null");

        Field field = getAllFieldsByName(arg.getClass()).get(fieldName);
        if (field == null) {
            throw new NoSuchFieldError("Field " + fieldName + " does not exist on object " + arg);
        }

        field.setAccessible(true);
        return field.get(arg);
    }

    private static Map<String, Field> getAllFieldsByName(Class<?> clazz) {
        ConcurrentMap<String, Field> fields = new ConcurrentHashMap<String, Field>();
        Field[] ownFields = clazz.getDeclaredFields();
        for (Field field : ownFields) {
            fields.put(field.getName(), field);
        }
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
            ownFields = superClass.getDeclaredFields();
            for (Field field : ownFields) {
                fields.putIfAbsent(field.getName(), field);
            }
            superClass = superClass.getSuperclass();
        }
        return fields;
    }

    /**
     * (Class toProxy, ClassLoader targetClassLoader) tuple that is used as a key for caching the generated
     * proxy class for {@code toProxy} on {@code targetClassloader}.
     */
    private static class ProxySource {
        private final Class<?> klass;
        private final ClassLoader targetClassLoader;

        public ProxySource(Class<?> klass, ClassLoader targetClassLoader) {
            this.klass = klass;
            this.targetClassLoader = targetClassLoader;
        }

        public Class<?> getKlass() {
            return klass;
        }

        public ClassLoader getTargetClassLoader() {
            return targetClassLoader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ProxySource that = (ProxySource) o;

            if (!klass.equals(that.klass)) {
                return false;
            }
            return targetClassLoader.equals(that.targetClassLoader);
        }

        @Override
        public int hashCode() {
            int result = klass.hashCode();
            result = 31 * result + targetClassLoader.hashCode();
            return result;
        }

        public static ProxySource of(Class<?> klass, ClassLoader targetClassLoader) {
            return new ProxySource(klass, targetClassLoader);
        }
    }
}
