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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

class ProxyInvocationHandler implements InvocationHandler, Serializable {

    private final Object delegate;

    ProxyInvocationHandler(Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ClassLoader targetClassLoader = proxy.getClass().getClassLoader();
        Utils.debug("Proxy " + this + " called. Method: " + method);
        Class<?> delegateClass = delegate.getClass();
        Method methodDelegate = getMethodDelegate(method, delegateClass);

        Type returnType = method.getGenericReturnType();
        Type delegateReturnType = methodDelegate.getGenericReturnType();
        ClassLoader delegateClassClassLoader = delegateClass.getClassLoader();
        Object[] newArgs = proxyArgumentsIfNeeded(proxy, args, delegateClassClassLoader);
        Object delegateResult = invokeMethodDelegate(methodDelegate, newArgs);
        if (!shouldProxy(method, methodDelegate, delegateResult)) {
            return delegateResult;
        }

        if (returnType instanceof ParameterizedType) {
            ParameterizedType parameterizedReturnType = (ParameterizedType) returnType;
            ParameterizedType parameterizedDelegateReturnType = (ParameterizedType) delegateReturnType;

            if (Collection.class.isAssignableFrom((Class) parameterizedDelegateReturnType.getRawType())) {
                Collection result;
                Collection delegateCollectionResult = (Collection) delegateResult;
                // check if the raw types are equal: if yes, then return a collection of the same type
                // otherwise proxy it
                if (parameterizedDelegateReturnType.getRawType().equals(parameterizedReturnType.getRawType())) {
                    result = delegateCollectionResult;
                } else {
                    result = (Collection) proxyReturnObject(targetClassLoader, method.getReturnType(), delegateResult);
                }

                // if the parameter type is not equal, need to proxy it
                Type returnParameterType = parameterizedReturnType.getActualTypeArguments()[0];
                Type delegateParameterType = parameterizedDelegateReturnType.getActualTypeArguments()[0];
                // if the type argument is equal, just return the result, otherwise proxy each item in the collection
                if (returnParameterType.equals(delegateParameterType)) {
                    return result;
                } else {
                    // TODO assume the collection class of the delegate has a no-args constructor
                    Collection temp = newCollectionFor((Class) parameterizedDelegateReturnType.getRawType());
                    for (Object o : delegateCollectionResult) {
                        // TODO hack: assume the return type argument is not parameterized itself and cast to class
                        temp.add(proxyReturnObject(targetClassLoader, (Class) returnParameterType, o));
                    }
                    try {
                        result.clear();
                        result.addAll(temp);
                        return result;
                    } catch (UnsupportedOperationException e) {
                        return temp;
                    }
                }
            } else {
                // TODO proxy ignoring the type arguments
                return proxyReturnObject(targetClassLoader, method.getReturnType(), delegateResult);
            }
        } else {
            //at this point we know the delegate return something loaded by
            //different class then the proxy -> we need to proxy the result
            return proxyReturnObject(targetClassLoader, method.getReturnType(), delegateResult);
        }
    }

    /**
     *
     * @param targetClassLoader the classloader on which the proxy will be created
     * @param returnClass       the expected Class of the proxy to be created
     * @param delegate    the object to be delegated to by the proxy
     * @return                  a proxy to delegate
     */
    private Object proxyReturnObject(ClassLoader targetClassLoader, Class<?> returnClass, Object delegate) {
        Class<?>[] interfaces = getAllInterfacesIncludingSelf(returnClass);
        Object resultingProxy = HazelcastProxyFactory
                .generateProxyForInterface(delegate, targetClassLoader, interfaces);
        printInfoAboutResultProxy(resultingProxy);
        return resultingProxy;
    }

    private Object[] proxyArgumentsIfNeeded(Object proxy, Object[] args, ClassLoader delegateClassClassLoader)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (args == null) {
            return null;
        }

        Object[] newArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null || arg.getClass().getClassLoader() == String.class.getClassLoader()) {
                newArgs[i] = arg;
            } else {
                newArgs[i] = proxyObjectForStarter(delegateClassClassLoader, arg);
            }
        }
        return newArgs;
    }

    /**
     * Create an Object valid for the Hazelcast version started with {@code starterClassLoader} that proxies
     * the given {@code arg} which is valid in the current Hazelcast version.
     * @param starterClassLoader
     * @param arg
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private Object proxyObjectForStarter(ClassLoader starterClassLoader, Object arg)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?>[] ifaces = getAllInterfacesIncludingSelf(arg.getClass());
        Class<?>[] delegateIfaces = new Class<?>[ifaces.length];
        Object newArg;
        if (ifaces.length > 0) {
            for (int j = 0; j < ifaces.length; j++) {
                Class<?> clazz = ifaces[j];
                Class<?> delegateInterface = starterClassLoader.loadClass(clazz.getName());
                delegateIfaces[j] = delegateInterface;
            }
            newArg = HazelcastProxyFactory
                    .generateProxyForInterface(arg, starterClassLoader, delegateIfaces);
        } else {
            throw new UnsupportedOperationException("Concrete class proxying not implemented yet");
        }
        return newArg;
    }

    /**
     *
     * @param starterClassLoader
     * @param arg
     * @param delegateClass
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private Object proxyWithSubclass(ClassLoader starterClassLoader, Object arg, Class<?> delegateClass)
            throws InstantiationException, IllegalAccessException {
        return null;
    }

    private Object invokeMethodDelegate(Method methodDelegate, Object[] args) throws Throwable {
        Object delegateResult;
        try {
            methodDelegate.setAccessible(true);
            delegateResult = methodDelegate.invoke(delegate, args);
        } catch (IllegalAccessException e) {
            throw Utils.rethrow(e);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
        return delegateResult;
    }

    private Method getMethodDelegate(Method method, Class<?> delegateClass) {
        Method methodDelegate;
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes != null) {
            for (int i = 0; i < parameterTypes.length; i++) {
                Class<?> parameterType = parameterTypes[i];
                ClassLoader parameterTypeClassloader = parameterType.getClassLoader();
                ClassLoader delegateClassLoader = delegateClass.getClassLoader();
                if (parameterTypeClassloader != String.class.getClassLoader() && parameterTypeClassloader != delegateClassLoader) {
                    try {
                        Class<?> delegateParameterType = delegateClassLoader.loadClass(parameterType.getName());
                        parameterTypes[i] = delegateParameterType;
                    } catch (ClassNotFoundException e) {
                        throw Utils.rethrow(e);
                    }
                }
            }
        }
        try {
            methodDelegate = delegateClass.getMethod(method.getName(), parameterTypes);
        } catch (NoSuchMethodException e) {
            throw Utils.rethrow(e);
        }
        return methodDelegate;
    }

    /**
     * Recursively
     * @param type
     * @return
     */
    private Class<?>[] getAllInterfacesIncludingSelf(Class<?> type) {
        Set<Class<?>> interfaces = new HashSet<Class<?>>();
        interfaces.addAll(Arrays.asList(getAllInterfaces(type)));
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (type.isInterface()) {
            interfaces.add(type);
        }
        return interfaces.toArray(new Class<?>[0]);
    }

    private static void printInfoAboutResultProxy(Object resultingProxy) {
        if (!Utils.DEBUG_ENABLED) {
            return;
        }
        Utils.debug("Returning proxy " + resultingProxy + ", loaded by " + resultingProxy.getClass().getClassLoader());
        Class<?>[] ifaces = resultingProxy.getClass().getInterfaces();
        Utils.debug("The proxy implementes intefaces: ");
        for (Class<?> iface : ifaces) {
            Utils.debug(iface + ", loaded by " + iface.getClassLoader());
        }
    }

    /**
     * HACK HACK HACK
     * @return a new Collection type that can be used as substitute for given type
     */
    private static Collection newCollectionFor(Class type) {
        if (Set.class.isAssignableFrom(type)) {
            return new HashSet();
        } else if (List.class.isAssignableFrom(type)) {
            return new ArrayList();
        } else if (Queue.class.isAssignableFrom(type)) {
            return new ConcurrentLinkedQueue();
        } else {
            throw new UnsupportedOperationException("Cannot locate collection type for " + type);
        }
    }

    private boolean shouldProxy(Method proxyMethod, Method delegateMethod, Object delegateResult) {
        if (delegateResult == null) {
            return false;
        }

        Type returnType = proxyMethod.getGenericReturnType();
        if (returnType instanceof ParameterizedType) {
            return true;
        }

        // if there return types are equals -> they are loaded
        // by the same classloader -> no need to proxy what it returns
        Class<?> returnClass = proxyMethod.getReturnType();
        Class<?> delegateReturnClass = delegateMethod.getReturnType();
        return !(returnClass.equals(delegateReturnClass));
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
}
