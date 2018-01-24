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

package com.hazelcast.nio.unix;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.nio.SelectorMode;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.TcpIpConnector;
import jnr.enxio.channels.NativeSelectorProvider;
import jnr.enxio.channels.ServerSocketChannelAdapter;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_WITH_FIX;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains the logic for accepting TcpIpConnections.
 * <p>
 * The {@link UnixServerSocketAcceptor} and {@link TcpIpConnector} are 2 sides of the same coin. The {@link TcpIpConnector} take care
 * of the 'client' side of a connection and the {@link UnixServerSocketAcceptor} is the 'server' side of a connection (each connection
 * has a client and server-side
 */
public class UnixServerSocketAcceptor
        extends Thread {

    private final ServerSocketChannelAdapter unixSocketChannel;
    private final Selector nativeSelector;
    private final IOService ioService;
    private final ILogger logger;
    private final TcpIpConnectionManager connectionManager;
    private volatile boolean stop;

    public UnixServerSocketAcceptor(ServerSocketChannelAdapter unixSocketChannel, Selector nativeSelector, ILogger logger,
                                    String instanceName, IOService ioService, TcpIpConnectionManager connectionManager) {
        super(createThreadPoolName(instanceName, "IO") + "UnixSocketAcceptor");
        this.unixSocketChannel = unixSocketChannel;
        this.logger = logger;
        this.nativeSelector = nativeSelector;
        this.ioService = ioService;
        this.connectionManager = connectionManager;
        this.stop = false;
    }

    @Override
    public void run() {
        logger.info("Starting UnixSocketAcceptor on " + unixSocketChannel);

        try {
            acceptLoop();
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (Throwable e) {
            logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
        } finally {
            closeSelector();
        }
    }

    public void notifyStop() {
        stop = true;
    }

    private void acceptLoop()
            throws IOException {
        while (!stop) {
            // block until new connection or interruption.
            int keyCount = nativeSelector.select();
            if (isInterrupted()) {
                break;
            }
            if (keyCount == 0) {
                continue;
            }
            Iterator<SelectionKey> it = nativeSelector.selectedKeys().iterator();
            handleSelectionKeys(it);
        }
    }

    private void handleSelectionKeys(Iterator<SelectionKey> it) {
        while (it.hasNext()) {
            SelectionKey sk = it.next();
            it.remove();
            // of course it is acceptable!
            if (sk.isValid() && sk.isAcceptable()) {
                acceptSocket();
            }
        }
    }

    private void closeSelector() {
        if (nativeSelector == null) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Closing selector " + Thread.currentThread().getName());
        }

        try {
            nativeSelector.close();
        } catch (Exception e) {
            logger.finest("Exception while closing selector", e);
        }
    }

    private void acceptSocket() {
        Channel channel = null;
        try {
            SocketChannel socketChannel = unixSocketChannel.accept();
            if (socketChannel != null) {
                channel = connectionManager.createChannel(socketChannel, false);
            }
        } catch (Exception e) {
            if (e instanceof ClosedChannelException && !connectionManager.isLive()) {
                // ClosedChannelException
                // or AsynchronousCloseException
                // or ClosedByInterruptException
                logger.finest("Terminating socket acceptor thread...", e);
            } else {
                logger.severe("Unexpected error while accepting connection! " + e.getClass().getName() + ": " + e.getMessage());
                try {
                    unixSocketChannel.close();
                } catch (Exception ex) {
                    logger.finest("Closing server socket failed", ex);
                }
                ioService.onFatalError(e);
            }
        }

        if (channel != null) {
            final Channel theChannel = channel;
            logger.info("Accepting socket connection from " + theChannel.socket());
            if (ioService.isSocketInterceptorEnabled()) {
                configureAndAssignSocket(theChannel);
            } else {
                ioService.executeAsync(new Runnable() {
                    @Override
                    public void run() {
                        configureAndAssignSocket(theChannel);
                    }
                });
            }
        }
    }

    private void configureAndAssignSocket(Channel channel) {
        try {
            ioService.configureSocket(channel.socket());
            ioService.interceptSocket(channel.socket(), true);
            connectionManager.newConnection(channel, null);
        } catch (Exception e) {
            logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
            closeResource(channel);
        }
    }
}
