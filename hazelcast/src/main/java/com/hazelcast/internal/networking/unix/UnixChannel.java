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

package com.hazelcast.internal.networking.unix;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.NioChannelReader;
import com.hazelcast.internal.networking.nio.NioChannelWriter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class UnixChannel implements Channel {
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<UnixChannel> CLOSED = newUpdater(UnixChannel.class, "closed");

    private final SocketChannel socketChannel;
    private final ILogger logger = Logger.getLogger(getClass());
    private final ConcurrentMap<?, ?> attributeMap = new ConcurrentHashMap<>();
    private final Set<ChannelCloseListener> closeListeners
            = newSetFromMap(new ConcurrentHashMap<ChannelCloseListener, Boolean>());
    private final boolean clientMode;
    private volatile int closed = FALSE;
    private NioChannelReader reader;
    private NioChannelWriter writer;

    public UnixChannel(SocketChannel socketChannel, boolean clientMode) {
        this.socketChannel = socketChannel;
        this.clientMode = clientMode;
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public void setReader(NioChannelReader reader) {
        this.reader = reader;
    }

    public void setWriter(NioChannelWriter writer) {
        this.writer = writer;
    }

    public NioChannelReader getReader() {
        return reader;
    }

    public NioChannelWriter getWriter() {
        return writer;
    }

    @Override
    public boolean isClientMode() {
        return clientMode;
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    @Override
    public Socket socket() {
        return socketChannel.socket();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        try {
            return socketChannel.getRemoteAddress();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        try {
            return socketChannel.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public long lastReadTimeMillis() {
        return reader.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return writer.lastWriteTimeMillis();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = socketChannel.read(dst);
        //logger.info(this + " read:" + read +" cap:"+dst.capacity()+" pos:"+dst.position());
        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = socketChannel.write(src);
        //logger.info(this + " written:" + written);
        return written;
    }

    @Override
    public void closeInbound() throws IOException {
        //no-op
    }

    @Override
    public void closeOutbound() throws IOException {
        //no-op
    }

    @Override
    public boolean isClosed() {
        return closed == TRUE;
    }

    @Override
    public void close() throws IOException {
        if (!CLOSED.compareAndSet(this, FALSE, TRUE)) {
            return;
        }

        try {
            socketChannel.close();
        } finally {
            for (ChannelCloseListener closeListener : closeListeners) {
                // it is important we catch exceptions so that other listeners aren't obstructed when
                // one of the listeners is throwing an exception.
                try {
                    closeListener.onClose(this);
                } catch (Exception e) {
                    logger.severe(format("Failed to process closeListener [%s] on channel [%s]", closeListener, this), e);
                }
            }
        }
    }

    @Override
    public void addCloseListener(ChannelCloseListener listener) {
        closeListeners.add(checkNotNull(listener, "listener"));
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (isClosed()) {
            return false;
        }
        writer.write(frame);
        return true;
    }

    @Override
    public void flush() {
        writer.flush();
    }

    @Override
    public String toString() {
        return "UdpNioChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
    }
}
