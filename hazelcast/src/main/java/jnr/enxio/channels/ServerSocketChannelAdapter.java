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

package jnr.enxio.channels;

import jnr.unixsocket.UnixServerSocketChannel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class ServerSocketChannelAdapter extends ServerSocketChannel implements NativeSelectableChannel {

    private final UnixServerSocketChannel serverSocketChannel;

    public ServerSocketChannelAdapter(UnixServerSocketChannel serverSocketChannel) {
        super(NativeSelectorProvider.getInstance());
        this.serverSocketChannel = serverSocketChannel;
    }

    @Override
    public ServerSocketChannel bind(SocketAddress local, int backlog)
            throws IOException {
        serverSocketChannel.socket().bind(local, backlog);
        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(SocketOption<T> name, T value)
            throws IOException {
        // nop
        return null;
    }

    @Override
    public ServerSocket socket() {
        return null;
    }

    @Override
    public SocketChannel accept() throws IOException {
        return serverSocketChannel.accept();
    }

    @Override
    public SocketAddress getLocalAddress() {
        return serverSocketChannel.getLocalSocketAddress();
    }

    @Override
    public <T> T getOption(SocketOption<T> name) {
        // nop
        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return null;
    }

    @Override
    protected void implCloseSelectableChannel()
            throws IOException {
        Native.close(serverSocketChannel.getFD());
    }

    @Override
    protected void implConfigureBlocking(boolean block)
            throws IOException {
        // copied from UnixServerSocketChannel#implConfigureBlocking
        Native.setBlocking(serverSocketChannel.getFD(), block);
    }

    @Override
    public int getFD() {
        return serverSocketChannel.getFD();
    }

    @Override
    public String toString() {
        return "ServerSocketChannelAdapter[" + serverSocketChannel.getLocalSocketAddress().humanReadablePath() + "]";
    }
}
