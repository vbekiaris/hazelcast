/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.uds;

import com.hazelcast.internal.networking.ChannelOption;
import com.hazelcast.internal.networking.ChannelOptions;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;

/** Unix domain sockets do not support channel options */
public class NoopChannelOptions implements ChannelOptions {
    @Override
    public <T> ChannelOptions setOption(ChannelOption<T> option, T value) {
        return this;
    }

    @Override
    public <T> T getOption(ChannelOption<T> option) {
        if (option.equals(TCP_NODELAY)
            || option.equals(SO_KEEPALIVE)
            || option.equals(SO_REUSEADDR)
            || option.equals(DIRECT_BUF)) {
            return (T) (Boolean) false;
        } else {
            return (T) (Integer) (int)Math.pow(2, 18);
        }
    }
}
