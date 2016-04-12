package com.hazelcast.util;

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 *
 */
public interface Natives extends Library {

    static final Natives INSTANCE = (Natives) Native.loadLibrary("c", Natives.class);

    int getpid();
}
