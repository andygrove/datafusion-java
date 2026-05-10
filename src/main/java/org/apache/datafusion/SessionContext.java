/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * A DataFusion session context.
 *
 * <p>Instances are <strong>not thread-safe</strong>. Concurrent calls to any of
 * {@link #sql}, {@link #registerParquet}, or {@link #close} from different threads can
 * produce a use-after-free on the native side. Callers must externally synchronize, or
 * confine each context to a single thread.
 */
public final class SessionContext implements AutoCloseable {
    static {
        NativeLibraryLoader.loadLibrary();
    }

    private long nativeHandle;

    public SessionContext() {
        this.nativeHandle = createSessionContext();
        if (this.nativeHandle == 0) {
            throw new RuntimeException("Failed to create native SessionContext");
        }
    }

    public ArrowReader sql(String query, BufferAllocator allocator) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        try {
            executeQuery(nativeHandle, query, stream.memoryAddress());
            return Data.importArrayStream(allocator, stream);
        } catch (Throwable e) {
            stream.close();
            throw e;
        }
    }

    public void registerParquet(String name, String path) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        registerParquet(nativeHandle, name, path);
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            closeSessionContext(nativeHandle);
            nativeHandle = 0;
        }
    }

    private static native long createSessionContext();
    private static native void executeQuery(long handle, String sql, long ffiStreamAddr);
    private static native void registerParquet(long handle, String name, String path);
    private static native void closeSessionContext(long handle);
}
