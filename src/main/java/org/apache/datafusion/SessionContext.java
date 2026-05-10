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

/**
 * A DataFusion session context.
 *
 * <p>Instances are <strong>not thread-safe</strong>. Concurrent calls to {@link #sql} and
 * {@link #close} from different threads can produce a use-after-free on the native side.
 * Callers must externally synchronize, or confine each context to a single thread.
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

    public void sql(String query) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        executeSql(nativeHandle, query);
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
    private static native void executeSql(long handle, String sql);
    private static native void registerParquet(long handle, String name, String path);
    private static native void closeSessionContext(long handle);
}
