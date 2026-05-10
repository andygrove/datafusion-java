package org.apache.datafusion;

public final class SessionContext implements AutoCloseable {
    static {
        NativeLibraryLoader.loadLibrary();
    }

    private long nativeHandle;

    public SessionContext() {
        this.nativeHandle = createSessionContext();
    }

    public void sql(String query) {
        executeSql(nativeHandle, query);
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
    private static native void closeSessionContext(long handle);
}
