package org.apache.datafusion;

public final class NativeLibraryLoader {
    private static boolean loaded = false;

    private NativeLibraryLoader() {}

    public static synchronized void loadLibrary() {
        if (!loaded) {
            System.loadLibrary("datafusion_jni");
            loaded = true;
        }
    }
}
