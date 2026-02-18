package com.data.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * Linux 上线程 CPU 绑定工具类
 */
public class ThreadAffinityUtils {

    // JNA 映射 Linux C 库
    interface CLibrary extends Library {
        CLibrary INSTANCE = Native.load(Platform.C_LIBRARY_NAME, CLibrary.class);

        int pthread_self();

        int pthread_setaffinity_np(int thread, int cpusetsize, long[] mask);
    }

    /**
     * 将当前线程绑定到指定 CPU 核心
     *
     * @param cpu 核心编号，从 0 开始
     */
    public static void pinCurrentThread(int cpu) {
        if (!Platform.isLinux()) {
            throw new UnsupportedOperationException("Thread affinity only supported on Linux");
        }
        long[] mask = new long[1];
        mask[0] = 1L << cpu; // 将 cpu 位设置为 1
        int threadId = CLibrary.INSTANCE.pthread_self();
        int res = CLibrary.INSTANCE.pthread_setaffinity_np(threadId, mask.length * Long.BYTES, mask);
        if (res != 0) {
            throw new RuntimeException("Failed to set thread affinity, error code: " + res);
        }
    }

    /**
     * 将指定线程绑定到 CPU 核心
     * 注意：Java 中获取原生线程 ID 复杂，推荐使用 pinCurrentThread()
     */
    public static void pinThread(Thread t, int cpu) {
        // Java 不支持直接获取其他线程的 pthread_id
        throw new UnsupportedOperationException("Cannot pin other Java threads directly. Use pinCurrentThread() inside the thread.");
    }

    // 测试示例
    public static void main(String[] args) {
        int availableCores = Runtime.getRuntime().availableProcessors();
        System.out.println("JVM 可用 CPU 核心数: " + availableCores);

        System.out.print("CPU 编号: ");
        for (int i = 0; i < availableCores; i++) {
            System.out.print(i + " ");
        }
        int cpuToBind = 0; // 绑定到 CPU 0
        Thread t = new Thread(() -> {
            System.out.println("Thread before pinning: " + Thread.currentThread().getName());
            pinCurrentThread(cpuToBind);
            System.out.println("Thread pinned to CPU " + cpuToBind + ": " + Thread.currentThread().getName());
            // 模拟长时间运行
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        });
        t.start();
    }
}
