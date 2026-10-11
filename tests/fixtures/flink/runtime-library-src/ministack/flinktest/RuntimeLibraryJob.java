// Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
package ministack.flinktest;

import java.util.Properties;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

/**
 * CounterJob, with its properties read through KinesisAnalyticsRuntime. Built
 * the way AWS documents: the library is provided by the runtime, not bundled,
 * so this runs only when MiniStack provides it from /opt/ministack/flink-lib.
 */
public class RuntimeLibraryJob {
    public static void main(String[] args) throws Exception {
        Properties group = KinesisAnalyticsRuntime.getApplicationProperties().get("Test");
        if (group == null) {
            throw new IllegalStateException("runtime property group Test is missing");
        }
        CounterJob.run(group.getProperty("StreamName"), group.getProperty("Message"));
    }
}
