/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.diagnostics;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class ThreadDumpTask implements DiagnosticTask {
    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final StringBuilder sb = new StringBuilder();

        try {
            final HotSpotDiagnosticMXBean diagnosticMXBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            final Path tempFile = Files.createTempFile("nifi-thread-dump-", ".txt");
            try {
                Files.deleteIfExists(tempFile);
                diagnosticMXBean.dumpThreads(tempFile.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
                sb.append(Files.readString(tempFile));
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } catch (final IOException e) {
            sb.append("Failed to capture thread dump: ").append(e.getMessage());
        }

        return new StandardDiagnosticsDumpElement("Thread Dump", Collections.singletonList(sb.toString()));
    }
}
