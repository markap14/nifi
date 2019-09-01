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
package org.apache.nifi.python;

import py4j.ClientServer;
import py4j.GatewayServer;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

public class PythonBridge {
    private static final Class[] entryPointTypes = new Class[] {PythonController.class};

    private final int pythonPort;
    private ClientServer clientServer;
    private PythonController pythonController;

    public PythonBridge(final int pythonPort) {
        this.pythonPort = pythonPort;
    }

    public synchronized PythonController getPythonController() {
        if (pythonController == null) {
            if (clientServer == null) {
                clientServer = new ClientServer(0, GatewayServer.defaultAddress(), pythonPort, GatewayServer.defaultAddress(), 5000, 60_000,
                    ServerSocketFactory.getDefault(), SocketFactory.getDefault(), null);
                pythonController = (PythonController) clientServer.getPythonServerEntryPoint(entryPointTypes);
            }
        }

        return pythonController;
    }
}
