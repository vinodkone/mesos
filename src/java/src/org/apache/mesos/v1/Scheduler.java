/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mesos.v1;

import org.apache.mesos.MesosNativeLibrary;

import org.apache.mesos.v1.Protos.*;

import org.apache.mesos.v1.scheduler.Protos.*;

import java.util.List;

public abstract class Scheduler {
static {
  MesosNativeLibrary.load();
}

  public Scheduler(String master, FrameworkInfo framework) {
    if (master == null) {
      throw new NullPointerException("Not expecting a null master");
    }

    if (framework == null) {
      throw new NullPointerException("Not expecting a null FrameworkInfo");
    }

    this.master = master;
    this.framework = framework;

    initialize();
  }

  abstract public void connected();

  abstract public void disconnected();

  abstract public void received(org.apache.mesos.v1.scheduler.Protos.Event event);

  public native void send(Call call);

  protected native void initialize();
  protected native void finalize();

  private final String master;
  private final FrameworkInfo framework;

  private long __scheduler;
}