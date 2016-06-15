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

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import org.apache.mesos.Protos.*;

import org.apache.mesos.v1.Protos.*;

import org.apache.mesos.v1.scheduler.Protos.*;

import java.util.List;

public abstract class ShimScheduler {
  static {
    MesosNativeLibrary.load();
  }

  public enum Mode {
    V0, V1
  }

  public ShimScheduler(Mode mode, String master, org.apache.mesos.v1.Protos.FrameworkInfo framework) {
    if (master == null) {
      throw new NullPointerException("Not expecting a null master");
    }

    if (framework == null) {
      throw new NullPointerException("Not expecting a null FrameworkInfo");
    }

    this.mode = mode;
    switch (mode) {
      case V0: {
        this.v0Scheduler = new V0Scheduler(this, master, framework);
        this.v1Scheduler = null;
        break;
      }
      case V1: {
        this.v0Scheduler = null;
        this.v1Scheduler = new V1Scheduler(this, master, framework);
        break;
      }
      default: {
        this.v0Scheduler = null;
        this.v1Scheduler = null;
      }
    }
  }

  abstract public void connected();

  abstract public void disconnected();

  abstract public void received(org.apache.mesos.v1.scheduler.Protos.Event event);

  public void send(Call call) {
    switch (mode) {
      case V0: {
        v0Scheduler.send(call);
        break;
      }
      case V1: {
        v1Scheduler.send(call);
        break;
      }
    }
  }

  private static class V1Scheduler extends org.apache.mesos.v1.Scheduler {
    public V1Scheduler(ShimScheduler shim, String master, org.apache.mesos.v1.Protos.FrameworkInfo framework) {
      super(master, framework);
      this.shim = shim;
    }
    public void connected() { shim.connected(); }
    public void disconnected() { shim.disconnected(); }
    public void received(org.apache.mesos.v1.scheduler.Protos.Event event) {
      shim.received(event);
    }

    private final ShimScheduler shim;
  }

  private static class V0Scheduler extends org.apache.mesos.v1.V0Scheduler {
    public V0Scheduler(ShimScheduler shim, String master, org.apache.mesos.v1.Protos.FrameworkInfo framework) {
      super(master, framework);
      this.shim = shim;
    }
    public void connected() { shim.connected(); }
    public void disconnected() { shim.disconnected(); }
    public void received(org.apache.mesos.v1.scheduler.Protos.Event event) {
      shim.received(event);
    }

    private final ShimScheduler shim;
  }

  private final Mode mode;
  private final V0Scheduler v0Scheduler;
  private final V1Scheduler v1Scheduler;
}