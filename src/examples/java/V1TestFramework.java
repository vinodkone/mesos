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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.concurrent.locks.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.mesos.v1.*;

import org.apache.mesos.v1.Protos.*;

import org.apache.mesos.v1.scheduler.Protos.*;

public class V1TestFramework {
  static class TestScheduler extends ShimScheduler {

    public TestScheduler(
        String master,
        FrameworkInfo framework,
        ExecutorInfo executor,
        int version) {
      this(master, framework, executor, version, 5);
    }

    public TestScheduler(
        String master,
        FrameworkInfo framework,
        ExecutorInfo executor,
        int version,
        int totalTasks) {
      super(version == 0 ?
              ShimScheduler.Mode.V0 :
              ShimScheduler.Mode.V1, master, framework);

      this.framework_ = framework;
      this.executor = executor;
      this.totalTasks = totalTasks;
    }

    public void connected() {
      System.out.println("Connected");
      Call.Builder callBuilder = Call.newBuilder()
          .setType(Call.Type.SUBSCRIBE)
          .setSubscribe(Call.Subscribe.newBuilder()
            .setFrameworkInfo(framework_)
            .build());

      send(callBuilder.build());
    }

    public void disconnected() { System.out.println("Disconnected"); }

    public void received(org.apache.mesos.v1.scheduler.Protos.Event event) {
      System.out.println("Event");
      if (event.getType() == Event.Type.SUBSCRIBED) {
        frameworkId = event.getSubscribed().getFrameworkId();
        System.out.println("Subscribed as " + frameworkId);
      } else if (event.getType() == Event.Type.OFFERS) {
        offers(event.getOffers().getOffersList());
      } else if (event.getType() == Event.Type.RESCIND) {
        System.out.println("Rescind");
      } else if (event.getType() == Event.Type.UPDATE) {
        statusUpdate(event.getUpdate().getStatus());
      } else if (event.getType() == Event.Type.MESSAGE) {
        System.out.println("Message");
      } else if (event.getType() == Event.Type.FAILURE) {
        System.out.println("Failure");
      } else if (event.getType() == Event.Type.ERROR) {
        System.out.println("Error");
      } else if (event.getType() == Event.Type.HEARTBEAT) {
        System.out.println("HeartBeat");
      }
    }

    public void offers(List<Offer> offers) {
      double CPUS_PER_TASK = 4;
      double MEM_PER_TASK = 128;

      for (Offer offer : offers) {
        Offer.Operation.Launch.Builder launch =
          Offer.Operation.Launch.newBuilder();

        double offerCpus = 0;
        double offerMem = 0;
        for (Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals("cpus")) {
            offerCpus += resource.getScalar().getValue();
          } else if (resource.getName().equals("mem")) {
            offerMem += resource.getScalar().getValue();
          }
        }

        System.out.println(
            "Received offer " + offer.getId().getValue() + " with cpus: " +
            offerCpus + " and mem: " + offerMem);

        double remainingCpus = offerCpus;
        double remainingMem = offerMem;
        while (launchedTasks < totalTasks &&
               remainingCpus >= CPUS_PER_TASK &&
               remainingMem >= MEM_PER_TASK) {
          TaskID taskId = TaskID.newBuilder()
            .setValue(Integer.toString(launchedTasks++)).build();

          System.out.println("Launching task " + taskId.getValue() +
                             " using offer " + offer.getId().getValue());

          TaskInfo task = TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setAgentId(offer.getAgentId())
            .addResources(Resource.newBuilder()
                          .setName("cpus")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder()
                            .setValue(CPUS_PER_TASK)
                            .build()))
            .addResources(Resource.newBuilder()
                          .setName("mem")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder()
                            .setValue(MEM_PER_TASK)
                            .build()))
            .setExecutor(ExecutorInfo.newBuilder(executor))
            .build();

          launch.addTaskInfos(TaskInfo.newBuilder(task));

          remainingCpus -= CPUS_PER_TASK;
          remainingMem -= MEM_PER_TASK;
        }

        send(Call.newBuilder()
          .setType(Call.Type.ACCEPT)
          .setFrameworkId(frameworkId)
          .setAccept(Call.Accept.newBuilder()
            .addOfferIds(offer.getId())
            .addOperations(Offer.Operation.newBuilder()
              .setType(Offer.Operation.Type.LAUNCH)
              .setLaunch(launch)
              .build())
            .setFilters(Filters.newBuilder()
              .setRefuseSeconds(1)
              .build()))
          .build());
      }
    }

    public void statusUpdate(TaskStatus status) {
      System.out.println(
          "Status update: task " + status.getTaskId().getValue() +
          " is in state " + status.getState().getValueDescriptor().getName());
      if (status.getState() == TaskState.TASK_FINISHED) {
        finishedTasks++;
        System.out.println("Finished tasks: " + finishedTasks);
        if (finishedTasks == totalTasks) {
          lock.lock();
          try {
            finished = true;
            finishedCondtion.signal();
          } finally {
            lock.unlock();
          }
        }
      }

      if (status.getState() == TaskState.TASK_LOST ||
          status.getState() == TaskState.TASK_KILLED ||
          status.getState() == TaskState.TASK_FAILED) {
        System.err.println(
            "Aborting because task " + status.getTaskId().getValue() +
            " is in unexpected state " +
            status.getState().getValueDescriptor().getName() +
            " with reason '" +
            status.getReason().getValueDescriptor().getName() + "'" +
            " from source '" +
            status.getSource().getValueDescriptor().getName() + "'" +
            " with message '" + status.getMessage() + "'");

        lock.lock();
        try {
          finished = true;
          finishedCondtion.signal();
        } finally {
          lock.unlock();
        }
      }

      send(Call.newBuilder()
        .setType(Call.Type.ACKNOWLEDGE)
        .setFrameworkId(frameworkId)
        .setAcknowledge(Call.Acknowledge.newBuilder()
          .setAgentId(status.getAgentId())
          .setTaskId(status.getTaskId())
          .setUuid(status.getUuid())
          .build())
        .build());
    }

    private FrameworkInfo framework_;
    private FrameworkID frameworkId;
    private final ExecutorInfo executor;
    private final int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;
  }

  private static void usage() {
    String name = V1TestFramework.class.getName();
    System.err.println("Usage: " + name + " master version{0,1} <tasks>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      usage();
      System.exit(1);
    }

    int version = Integer.parseInt(args[1]);
    if (version != 0 && version != 1) {
      usage();
      System.exit(1);
    }

    String uri = new File("./test-executor").getCanonicalPath();

    ExecutorInfo executor = ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue("default"))
      .setCommand(CommandInfo.newBuilder().setValue(uri))
      .setName("Test Executor (Java)")
      .setSource("java_test")
      .build();

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Framework (Java)")
        .setCheckpoint(true);

    ShimScheduler scheduler = args.length == 1
        ? new TestScheduler(
              args[0],
              frameworkBuilder.build(),
              executor,
              version)
        : new TestScheduler(
              args[0],
              frameworkBuilder.build(),
              executor,
              version,
              Integer.parseInt(args[2]));

    lock.lock();
    try {
      while (!finished) {
        finishedCondtion.await();
      }
    } finally {
      lock.unlock();
    }

    System.exit(0);
  }

  static boolean finished = false;
  final static Lock lock = new ReentrantLock();
  final static Condition finishedCondtion = lock.newCondition();
}
