// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <map>
#include <vector>

#include <process/owned.hpp>

#include <mesos/scheduler.hpp>

#include <mesos/v1/scheduler.hpp>

#include <mesos/v1/scheduler/scheduler.hpp>

#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

#include "jvm/jvm.hpp"

#include "construct.hpp"
#include "convert.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "master/validation.hpp"

#include "org_apache_mesos_MesosSchedulerDriver.h"
#include "org_apache_mesos_v1_Scheduler.h"

using namespace mesos;

using namespace mesos::v1::scheduler;

using std::string;
using std::map;
using std::vector;

namespace v1 {

class V0Scheduler : public Scheduler
{
public:
  V0Scheduler(JNIEnv* _env, jweak _jscheduler)
    : jvm(NULL), env(_env), jscheduler(_jscheduler), didSubscribe(false)
  {
    env->GetJavaVM(&jvm);

  }

  virtual ~V0Scheduler() {}

  virtual void registered(SchedulerDriver* driver,
                          const FrameworkID& frameworkId,
                          const MasterInfo& masterInfo);
  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo);
  virtual void disconnected(SchedulerDriver* driver);
  virtual void resourceOffers(SchedulerDriver* driver,
                              const vector<Offer>& offers);
  virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId);
  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status);
  virtual void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId,
                                const string& data);
  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& slaveId);
  virtual void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorId,
                            const SlaveID& slaveId,
                            int status);
  virtual void error(SchedulerDriver* driver, const string& message);

  void _received(const Event& event);

  JavaVM* jvm;
  JNIEnv* env;
  const jweak jscheduler;
  bool didSubscribe;
  Option<FrameworkID> frameworkId;

private:

  void __received(const Event& event);

  std::queue<Event> pendingEvents;
};


void V0Scheduler::_received(const Event& event)
{
  // If the user has not yet called `SUBSCRIBE`, then we enqueue the
  // events so that we only forward them post-subscription. This keeps
  // the behavior consistent with the V1 API.
  if (!didSubscribe) {
    pendingEvents.emplace(event);
    return;
  }

  __received(event);

  // Forward all events now that we are allowed to.
  // NOTE: This queue should empty as soon as we forward the mocked
  // `SUBSCRIBED` message.
  while (!pendingEvents.empty()) {
    __received(pendingEvents.front());
    pendingEvents.pop();
  }
}


void V0Scheduler::__received(const Event& event)
{
  jvm->AttachCurrentThread(JNIENV_CAST(&env), NULL);

  jclass clazz = env->GetObjectClass(jscheduler);

  jobject jevent = convert<Event>(env, event);

  // scheduler.received(event);
  jmethodID received =
    env->GetMethodID(clazz, "received",
         "(Lorg/apache/mesos/v1/scheduler/Protos$Event;)V");

  env->ExceptionClear();

  env->CallVoidMethod(jscheduler, received, jevent);

  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    ABORT("exception thrown during `received` call");
  }

  jvm->DetachCurrentThread();
}


void V0Scheduler::registered(
    SchedulerDriver* driver,
    const FrameworkID& frameworkId_,
    const MasterInfo& masterInfo)
{
  frameworkId = frameworkId_;

  jvm->AttachCurrentThread(JNIENV_CAST(&env), NULL);

  jclass clazz = env->GetObjectClass(jscheduler);

  // scheduler.connected();
  jmethodID connected =
    env->GetMethodID(clazz, "connected",
         "()V");

  env->ExceptionClear();

  env->CallVoidMethod(jscheduler, connected);

  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    ABORT("exception thrown during `connected` call");
  }

  jvm->DetachCurrentThread();
}


void V0Scheduler::reregistered(SchedulerDriver*, const MasterInfo& masterInfo)
{
  CHECK_SOME(frameworkId);

  jvm->AttachCurrentThread(JNIENV_CAST(&env), NULL);

  jclass clazz = env->GetObjectClass(jscheduler);

  // scheduler.connected();
  jmethodID connected =
    env->GetMethodID(clazz, "connected",
         "()V");

  env->ExceptionClear();

  env->CallVoidMethod(jscheduler, connected);

  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    ABORT("exception thrown during `connected` call");
  }

  jvm->DetachCurrentThread();
}


void V0Scheduler::disconnected(SchedulerDriver* driver)
{
  // Force users to call `SUBSCRIBE` again when they re-connect.
  didSubscribe = false;

  jvm->AttachCurrentThread(JNIENV_CAST(&env), NULL);

  jclass clazz = env->GetObjectClass(jscheduler);

  // scheduler.disconnected();
  jmethodID disconnected =
    env->GetMethodID(clazz, "disconnected",
         "()V");

  env->ExceptionClear();

  env->CallVoidMethod(jscheduler, disconnected);

  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    ABORT("exception thrown during `disconnected` call");
    return;
  }

  jvm->DetachCurrentThread();
}


void V0Scheduler::resourceOffers(
    SchedulerDriver* driver,
    const vector<Offer>& _offers)
{
  Event event;
  event.set_type(Event::OFFERS);

  Event::Offers* offers = event.mutable_offers();

  foreach (const Offer& offer, _offers) {
    offers->add_offers()->CopyFrom(mesos::internal::evolve(offer));
  }

  _received(event);
}


void V0Scheduler::offerRescinded(
    SchedulerDriver* driver,
    const OfferID& offerId)
{
  Event event;
  event.set_type(Event::RESCIND);

  event.mutable_rescind()->mutable_offer_id()->
    CopyFrom(mesos::internal::evolve(offerId));

  _received(event);
}


void V0Scheduler::statusUpdate(
    SchedulerDriver* driver,
    const TaskStatus& status)
{
  Event event;
  event.set_type(Event::UPDATE);

  event.mutable_update()->mutable_status()->
    CopyFrom(mesos::internal::evolve(status));

  _received(event);
}


void V0Scheduler::frameworkMessage(
    SchedulerDriver* driver,
    const ExecutorID& executorId,
    const SlaveID& slaveId,
    const string& data)
{
  Event event;
  event.set_type(Event::MESSAGE);

  event.mutable_message()->mutable_agent_id()->
    CopyFrom(mesos::internal::evolve(slaveId));

  event.mutable_message()->mutable_executor_id()->
    CopyFrom(mesos::internal::evolve(executorId));

  event.mutable_message()->set_data(data.data());

  _received(event);
}


void V0Scheduler::slaveLost(SchedulerDriver* driver, const SlaveID& slaveId)
{
  Event event;
  event.set_type(Event::FAILURE);

  event.mutable_failure()->mutable_agent_id()->
    CopyFrom(mesos::internal::evolve(slaveId));

  _received(event);
}


void V0Scheduler::executorLost(
    SchedulerDriver* driver,
    const ExecutorID& executorId,
    const SlaveID& slaveId,
    int status)
{
  Event event;
  event.set_type(Event::FAILURE);

  event.mutable_failure()->mutable_agent_id()->
    CopyFrom(mesos::internal::evolve(slaveId));

  event.mutable_failure()->mutable_executor_id()->
    CopyFrom(mesos::internal::evolve(executorId));

  event.mutable_failure()->set_status(status);

  _received(event);
}


void V0Scheduler::error(SchedulerDriver* driver, const string& message)
{
  Event event;
  event.set_type(Event::ERROR);

  event.mutable_error()->set_message(message);

  _received(event);
}

} // namespace v1 {

extern "C" {

/*
 * Class:     org_apache_mesos_v1_V0Scheduler
 * Method:    initialize
 * Signature: ()V
 *
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_V0Scheduler_initialize
  (JNIEnv* env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  // Create a weak global reference to the V0Scheduler
  // instance (we want a global reference so the GC doesn't collect
  // the instance but we make it weak so the JVM can exit).
  jweak jscheduler = env->NewWeakGlobalRef(thiz);

  // Get out the FrameworkInfo passed into the constructor.
  jfieldID framework = env->GetFieldID(clazz, "framework", "Lorg/apache/mesos/v1/Protos$FrameworkInfo;");
  jobject jframework = env->GetObjectField(thiz, framework);

  // Get out the master passed into the constructor.
  jfieldID master = env->GetFieldID(clazz, "master", "Ljava/lang/String;");
  jobject jmaster = env->GetObjectField(thiz, master);

  // Create the C++ scheduler and initialize the __scheduler variable.
  ::v1::V0Scheduler* scheduler = new ::v1::V0Scheduler(env, jscheduler);

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  env->SetLongField(thiz, __scheduler, (jlong) scheduler);

  SchedulerDriver* driver = new MesosSchedulerDriver(
      scheduler,
      construct<FrameworkInfo>(env, jframework),
      construct<string>(env, jmaster),
      false /* TODO(jmlvanre): implicit ack */);

  driver->start();

  // Initialize the __driver variable
  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  env->SetLongField(thiz, __driver, (jlong) driver);
}


/*
 * Class:     org_apache_mesos_v1_V0Scheduler
 * Method:    finalize
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_V0Scheduler_finalize
  (JNIEnv* env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  ::v1::V0Scheduler* scheduler =
    (::v1::V0Scheduler*) env->GetLongField(thiz, __scheduler);

  env->DeleteWeakGlobalRef(scheduler->jscheduler);

  delete scheduler;
}


/*
 * Class:     org_apache_mesos_v1_V0Scheduler
 * Method:    send
 * Signature: (Lorg/apache/mesos/v1/scheduler/Protos/Call;)V
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_V0Scheduler_send
  (JNIEnv* env, jobject thiz, jobject jcall)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  MesosSchedulerDriver* driver =
    (MesosSchedulerDriver*) env->GetLongField(thiz, __driver);

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  ::v1::V0Scheduler* scheduler =
    (::v1::V0Scheduler*) env->GetLongField(thiz, __scheduler);

  // Construct a C++ Call from the Java Call.
  const Call _call = construct<Call>(env, jcall);

  // Devolve call to V0.
  const mesos::scheduler::Call call = mesos::internal::devolve(_call);

  // TODO(jmlvanre): Validate call.
  Option<Error> validate =
    mesos::internal::master::validation::scheduler::call::validate(call);

  if (validate.isSome()) {
    // TODO(jmlvanre): Throw java error.
  }

  if (!scheduler->didSubscribe && call.type() != mesos::scheduler::Call_Type_SUBSCRIBE) {
    // The first call should be a subscribe.
    // TODO(jmlvanre): Throw java error.
  }

  switch (call.type()) {
    case Call_Type_SUBSCRIBE: {
      // Mock a `SUBSCRIBED` call to mimick the V1 API.
      Event subscribed;
      subscribed.set_type(Event::Type::Event_Type_SUBSCRIBED);
      // TODO(jmlvanre): Ensure frameworkId is always set by this point.
      subscribed.mutable_subscribed()->mutable_framework_id()->CopyFrom(
          mesos::internal::evolve(scheduler->frameworkId.get()));

      scheduler->didSubscribe = true;
      scheduler->_received(subscribed);
      break;
    }
    case Call_Type_TEARDOWN: {
      driver->stop(false);
      break;
    }
    case Call_Type_ACCEPT: {
      CHECK(call.has_accept());
      std::vector<OfferID> offerIds;
      foreach (const OfferID& offerId, call.accept().offer_ids()) {
        offerIds.emplace_back(offerId);
      }

      std::vector<Offer::Operation> operations;
      foreach (const Offer::Operation& operation, call.accept().operations()) {
        operations.emplace_back(operation);
      }

      if (call.accept().has_filters()) {
        driver->acceptOffers(offerIds, operations, call.accept().filters());
      } else {
        driver->acceptOffers(offerIds, operations);
      }

      break;
    }
    case Call_Type_DECLINE: {
      CHECK(call.has_decline());
      foreach (const OfferID& offerId, call.decline().offer_ids()) {
        if (call.decline().has_filters()) {
          driver->declineOffer(offerId, call.decline().filters());
        } else {
          driver->declineOffer(offerId);
        }
      }

      break;
    }
    case Call_Type_REVIVE: {
      driver->reviveOffers();
      break;
    }
    case Call_Type_KILL: {
      CHECK(call.has_kill());
      driver->killTask(call.kill().task_id());
      break;
    }
    case Call_Type_SHUTDOWN: {
      // Not implemented in v0;
      // TODO(jmlvanre): Throw java error.
      break;
    }
    case Call_Type_ACKNOWLEDGE: {
      CHECK(call.has_acknowledge());
      TaskStatus status;
      status.mutable_task_id()->CopyFrom(call.acknowledge().task_id());
      status.mutable_slave_id()->CopyFrom(call.acknowledge().slave_id());
      status.set_uuid(call.acknowledge().uuid());

      driver->acknowledgeStatusUpdate(status);
      break;
    }
    case Call_Type_RECONCILE: {
      CHECK(call.has_reconcile());
      std::vector<TaskStatus> statuses;

      foreach (
          const mesos::scheduler::Call::Reconcile::Task& task,
          call.reconcile().tasks()) {
        TaskStatus status;
        status.mutable_task_id()->CopyFrom(task.task_id());
        statuses.emplace_back(status);
      }

      driver->reconcileTasks(statuses);
      break;
    }
    case Call_Type_MESSAGE: {
      CHECK(call.has_message());
      driver->sendFrameworkMessage(
          call.message().executor_id(),
          call.message().slave_id(),
          string(call.message().data()));
      break;
    }
    case Call_Type_REQUEST: {
      CHECK(call.has_request());
      std::vector<Request> requests;

      foreach (const Request& request, call.request().requests()) {
        requests.emplace_back(request);
      }

      driver->requestResources(requests);
      break;
    }
    case Call_Type_SUPPRESS: {
      driver->suppressOffers();
      break;
    }
    default: {
      // TODO(jmlvanre): Throw java exception.
      break;
    }
  }
}

} // extern "C" {