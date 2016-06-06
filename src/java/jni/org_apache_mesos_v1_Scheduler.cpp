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

#include <mesos/v1/mesos.hpp>

#include <mesos/v1/scheduler.hpp>

#include <mesos/v1/scheduler/scheduler.hpp>

#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/net.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

#include "jvm/jvm.hpp"

#include "construct.hpp"
#include "convert.hpp"
#include "org_apache_mesos_v1_Scheduler.h"

using namespace mesos::v1::scheduler;

using std::string;
using std::map;
using std::vector;

namespace v1 {

class JNIScheduler
{
public:
  JNIScheduler(JNIEnv* _env,
               jweak _jscheduler,
               const string& master,
               const Option<mesos::v1::Credential>& credential)
    : jvm(NULL), env(_env), jscheduler(_jscheduler)
  {
    env->GetJavaVM(&jvm);

    mesos.reset(
        new Mesos(master,
            mesos::ContentType::PROTOBUF,
            std::bind(&JNIScheduler::connected, this),
            std::bind(&JNIScheduler::disconnected, this),
            std::bind(&JNIScheduler::received_, this, lambda::_1),
            credential));
  }

  virtual ~JNIScheduler() {}

  virtual void connected();
  virtual void disconnected();
  virtual void received(const Event& event);

  void received_(std::queue<Event> events) {
    while (!events.empty()) {
      received(events.front());
      events.pop();
    }
  }

  JavaVM* jvm;
  JNIEnv* env;
  jweak jscheduler;

  process::Owned<Mesos> mesos;
};


void JNIScheduler::connected()
{
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
    // TODO(jmlvanre): Abort?
    return;
  }

  jvm->DetachCurrentThread();
}


void JNIScheduler::disconnected()
{
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
    // TODO(jmlvanre): Abort?
    return;
  }

  jvm->DetachCurrentThread();
}


void JNIScheduler::received(const Event& event)
{
  jvm->AttachCurrentThread(JNIENV_CAST(&env), NULL);

  jclass clazz = env->GetObjectClass(jscheduler);

  // scheduler.received(event);
  jmethodID received =
    env->GetMethodID(clazz, "received",
         "(Lorg/apache/mesos/v1/scheduler/Protos$Event;)V");

  jobject jevent = convert<Event>(env, event);

  env->ExceptionClear();

  env->CallVoidMethod(jscheduler, received, jevent);

  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    // TODO(jmlvanre): Abort?
    return;
  }

  jvm->DetachCurrentThread();
}

} // namespace v1 {

extern "C" {

/*
 * Class:     org_apache_mesos_v1_Scheduler
 * Method:    initialize
 * Signature: ()V
 *
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_Scheduler_initialize
  (JNIEnv* env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  // Create a weak global reference to the Scheduler
  // instance (we want a global reference so the GC doesn't collect
  // the instance but we make it weak so the JVM can exit).
  jweak jscheduler = env->NewWeakGlobalRef(thiz);

  // Get out the master passed into the constructor.
  jfieldID master = env->GetFieldID(clazz, "master", "Ljava/lang/String;");
  jobject jmaster = env->GetObjectField(thiz, master);

  // TODO(vinod): Extract the credential.

  // Create the C++ scheduler and initialize the __scheduler variable.
  v1::JNIScheduler* scheduler = new v1::JNIScheduler(
      env, jscheduler, construct<string>(env, jmaster), None());

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  env->SetLongField(thiz, __scheduler, (jlong) scheduler);
}


/*
 * Class:     org_apache_mesos_v1_Scheduler
 * Method:    finalize
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_Scheduler_finalize
  (JNIEnv* env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  v1::JNIScheduler* scheduler =
    (v1::JNIScheduler*) env->GetLongField(thiz, __scheduler);

  env->DeleteWeakGlobalRef(scheduler->jscheduler);

  delete scheduler;
}


/*
 * Class:     org_apache_mesos_v1_Scheduler
 * Method:    send
 * Signature: (Lorg/apache/mesos/v1/scheduler/Protos/Call;)V
 */
JNIEXPORT void JNICALL Java_org_apache_mesos_v1_Scheduler_send
  (JNIEnv* env, jobject thiz, jobject jcall)
{
  // Construct a C++ Call from the Java Call.
  Call call = construct<Call>(env, jcall);

  // TODO(jmlvanre): Consider forcing users of the API to specify
  // the fields we currently automatically fill in for them.
  if (call.type() == Call_Type_SUBSCRIBE) {
    // See FrameworkInfo in include/mesos/v1/mesos.proto:
    if (call.subscribe().framework_info().user().empty()) {
      Result<string> user = os::user();
      CHECK_SOME(user);

      call.mutable_subscribe()->mutable_framework_info()->set_user(user.get());
    }

    if (call.subscribe().framework_info().hostname().empty()) {
      Try<string> hostname = net::hostname();
      if (hostname.isSome()) {
        call.mutable_subscribe()->mutable_framework_info()->set_hostname(hostname.get());
      }
    }
  }

  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __scheduler = env->GetFieldID(clazz, "__scheduler", "J");
  v1::JNIScheduler* scheduler =
    (v1::JNIScheduler*) env->GetLongField(thiz, __scheduler);

  scheduler->mesos->send(call);
}

} // extern "C" {
