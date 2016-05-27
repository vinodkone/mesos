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

#include <mesos/v1/master.hpp>

#include <mesos/http.hpp>

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/gtest.hpp>
#include <stout/jsonify.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/http.hpp"

#include "tests/mesos.hpp"

using process::Future;
using process::Owned;

using process::http::OK;
using process::http::Response;


using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Return;
using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {

class APITest
  : public MesosTest,
    public WithParamInterface<ContentType> {};


// These tests are parameterized by the content type of the HTTP request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    APITest,
    ::testing::Values(ContentType::PROTOBUF, ContentType::JSON));


TEST_P(APITest, GetFlags)
{
  Try<Owned<cluster::Master>> master = this->StartMaster();
  ASSERT_SOME(master);

  v1::master::Call call;
  call.set_type(v1::master::Call::GET_FLAGS);

  ContentType contentType = GetParam();

  process::http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  headers["Accept"] = stringify(contentType);

  Future<Response> response = process::http::post(
      master.get()->pid,
      "/api/v1",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response)
    << response.get().body;
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
