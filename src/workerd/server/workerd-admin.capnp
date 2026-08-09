# Copyright (c) 2026 Cloudflare, Inc.
# Licensed under the Apache 2.0 license found in the LICENSE file or at:
#     https://opensource.org/licenses/Apache-2.0

@0xe82187e02a763e0d;

using Cxx = import "/capnp/c++.capnp";
using Workerd = import "/workerd/server/workerd.capnp";
$Cxx.namespace("workerd::server::admin");
$Cxx.allowCancellation;

struct AddWorkerResult {
  union {
    added @0 :Void;
    alreadyLoaded @1 :Void;
    restartRequired @2 :Text;
    error @3 :Text;
  }
}

interface WorkerdAdmin {
  # Privileged process control interface served over the connected Unix stream passed by --admin-fd.

  stats @0 () -> (workerServiceCount :UInt32);
  addWorker @1 (serviceName :Text, digest :Data, worker :Workerd.Worker)
      -> (result :AddWorkerResult);
}
