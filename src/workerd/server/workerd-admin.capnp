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

struct RemoveWorkerResult {
  union {
    removed @0 :Void;
    notFound @1 :Void;
    restartRequired @2 :Text;
    error @3 :Text;
  }
}

struct DispatchScheduledResult {
  union {
    accepted @0 :ScheduledCompletion;
    notFound @1 :Void;
    noHandler @2 :Void;
    error @3 :Text;
  }
}

interface ScheduledCompletion {
  # Completion signal for one dispatched scheduled event. The event runs detached from this
  # capability: dropping it (or losing the admin connection) does not cancel the handler.

  wait @0 () -> (outcome :Text, retry :Bool, deadlineExceeded :Bool);
  # Resolves once the event's handler and its waitUntil tasks have settled. `outcome` carries the
  # event outcome name (e.g. "ok", "exception"); `retry` is false when the handler called
  # noRetry(). A dispatch cut short by its deadline reports `deadlineExceeded` alongside outcome
  # "exceededWallTime" and `retry` true: the cancellation destroys the event's context, so a
  # noRetry() call the handler made is not observable.
}

interface WorkerdAdmin {
  # Privileged process control interface served over the connected Unix stream passed by --admin-fd.

  stats @0 () -> (workerServiceCount :UInt32);
  addWorker @1 (serviceName :Text, digest :Data, worker :Workerd.Worker)
      -> (result :AddWorkerResult);
  removeWorker @2 (serviceName :Text) -> (result :RemoveWorkerResult);
  # Removes a Worker previously loaded through addWorker(). Returns `removed` once the Worker is
  # unrouted: new invocations fail and the serviceName is immediately free for a fresh addWorker().
  # In-flight requests and waitUntil tasks drain in the background, after which the isolate is
  # destroyed. Statically-configured services answer `restartRequired`.

  dispatchScheduled @3 (serviceName :Text, scheduledTimeMs :Int64, cron :Text, deadlineMs :UInt32)
      -> (result :DispatchScheduledResult);
  # Delivers a ScheduledEvent to the named Worker's default entrypoint. Returns `accepted` as soon
  # as the event is running; execution then survives RPC cancellation and admin shutdown, the
  # removal drain counts it, and the process exit drain waits for it. A `deadlineMs` greater than
  # zero cancels the event that long after acceptance; the cancellation takes effect at the
  # handler's next yield to the event loop, so JavaScript that never yields is not preemptible.
  # `noHandler` answers a Worker whose default entrypoint does not export scheduled().
}
