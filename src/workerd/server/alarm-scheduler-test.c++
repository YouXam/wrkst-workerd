// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "alarm-scheduler.h"

#include <kj/async-io.h>
#include <kj/test.h>
#include <kj/timer.h>

namespace workerd::server {
namespace {

// A getActor callback that must never be invoked. These tests only exercise startup migration and
// persistence; no alarm is ever allowed to fire (the timer is never advanced), so getActor should
// not be called.
AlarmScheduler::GetActorFn failingGetActor() {
  return [](const ActorKey&) -> kj::Own<WorkerInterface> {
    KJ_FAIL_ASSERT("getActor should not be called in this test");
  };
}

int64_t toNs(kj::Date date) {
  return (date - kj::UNIX_EPOCH) / kj::NANOSECONDS;
}

// A clock whose current time can be moved forward manually, so a test can drive an alarm to fire.
class AdjustableClock final: public kj::Clock {
 public:
  kj::Date now() const override {
    return time;
  }
  void setTime(kj::Date newTime) {
    time = newTime;
  }

 private:
  kj::Date time = kj::UNIX_EPOCH;
};

// Test double for the alarm scheduler's WorkerInterface; every entry point other than
// runAlarm() and abandonAlarm() is unused here. Tests that block or observe handlers hold an
// AlarmStubState and construct the stub over it; simpler tests use the owning constructors.
struct AlarmStubState {
  kj::Function<void()> onRun = []() {};
  WorkerInterface::AlarmOutcome outcome{
    .retry = false, .retryCountsAgainstLimit = true, .outcome = EventOutcome::OK};
  bool blockRun = false;
  bool runSettled = false;
  bool runCanceled = false;
  uint runCount = 0;
  kj::Maybe<kj::Own<kj::PromiseFulfiller<WorkerInterface::AlarmResult>>> runFulfiller;

  bool blockAbandon = false;
  bool abandonSettled = false;
  bool abandonCanceled = false;
  uint abandonCount = 0;
  kj::Maybe<kj::Own<kj::PromiseFulfiller<kj::Maybe<kj::Date>>>> abandonFulfiller;

  // When set, abandonAlarm() defers to this instead of the block/fulfiller machinery.
  kj::Maybe<kj::Function<kj::Promise<kj::Maybe<kj::Date>>()>> onAbandon;

  WorkerInterface::AlarmResult makeResult() const {
    return {.retry = outcome.retry,
      .retryCountsAgainstLimit = outcome.retryCountsAgainstLimit,
      .outcome = outcome.outcome};
  }

  void finishRun() {
    runSettled = true;
    auto fulfiller = kj::mv(KJ_ASSERT_NONNULL(runFulfiller));
    fulfiller->fulfill(makeResult());
  }

  void finishAbandon() {
    abandonSettled = true;
    auto fulfiller = kj::mv(KJ_ASSERT_NONNULL(abandonFulfiller));
    fulfiller->fulfill(kj::none);
  }
};

class AlarmStubWorkerInterface final: public WorkerInterface {
 public:
  explicit AlarmStubWorkerInterface(AlarmStubState& state): state(state) {}

  explicit AlarmStubWorkerInterface(kj::Function<void()> onAlarm)
      : ownState(kj::heap<AlarmStubState>()),
        state(*ownState) {
    state.onRun = kj::mv(onAlarm);
  }

  AlarmStubWorkerInterface(kj::Function<void()> onAlarm,
      AlarmOutcome outcome,
      kj::Function<kj::Promise<kj::Maybe<kj::Date>>()> onAbandon)
      : ownState(kj::heap<AlarmStubState>()),
        state(*ownState) {
    state.onRun = kj::mv(onAlarm);
    state.outcome = outcome;
    state.onAbandon = kj::mv(onAbandon);
  }

  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    ++state.runCount;
    state.onRun();
    if (!state.blockRun) return state.makeResult();

    auto paf = kj::newPromiseAndFulfiller<AlarmResult>();
    state.runFulfiller = kj::mv(paf.fulfiller);
    return kj::mv(paf.promise).attach(kj::defer([&state = state]() {
      if (!state.runSettled) state.runCanceled = true;
    }));
  }

  kj::Promise<kj::Maybe<kj::Date>> abandonAlarm(kj::Date) override {
    ++state.abandonCount;
    KJ_IF_SOME(f, state.onAbandon) {
      return f();
    }
    if (!state.blockAbandon) return kj::Maybe<kj::Date>(kj::none);

    auto paf = kj::newPromiseAndFulfiller<kj::Maybe<kj::Date>>();
    state.abandonFulfiller = kj::mv(paf.fulfiller);
    return kj::mv(paf.promise).attach(kj::defer([&state = state]() {
      if (!state.abandonSettled) state.abandonCanceled = true;
    }));
  }

  kj::Promise<void> request(kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      kj::HttpService::Response&) override {
    KJ_UNIMPLEMENTED("AlarmStubWorkerInterface::request not used");
  }
  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncIoStream&,
      ConnectResponse&,
      kj::HttpConnectSettings) override {
    KJ_UNIMPLEMENTED("AlarmStubWorkerInterface::connect not used");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    KJ_UNIMPLEMENTED("AlarmStubWorkerInterface::prewarm not used");
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    KJ_UNIMPLEMENTED("AlarmStubWorkerInterface::runScheduled not used");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent>) override {
    KJ_UNIMPLEMENTED("AlarmStubWorkerInterface::customEvent not used");
  }

 private:
  kj::Own<AlarmStubState> ownState;
  AlarmStubState& state;
};

template <typename Func>
void pollUntil(kj::WaitScope& waitScope, Func&& predicate) {
  for (uint i = 0; i < 1000 && !predicate(); ++i) {
    waitScope.poll();
  }
  KJ_ASSERT(predicate());
}

kj::Maybe<kj::Date> readStoredAlarm(
    SqliteDatabase::Vfs& vfs, kj::PathPtr path, kj::StringPtr actorId) {
  SqliteDatabase db(vfs, path.clone(), kj::WriteMode::MODIFY);
  auto query = db.run("SELECT scheduled_time FROM _cf_ALARM WHERE actor_id = ?", actorId);
  if (query.isDone()) return kj::none;
  return kj::UNIX_EPOCH + query.getInt64(0) * kj::NANOSECONDS;
}

struct AlarmSchedulerTest {
  kj::EventLoop loop;
  kj::WaitScope waitScope;
  AdjustableClock clock;
  kj::TimerImpl timer;
  kj::Own<const kj::Directory> dir;
  SqliteDatabase::Vfs vfs;
  kj::Path path = kj::Path({"alarms"});
  AlarmStubState state;
  AlarmScheduler scheduler;

  AlarmSchedulerTest()
      : waitScope(loop),
        timer(kj::origin<kj::TimePoint>()),
        dir(kj::newInMemoryDirectory(kj::nullClock())),
        vfs(*dir),
        scheduler(clock, timer, vfs, path.clone(), [this](const ActorKey&) {
          return kj::Own<WorkerInterface>(kj::heap<AlarmStubWorkerInterface>(state));
        }) {}

  void startAlarm(ActorKey actor, kj::Date scheduledTime) {
    scheduler.setAlarm(actor, scheduledTime);
    if (scheduledTime <= clock.now()) timer.advanceTo(timer.now());
  }
};

KJ_TEST("AlarmScheduler begin revoke stops timers but accepts draining commit updates") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "waiting"_kj};
  auto deletedActor = ActorKey{.actorId = "deleted"_kj};
  auto scheduledTime = kj::UNIX_EPOCH + 1 * kj::HOURS;
  auto updatedTime = kj::UNIX_EPOCH + 2 * kj::HOURS;
  test.startAlarm(actor, scheduledTime);
  test.startAlarm(deletedActor, scheduledTime);
  KJ_EXPECT(test.timer.nextEvent() != kj::none);

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "first revoke"));
  KJ_EXPECT(test.timer.nextEvent() == kj::none);
  KJ_EXPECT_THROW_MESSAGE("first revoke", test.scheduler.getAlarm(actor));
  KJ_EXPECT_THROW_MESSAGE("first revoke", test.scheduler.deleteAll());
  KJ_EXPECT(test.scheduler.setAlarm(actor, updatedTime));
  KJ_EXPECT(test.scheduler.deleteAlarm(deletedActor));
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == updatedTime);
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, deletedActor.actorId) == kj::none);
  KJ_EXPECT(test.timer.nextEvent() == kj::none);

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "second revoke"));
  KJ_EXPECT_THROW_MESSAGE("first revoke", test.scheduler.getAlarm(actor));
  KJ_EXPECT(test.scheduler.finishRevoke().poll(test.waitScope));
}

KJ_TEST("AlarmScheduler finish revoke closes storage while a started alarm drains") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "started"_kj};
  test.state.blockRun = true;
  test.startAlarm(actor, kj::UNIX_EPOCH);
  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "storage revoked"));
  KJ_EXPECT(test.scheduler.hasAlarmForTest(actor));
  auto drain = test.scheduler.finishRevoke();
  KJ_EXPECT(!drain.poll(test.waitScope));
  KJ_EXPECT(!test.state.runCanceled);
  KJ_EXPECT_THROW_MESSAGE("storage revoked", test.scheduler.getStorageForTest().run("SELECT 1"));
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == kj::UNIX_EPOCH);

  test.state.finishRun();
  drain.wait(test.waitScope);
  KJ_EXPECT(!test.state.runCanceled);
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == kj::UNIX_EPOCH);
}

KJ_TEST("AlarmScheduler revoke does not start a queued alarm") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "queued"_kj};
  auto queuedTime = kj::UNIX_EPOCH + 1 * kj::HOURS;
  test.state.blockRun = true;
  test.startAlarm(actor, kj::UNIX_EPOCH);
  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });
  test.scheduler.setAlarm(actor, queuedTime);

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "storage revoked"));
  KJ_EXPECT(test.timer.nextEvent() == kj::none);
  auto drain = test.scheduler.finishRevoke();
  test.state.finishRun();
  drain.wait(test.waitScope);
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == queuedTime);
  KJ_EXPECT(test.timer.nextEvent() == kj::none);
  KJ_EXPECT(test.state.runCount == 1);
}

KJ_TEST("AlarmScheduler revoke cancels an alarm waiting to retry") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "retry"_kj};
  test.state.outcome = {
    .retry = true, .retryCountsAgainstLimit = true, .outcome = EventOutcome::EXCEPTION};
  test.startAlarm(actor, kj::UNIX_EPOCH);
  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });
  for (uint i = 0; i < 10; ++i) test.waitScope.poll();
  KJ_EXPECT(test.timer.nextEvent() != kj::none);

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "storage revoked"));
  KJ_EXPECT(test.timer.nextEvent() == kj::none);
  auto drain = test.scheduler.finishRevoke();
  KJ_EXPECT(drain.poll(test.waitScope));
  KJ_EXPECT(test.state.runCount == 1);
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == kj::UNIX_EPOCH);
}

KJ_TEST("AlarmScheduler revoke during terminal cleanup retains the alarm") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "terminal"_kj};
  test.state.outcome = {
    .retry = true, .retryCountsAgainstLimit = true, .outcome = EventOutcome::EXCEPTION};
  test.state.blockAbandon = true;
  test.startAlarm(actor, kj::UNIX_EPOCH);

  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });
  for (uint expected = 2; expected <= AlarmScheduler::RETRY_MAX_TRIES + 1; ++expected) {
    test.clock.setTime(test.clock.now() + 1 * kj::HOURS);
    test.timer.advanceTo(test.timer.now() + 1 * kj::HOURS);
    pollUntil(test.waitScope, [&]() { return test.state.runCount == expected; });
  }
  pollUntil(test.waitScope, [&]() { return test.state.abandonCount == 1; });

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "storage revoked"));
  KJ_EXPECT(test.scheduler.hasAlarmForTest(actor));
  KJ_EXPECT(!test.state.abandonCanceled);
  test.state.finishAbandon();
  pollUntil(test.waitScope, [&]() { return !test.scheduler.hasActiveAlarmTasksForTest(); });
  KJ_EXPECT(!test.state.abandonCanceled);
  KJ_EXPECT(test.scheduler.hasAlarmForTest(actor));
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == kj::UNIX_EPOCH);
  KJ_EXPECT(test.scheduler.finishRevoke().poll(test.waitScope));
}

KJ_TEST("AlarmScheduler repeated finish revoke shares the pending drain") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "idempotent"_kj};
  test.state.blockRun = true;
  test.startAlarm(actor, kj::UNIX_EPOCH);
  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });

  test.scheduler.beginRevoke(KJ_EXCEPTION(DISCONNECTED, "storage revoked"));
  auto first = test.scheduler.finishRevoke();
  auto second = test.scheduler.finishRevoke();
  KJ_EXPECT(!first.poll(test.waitScope));
  KJ_EXPECT(!second.poll(test.waitScope));

  test.state.finishRun();
  first.wait(test.waitScope);
  second.wait(test.waitScope);
}

KJ_TEST("AlarmScheduler deleteAll cancels a started alarm") {
  AlarmSchedulerTest test;
  auto actor = ActorKey{.actorId = "delete-all"_kj};
  test.state.blockRun = true;
  test.startAlarm(actor, kj::UNIX_EPOCH);
  pollUntil(test.waitScope, [&]() { return test.state.runCount == 1; });

  test.scheduler.deleteAll();
  KJ_EXPECT(test.state.runCanceled);
  KJ_EXPECT(readStoredAlarm(test.vfs, test.path, actor.actorId) == kj::none);
  test.waitScope.poll();
}

KJ_TEST("AlarmScheduler migrates a database created before the actor_name column existed") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto& clock = kj::nullClock();
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());

  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs(*dir);
  kj::Path path({"alarms"});

  // Alarms are scheduled far in the future so they never fire during the test.
  kj::Date scheduledTime = kj::UNIX_EPOCH + 24 * kj::HOURS;

  // Seed a database using the *old* schema, which lacks the `actor_name` column, and insert one
  // pending alarm. This is what a database persisted by an older workerd would look like.
  {
    SqliteDatabase db(vfs, path.clone(), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
    db.run("PRAGMA journal_mode=WAL;");
    db.run(R"(
      CREATE TABLE _cf_ALARM (
        actor_id TEXT PRIMARY KEY,
        scheduled_time INTEGER
      ) WITHOUT ROWID;
    )");
    db.run("INSERT INTO _cf_ALARM VALUES (?, ?)", "old-actor"_kj, toNs(scheduledTime));
  }

  // Constructing the scheduler must migrate the schema (adding actor_name) and load the pre-existing
  // alarm without error.
  {
    AlarmScheduler scheduler(clock, timer, vfs, path.clone(), failingGetActor());

    auto alarm = scheduler.getAlarm(ActorKey{.actorId = "old-actor"_kj});
    KJ_EXPECT(KJ_ASSERT_NONNULL(alarm) == scheduledTime);
  }

  // The migrated database should now have the actor_name column, populated with NULL for the row
  // that predated it.
  {
    SqliteDatabase db(vfs, path.clone(), kj::WriteMode::MODIFY);
    auto query = db.run("SELECT actor_name FROM _cf_ALARM WHERE actor_id = ?", "old-actor"_kj);
    KJ_ASSERT(!query.isDone());
    KJ_EXPECT(query.getMaybeText(0) == kj::none);
  }
}

KJ_TEST("AlarmScheduler persists actor_name and preserves it across a nameless update") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto& clock = kj::nullClock();
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());

  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs(*dir);
  kj::Path path({"alarms"});

  kj::Date scheduledTime = kj::UNIX_EPOCH + 24 * kj::HOURS;
  kj::Date updatedTime = kj::UNIX_EPOCH + 48 * kj::HOURS;

  {
    AlarmScheduler scheduler(clock, timer, vfs, path.clone(), failingGetActor());

    // A named actor persists its name alongside the alarm.
    scheduler.setAlarm(ActorKey{.actorId = "named-actor"_kj, .name = "my-name"_kj}, scheduledTime);

    // A subsequent update without a name must not clear the previously-persisted name. This mirrors
    // how the alarm scheduler is driven: the name is only supplied when the actor is created via
    // getByName(), but later setAlarm() calls (e.g. from an already-running alarm handler) may not
    // carry it.
    scheduler.setAlarm(ActorKey{.actorId = "named-actor"_kj}, updatedTime);
  }

  // Reopen the database directly to confirm both the updated time and the retained name.
  {
    SqliteDatabase db(vfs, path.clone(), kj::WriteMode::MODIFY);
    auto query = db.run(
        "SELECT scheduled_time, actor_name FROM _cf_ALARM WHERE actor_id = ?", "named-actor"_kj);
    KJ_ASSERT(!query.isDone());
    KJ_EXPECT(query.getInt64(0) == toNs(updatedTime));
    KJ_EXPECT(KJ_ASSERT_NONNULL(query.getMaybeText(1)) == "my-name"_kj);
  }

  // A fresh scheduler should load the named alarm from disk without error.
  {
    AlarmScheduler scheduler(clock, timer, vfs, path.clone(), failingGetActor());
    auto alarm = scheduler.getAlarm(ActorKey{.actorId = "named-actor"_kj});
    KJ_EXPECT(KJ_ASSERT_NONNULL(alarm) == updatedTime);
  }
}

KJ_TEST("AlarmScheduler restores the persisted actor_name onto the ActorKey when an alarm fires") {
  // The in-memory name loaded by loadAlarmsFromDb is only observable when an alarm actually fires
  // (it is handed to getActor so the reconstructed ID exposes ctx.id.name). This test seeds a named
  // alarm, drops the scheduler, then constructs a fresh scheduler that must load the alarm from disk
  // and fire it, and verifies the name reaches getActor.
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  AdjustableClock clock;
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());

  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs(*dir);
  kj::Path path({"alarms"});

  kj::Date scheduledTime = kj::UNIX_EPOCH + 1 * kj::HOURS;

  // Persist a named alarm, then drop the scheduler so nothing about the name survives in memory.
  {
    AlarmScheduler scheduler(clock, timer, vfs, path.clone(), failingGetActor());
    scheduler.setAlarm(ActorKey{.actorId = "named-actor"_kj, .name = "my-name"_kj}, scheduledTime);
  }

  // A fresh scheduler must reload the alarm (and its name) from disk.
  kj::Maybe<kj::String> observedName;
  bool fired = false;
  AlarmStubState state;
  state.onRun = [&fired]() { fired = true; };
  auto getActor = [&](const ActorKey& actor) -> kj::Own<WorkerInterface> {
    observedName = actor.name.map([](kj::StringPtr n) { return kj::str(n); });
    return kj::heap<AlarmStubWorkerInterface>(state);
  };

  AlarmScheduler scheduler(clock, timer, vfs, path.clone(), kj::mv(getActor));

  // Advance both the wall clock and the timer past the scheduled time so the alarm fires.
  clock.setTime(scheduledTime);
  timer.advanceTo(kj::origin<kj::TimePoint>() + (scheduledTime - kj::UNIX_EPOCH));

  // Pump the event loop until the alarm task has run (bounded so a regression can't hang forever).
  for (uint i = 0; i < 100 && !fired; i++) {
    waitScope.poll();
  }

  KJ_EXPECT(fired);
  KJ_EXPECT(KJ_ASSERT_NONNULL(observedName) == "my-name"_kj);
}

KJ_TEST("AlarmScheduler abandons an alarm when ctx.abort() sets retryAlarm to false") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  AdjustableClock clock;
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());

  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs(*dir);
  kj::Path path({"alarms"});
  auto scheduledTime = kj::UNIX_EPOCH + 1 * kj::HOURS;
  auto actor = ActorKey{.actorId = "terminal-actor"_kj};

  bool fired = false;
  bool abandoned = false;
  auto getActor = [&](const ActorKey&) -> kj::Own<WorkerInterface> {
    return kj::heap<AlarmStubWorkerInterface>([&fired]() { fired = true; },
        WorkerInterface::AlarmOutcome{
          .retry = false,
          .retryCountsAgainstLimit = true,
          .outcome = EventOutcome::ABORTED,
        },
        [&abandoned]() {
      abandoned = true;
      return kj::Maybe<kj::Date>(kj::none);
    });
  };

  AlarmScheduler scheduler(clock, timer, vfs, path.clone(), kj::mv(getActor));
  scheduler.setAlarm(actor, scheduledTime);

  clock.setTime(scheduledTime);
  timer.advanceTo(kj::origin<kj::TimePoint>() + (scheduledTime - kj::UNIX_EPOCH));
  for (uint i = 0; i < 100 && !abandoned; i++) {
    waitScope.poll();
  }

  KJ_EXPECT(fired);
  KJ_EXPECT(abandoned);
  KJ_EXPECT(scheduler.getAlarm(actor) == kj::none);
}

KJ_TEST("AlarmScheduler preserves an alarm queued while abandonment is pending") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  AdjustableClock clock;
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());

  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs(*dir);
  kj::Path path({"alarms"});
  auto scheduledTime = kj::UNIX_EPOCH + 1 * kj::HOURS;
  auto replacementTime = kj::UNIX_EPOCH + 2 * kj::HOURS;
  auto actor = ActorKey{.actorId = "terminal-actor"_kj};
  auto pendingAbandon = kj::newPromiseAndFulfiller<kj::Maybe<kj::Date>>();
  auto abandonPromise = pendingAbandon.promise.fork();
  bool abandonStarted = false;

  {
    auto getActor = [&](const ActorKey&) -> kj::Own<WorkerInterface> {
      return kj::heap<AlarmStubWorkerInterface>([]() {},
          WorkerInterface::AlarmOutcome{
            .retry = false,
            .retryCountsAgainstLimit = true,
            .outcome = EventOutcome::ABORTED,
          },
          [&abandonStarted, &abandonPromise]() {
        abandonStarted = true;
        return abandonPromise.addBranch();
      });
    };

    AlarmScheduler scheduler(clock, timer, vfs, path.clone(), kj::mv(getActor));
    scheduler.setAlarm(actor, scheduledTime);

    clock.setTime(scheduledTime);
    timer.advanceTo(kj::origin<kj::TimePoint>() + (scheduledTime - kj::UNIX_EPOCH));
    for (uint i = 0; i < 100 && !abandonStarted; i++) {
      waitScope.poll();
    }

    KJ_EXPECT(abandonStarted);
    scheduler.setAlarm(actor, replacementTime);
    pendingAbandon.fulfiller->fulfill(kj::none);

    for (uint i = 0; i < 100 && scheduler.getAlarm(actor) != replacementTime; i++) {
      waitScope.poll();
    }
    KJ_EXPECT(scheduler.getAlarm(actor) == replacementTime);
  }

  AlarmScheduler scheduler(clock, timer, vfs, path.clone(), failingGetActor());
  KJ_EXPECT(scheduler.getAlarm(actor) == replacementTime);
}

}  // namespace
}  // namespace workerd::server
