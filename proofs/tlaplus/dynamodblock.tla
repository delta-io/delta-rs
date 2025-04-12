---------------------------- MODULE dynamodblock ----------------------------

EXTENDS TLC, Integers, Sequences, FiniteSets

CONSTANT NULL
    , WRITERS  \* number of writers defined as model values set
    , LOOP_COUNT  \* how many loop iteration each writer should go through
    \* set TIME_TICK_UNIT to 0 to disable lock expiration check, > 1 to enable it
    , TIME_TICK_UNIT

ASSUME Cardinality(WRITERS) > 0
ASSUME TIME_TICK_UNIT \in Nat
ASSUME LOOP_COUNT \in Nat \ {0}

(*--algorithm dynamodblock
variables
    shared_counter = 0,
    time_now = 0,
    active_writer = Cardinality(WRITERS),

    \* local record version is only guaranteed to be unique within worker process, not globally
    local_record_version = [ x \in WRITERS |-> 0 ],
    local_counter = [ x \in WRITERS |-> 0 ],
    re_upsert_err = [ x \in WRITERS |-> NULL ],

    \* dynamodb lock item
    lock_owner = NULL,
    lock_released = NULL,
    lock_record_version = NULL,
    lock_duration = NULL;

define
    \* Sum all numbers in a set
    SumSet(set) ==
        LET F[s \in SUBSET set] ==
            IF s = {}
            THEN 0
            ELSE
                LET x == CHOOSE x \in s : TRUE
                IN x + F[s \ {x}]
        IN F[set]

    \* extract struct values into a set
    StructValues(st) == {st[x] : x \in DOMAIN st}

    \* invariants
    ConsistentCount == shared_counter >= SumSet(StructValues(local_counter))
    \* properties
    NoMissCount == <>[](shared_counter = Cardinality(WRITERS) * LOOP_COUNT)
end define;

macro Sleep() begin
    \* sleep becomes a no op when timer is disabled in the spec
    if TIME_TICK_UNIT > 0 then
        next_time_tick := time_now + TIME_TICK_UNIT;
        await time_now = next_time_tick;
    end if;
end macro;

macro ReleaseLock(owner, record_version) begin
    if (lock_owner = owner /\ lock_record_version = record_version) then
        lock_released := TRUE;
    end if;
end macro;

macro SetNewCachedLockAndTimeout() begin
    cached_lock_version := lock_record_version;
    cached_lock_lookup_time := time_now;
    cached_lock_duration := lock_duration;
end macro;

procedure DynamodbUpsertLock(
    owner=NULL,
    record_version=NULL,
    cached_lock_lookup_time=NULL,
    cached_lock_duration=NULL)
begin
  cond_put:  \* we should only try to acquire a lock in the following scenarios
             if lock_released = TRUE  \* lock has been released
                 \/ lock_owner = NULL  \* lock has never been acquired before
                 \/ (  \* lock expired
                     /\ cached_lock_lookup_time /= NULL
                     /\ time_now - cached_lock_lookup_time > cached_lock_duration) then

                 lock_owner := owner;
                 lock_released := NULL;
                 lock_record_version := record_version;
                 lock_duration := 20;
                 re_upsert_err[self] := NULL;
             else
                 \* conditional put failed due to concurrent lock acquire from another writer
                 re_upsert_err[self] := TRUE;
             end if;
             return;
end procedure;

procedure AcquireLock(owner=NULL, record_version=NULL)
    variables
        next_time_tick = NULL,
        cached_lock_lookup_time = NULL,
        cached_lock_duration = NULL,
        cached_lock_version = NULL;
begin
  validate:  assert owner /= NULL;
             assert record_version /= NULL;
  try_loop:  while TRUE do
               \* begin of try_acquire_lock
               if (lock_owner = NULL \/ lock_released = TRUE) then
                  \* lock doesn't exist or already released
                  \* try acquiring the dynamodb row now
createlock:       call DynamodbUpsertLock(owner, record_version, cached_lock_lookup_time, cached_lock_duration);
   dyn_err:       if re_upsert_err[self] /= NULL then
                      goto retry;
                  end if;
    locked:       return;
               else
                  \* lock is currently being held by someone else
                  assert lock_owner /= self;
                  assert lock_record_version /= NULL;

                  if (cached_lock_version = NULL) then
                    \* cache lock look up time to start local expiration timer
                    SetNewCachedLockAndTimeout();
                  elsif (cached_lock_version = lock_record_version) then
                    \* cached lock is still active, check for expiration
                    if time_now - cached_lock_lookup_time > cached_lock_duration then
                        \* lock expired, try acquire it
  takelock:             call DynamodbUpsertLock(owner, record_version, cached_lock_lookup_time, cached_lock_duration);
  dyn_err2:                 if re_upsert_err[self] /= NULL then
                            goto retry;
                        end if;
   locked2:             return;
                    else
                        \* cached lock has not expired yet
                        \* sleep and retry
                        goto retry;
                    end if;
                  else
                    \* lock version mismatch, lock changed hands
                    \* update cached lock to the new lock
                    SetNewCachedLockAndTimeout();
                  end if;
               end if;
     retry:    Sleep();
             end while;

             return;
end procedure;

\* if our lock works as expected, steps in this procedure should be executed atomically
procedure CriticalSection()
    variables
        local_count_value = NULL;
begin
      read:  local_count_value := shared_counter + 1;
             \* lock owner could crash or pause here before updating the shared_counter
    commit:  shared_counter := local_count_value;
             return;
end procedure;

\* concurrent writers loop that tries to access a critical section guarded by dynamodb lock
fair process Writer \in WRITERS
begin
writerloop:  while (local_counter[self] < LOOP_COUNT) do
                 \* generate the next record version for lock
                 local_record_version[self] := local_record_version[self] + 1;
      lock:      call AcquireLock(self, local_record_version[self]);
  csection:      call CriticalSection();
   release:      if TIME_TICK_UNIT = 0 then
                     \* when expiration check is disabled, lock owner should not
                     \* change without explicit release
                     assert lock_owner = self;
                 end if;
                 ReleaseLock(self, local_record_version[self]);
loop_count:      local_counter[self] := local_counter[self] + 1;
             end while;
             active_writer := active_writer - 1;
end process;

\* this process simulates flow of time using a global time_now counter
fair process Timer = TIME_TICK_UNIT
begin
    tick:  while TIME_TICK_UNIT > 0 /\ active_writer > 0 do
               time_now := time_now + TIME_TICK_UNIT;
           end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "27144e93" /\ chksum(tla) = "26d8d366")
\* Procedure variable cached_lock_lookup_time of procedure AcquireLock at line 100 col 9 changed to cached_lock_lookup_time_
\* Procedure variable cached_lock_duration of procedure AcquireLock at line 101 col 9 changed to cached_lock_duration_
\* Parameter owner of procedure DynamodbUpsertLock at line 73 col 5 changed to owner_
\* Parameter record_version of procedure DynamodbUpsertLock at line 74 col 5 changed to record_version_
VARIABLES shared_counter, time_now, active_writer, local_record_version, 
          local_counter, re_upsert_err, lock_owner, lock_released, 
          lock_record_version, lock_duration, pc, stack

(* define statement *)
SumSet(set) ==
    LET F[s \in SUBSET set] ==
        IF s = {}
        THEN 0
        ELSE
            LET x == CHOOSE x \in s : TRUE
            IN x + F[s \ {x}]
    IN F[set]


StructValues(st) == {st[x] : x \in DOMAIN st}


ConsistentCount == shared_counter >= SumSet(StructValues(local_counter))

NoMissCount == <>[](shared_counter = Cardinality(WRITERS) * LOOP_COUNT)

VARIABLES owner_, record_version_, cached_lock_lookup_time, 
          cached_lock_duration, owner, record_version, next_time_tick, 
          cached_lock_lookup_time_, cached_lock_duration_, 
          cached_lock_version, local_count_value

vars == << shared_counter, time_now, active_writer, local_record_version, 
           local_counter, re_upsert_err, lock_owner, lock_released, 
           lock_record_version, lock_duration, pc, stack, owner_, 
           record_version_, cached_lock_lookup_time, cached_lock_duration, 
           owner, record_version, next_time_tick, cached_lock_lookup_time_, 
           cached_lock_duration_, cached_lock_version, local_count_value >>

ProcSet == (WRITERS) \cup {TIME_TICK_UNIT}

Init == (* Global variables *)
        /\ shared_counter = 0
        /\ time_now = 0
        /\ active_writer = Cardinality(WRITERS)
        /\ local_record_version = [ x \in WRITERS |-> 0 ]
        /\ local_counter = [ x \in WRITERS |-> 0 ]
        /\ re_upsert_err = [ x \in WRITERS |-> NULL ]
        /\ lock_owner = NULL
        /\ lock_released = NULL
        /\ lock_record_version = NULL
        /\ lock_duration = NULL
        (* Procedure DynamodbUpsertLock *)
        /\ owner_ = [ self \in ProcSet |-> NULL]
        /\ record_version_ = [ self \in ProcSet |-> NULL]
        /\ cached_lock_lookup_time = [ self \in ProcSet |-> NULL]
        /\ cached_lock_duration = [ self \in ProcSet |-> NULL]
        (* Procedure AcquireLock *)
        /\ owner = [ self \in ProcSet |-> NULL]
        /\ record_version = [ self \in ProcSet |-> NULL]
        /\ next_time_tick = [ self \in ProcSet |-> NULL]
        /\ cached_lock_lookup_time_ = [ self \in ProcSet |-> NULL]
        /\ cached_lock_duration_ = [ self \in ProcSet |-> NULL]
        /\ cached_lock_version = [ self \in ProcSet |-> NULL]
        (* Procedure CriticalSection *)
        /\ local_count_value = [ self \in ProcSet |-> NULL]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in WRITERS -> "writerloop"
                                        [] self = TIME_TICK_UNIT -> "tick"]

cond_put(self) == /\ pc[self] = "cond_put"
                  /\ IF lock_released = TRUE
                         \/ lock_owner = NULL
                         \/ (
                             /\ cached_lock_lookup_time[self] /= NULL
                             /\ time_now - cached_lock_lookup_time[self] > cached_lock_duration[self])
                        THEN /\ lock_owner' = owner_[self]
                             /\ lock_released' = NULL
                             /\ lock_record_version' = record_version_[self]
                             /\ lock_duration' = 20
                             /\ re_upsert_err' = [re_upsert_err EXCEPT ![self] = NULL]
                        ELSE /\ re_upsert_err' = [re_upsert_err EXCEPT ![self] = TRUE]
                             /\ UNCHANGED << lock_owner, lock_released, 
                                             lock_record_version, 
                                             lock_duration >>
                  /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                  /\ owner_' = [owner_ EXCEPT ![self] = Head(stack[self]).owner_]
                  /\ record_version_' = [record_version_ EXCEPT ![self] = Head(stack[self]).record_version_]
                  /\ cached_lock_lookup_time' = [cached_lock_lookup_time EXCEPT ![self] = Head(stack[self]).cached_lock_lookup_time]
                  /\ cached_lock_duration' = [cached_lock_duration EXCEPT ![self] = Head(stack[self]).cached_lock_duration]
                  /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, owner, 
                                  record_version, next_time_tick, 
                                  cached_lock_lookup_time_, 
                                  cached_lock_duration_, cached_lock_version, 
                                  local_count_value >>

DynamodbUpsertLock(self) == cond_put(self)

validate(self) == /\ pc[self] = "validate"
                  /\ Assert(owner[self] /= NULL, 
                            "Failure of assertion at line 104, column 14.")
                  /\ Assert(record_version[self] /= NULL, 
                            "Failure of assertion at line 105, column 14.")
                  /\ pc' = [pc EXCEPT ![self] = "try_loop"]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, 
                                  re_upsert_err, lock_owner, lock_released, 
                                  lock_record_version, lock_duration, stack, 
                                  owner_, record_version_, 
                                  cached_lock_lookup_time, 
                                  cached_lock_duration, owner, record_version, 
                                  next_time_tick, cached_lock_lookup_time_, 
                                  cached_lock_duration_, cached_lock_version, 
                                  local_count_value >>

try_loop(self) == /\ pc[self] = "try_loop"
                  /\ IF (lock_owner = NULL \/ lock_released = TRUE)
                        THEN /\ pc' = [pc EXCEPT ![self] = "createlock"]
                             /\ UNCHANGED << cached_lock_lookup_time_, 
                                             cached_lock_duration_, 
                                             cached_lock_version >>
                        ELSE /\ Assert(lock_owner /= self, 
                                       "Failure of assertion at line 118, column 19.")
                             /\ Assert(lock_record_version /= NULL, 
                                       "Failure of assertion at line 119, column 19.")
                             /\ IF (cached_lock_version[self] = NULL)
                                   THEN /\ cached_lock_version' = [cached_lock_version EXCEPT ![self] = lock_record_version]
                                        /\ cached_lock_lookup_time_' = [cached_lock_lookup_time_ EXCEPT ![self] = time_now]
                                        /\ cached_lock_duration_' = [cached_lock_duration_ EXCEPT ![self] = lock_duration]
                                        /\ pc' = [pc EXCEPT ![self] = "retry"]
                                   ELSE /\ IF (cached_lock_version[self] = lock_record_version)
                                              THEN /\ IF time_now - cached_lock_lookup_time_[self] > cached_lock_duration_[self]
                                                         THEN /\ pc' = [pc EXCEPT ![self] = "takelock"]
                                                         ELSE /\ pc' = [pc EXCEPT ![self] = "retry"]
                                                   /\ UNCHANGED << cached_lock_lookup_time_, 
                                                                   cached_lock_duration_, 
                                                                   cached_lock_version >>
                                              ELSE /\ cached_lock_version' = [cached_lock_version EXCEPT ![self] = lock_record_version]
                                                   /\ cached_lock_lookup_time_' = [cached_lock_lookup_time_ EXCEPT ![self] = time_now]
                                                   /\ cached_lock_duration_' = [cached_lock_duration_ EXCEPT ![self] = lock_duration]
                                                   /\ pc' = [pc EXCEPT ![self] = "retry"]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, 
                                  re_upsert_err, lock_owner, lock_released, 
                                  lock_record_version, lock_duration, stack, 
                                  owner_, record_version_, 
                                  cached_lock_lookup_time, 
                                  cached_lock_duration, owner, record_version, 
                                  next_time_tick, local_count_value >>

retry(self) == /\ pc[self] = "retry"
               /\ IF TIME_TICK_UNIT > 0
                     THEN /\ next_time_tick' = [next_time_tick EXCEPT ![self] = time_now + TIME_TICK_UNIT]
                          /\ time_now = next_time_tick'[self]
                     ELSE /\ TRUE
                          /\ UNCHANGED next_time_tick
               /\ pc' = [pc EXCEPT ![self] = "try_loop"]
               /\ UNCHANGED << shared_counter, time_now, active_writer, 
                               local_record_version, local_counter, 
                               re_upsert_err, lock_owner, lock_released, 
                               lock_record_version, lock_duration, stack, 
                               owner_, record_version_, 
                               cached_lock_lookup_time, cached_lock_duration, 
                               owner, record_version, cached_lock_lookup_time_, 
                               cached_lock_duration_, cached_lock_version, 
                               local_count_value >>

createlock(self) == /\ pc[self] = "createlock"
                    /\ /\ cached_lock_duration' = [cached_lock_duration EXCEPT ![self] = cached_lock_duration_[self]]
                       /\ cached_lock_lookup_time' = [cached_lock_lookup_time EXCEPT ![self] = cached_lock_lookup_time_[self]]
                       /\ owner_' = [owner_ EXCEPT ![self] = owner[self]]
                       /\ record_version_' = [record_version_ EXCEPT ![self] = record_version[self]]
                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DynamodbUpsertLock",
                                                                pc        |->  "dyn_err",
                                                                owner_    |->  owner_[self],
                                                                record_version_ |->  record_version_[self],
                                                                cached_lock_lookup_time |->  cached_lock_lookup_time[self],
                                                                cached_lock_duration |->  cached_lock_duration[self] ] >>
                                                            \o stack[self]]
                    /\ pc' = [pc EXCEPT ![self] = "cond_put"]
                    /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                    local_record_version, local_counter, 
                                    re_upsert_err, lock_owner, lock_released, 
                                    lock_record_version, lock_duration, owner, 
                                    record_version, next_time_tick, 
                                    cached_lock_lookup_time_, 
                                    cached_lock_duration_, cached_lock_version, 
                                    local_count_value >>

dyn_err(self) == /\ pc[self] = "dyn_err"
                 /\ IF re_upsert_err[self] /= NULL
                       THEN /\ pc' = [pc EXCEPT ![self] = "retry"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "locked"]
                 /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                 local_record_version, local_counter, 
                                 re_upsert_err, lock_owner, lock_released, 
                                 lock_record_version, lock_duration, stack, 
                                 owner_, record_version_, 
                                 cached_lock_lookup_time, cached_lock_duration, 
                                 owner, record_version, next_time_tick, 
                                 cached_lock_lookup_time_, 
                                 cached_lock_duration_, cached_lock_version, 
                                 local_count_value >>

locked(self) == /\ pc[self] = "locked"
                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                /\ next_time_tick' = [next_time_tick EXCEPT ![self] = Head(stack[self]).next_time_tick]
                /\ cached_lock_lookup_time_' = [cached_lock_lookup_time_ EXCEPT ![self] = Head(stack[self]).cached_lock_lookup_time_]
                /\ cached_lock_duration_' = [cached_lock_duration_ EXCEPT ![self] = Head(stack[self]).cached_lock_duration_]
                /\ cached_lock_version' = [cached_lock_version EXCEPT ![self] = Head(stack[self]).cached_lock_version]
                /\ owner' = [owner EXCEPT ![self] = Head(stack[self]).owner]
                /\ record_version' = [record_version EXCEPT ![self] = Head(stack[self]).record_version]
                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                local_record_version, local_counter, 
                                re_upsert_err, lock_owner, lock_released, 
                                lock_record_version, lock_duration, owner_, 
                                record_version_, cached_lock_lookup_time, 
                                cached_lock_duration, local_count_value >>

takelock(self) == /\ pc[self] = "takelock"
                  /\ /\ cached_lock_duration' = [cached_lock_duration EXCEPT ![self] = cached_lock_duration_[self]]
                     /\ cached_lock_lookup_time' = [cached_lock_lookup_time EXCEPT ![self] = cached_lock_lookup_time_[self]]
                     /\ owner_' = [owner_ EXCEPT ![self] = owner[self]]
                     /\ record_version_' = [record_version_ EXCEPT ![self] = record_version[self]]
                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DynamodbUpsertLock",
                                                              pc        |->  "dyn_err2",
                                                              owner_    |->  owner_[self],
                                                              record_version_ |->  record_version_[self],
                                                              cached_lock_lookup_time |->  cached_lock_lookup_time[self],
                                                              cached_lock_duration |->  cached_lock_duration[self] ] >>
                                                          \o stack[self]]
                  /\ pc' = [pc EXCEPT ![self] = "cond_put"]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, 
                                  re_upsert_err, lock_owner, lock_released, 
                                  lock_record_version, lock_duration, owner, 
                                  record_version, next_time_tick, 
                                  cached_lock_lookup_time_, 
                                  cached_lock_duration_, cached_lock_version, 
                                  local_count_value >>

dyn_err2(self) == /\ pc[self] = "dyn_err2"
                  /\ IF re_upsert_err[self] /= NULL
                        THEN /\ pc' = [pc EXCEPT ![self] = "retry"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "locked2"]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, 
                                  re_upsert_err, lock_owner, lock_released, 
                                  lock_record_version, lock_duration, stack, 
                                  owner_, record_version_, 
                                  cached_lock_lookup_time, 
                                  cached_lock_duration, owner, record_version, 
                                  next_time_tick, cached_lock_lookup_time_, 
                                  cached_lock_duration_, cached_lock_version, 
                                  local_count_value >>

locked2(self) == /\ pc[self] = "locked2"
                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                 /\ next_time_tick' = [next_time_tick EXCEPT ![self] = Head(stack[self]).next_time_tick]
                 /\ cached_lock_lookup_time_' = [cached_lock_lookup_time_ EXCEPT ![self] = Head(stack[self]).cached_lock_lookup_time_]
                 /\ cached_lock_duration_' = [cached_lock_duration_ EXCEPT ![self] = Head(stack[self]).cached_lock_duration_]
                 /\ cached_lock_version' = [cached_lock_version EXCEPT ![self] = Head(stack[self]).cached_lock_version]
                 /\ owner' = [owner EXCEPT ![self] = Head(stack[self]).owner]
                 /\ record_version' = [record_version EXCEPT ![self] = Head(stack[self]).record_version]
                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                 /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                 local_record_version, local_counter, 
                                 re_upsert_err, lock_owner, lock_released, 
                                 lock_record_version, lock_duration, owner_, 
                                 record_version_, cached_lock_lookup_time, 
                                 cached_lock_duration, local_count_value >>

AcquireLock(self) == validate(self) \/ try_loop(self) \/ retry(self)
                        \/ createlock(self) \/ dyn_err(self)
                        \/ locked(self) \/ takelock(self) \/ dyn_err2(self)
                        \/ locked2(self)

read(self) == /\ pc[self] = "read"
              /\ local_count_value' = [local_count_value EXCEPT ![self] = shared_counter + 1]
              /\ pc' = [pc EXCEPT ![self] = "commit"]
              /\ UNCHANGED << shared_counter, time_now, active_writer, 
                              local_record_version, local_counter, 
                              re_upsert_err, lock_owner, lock_released, 
                              lock_record_version, lock_duration, stack, 
                              owner_, record_version_, cached_lock_lookup_time, 
                              cached_lock_duration, owner, record_version, 
                              next_time_tick, cached_lock_lookup_time_, 
                              cached_lock_duration_, cached_lock_version >>

commit(self) == /\ pc[self] = "commit"
                /\ shared_counter' = local_count_value[self]
                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                /\ local_count_value' = [local_count_value EXCEPT ![self] = Head(stack[self]).local_count_value]
                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                /\ UNCHANGED << time_now, active_writer, local_record_version, 
                                local_counter, re_upsert_err, lock_owner, 
                                lock_released, lock_record_version, 
                                lock_duration, owner_, record_version_, 
                                cached_lock_lookup_time, cached_lock_duration, 
                                owner, record_version, next_time_tick, 
                                cached_lock_lookup_time_, 
                                cached_lock_duration_, cached_lock_version >>

CriticalSection(self) == read(self) \/ commit(self)

writerloop(self) == /\ pc[self] = "writerloop"
                    /\ IF (local_counter[self] < LOOP_COUNT)
                          THEN /\ local_record_version' = [local_record_version EXCEPT ![self] = local_record_version[self] + 1]
                               /\ pc' = [pc EXCEPT ![self] = "lock"]
                               /\ UNCHANGED active_writer
                          ELSE /\ active_writer' = active_writer - 1
                               /\ pc' = [pc EXCEPT ![self] = "Done"]
                               /\ UNCHANGED local_record_version
                    /\ UNCHANGED << shared_counter, time_now, local_counter, 
                                    re_upsert_err, lock_owner, lock_released, 
                                    lock_record_version, lock_duration, stack, 
                                    owner_, record_version_, 
                                    cached_lock_lookup_time, 
                                    cached_lock_duration, owner, 
                                    record_version, next_time_tick, 
                                    cached_lock_lookup_time_, 
                                    cached_lock_duration_, cached_lock_version, 
                                    local_count_value >>

lock(self) == /\ pc[self] = "lock"
              /\ /\ owner' = [owner EXCEPT ![self] = self]
                 /\ record_version' = [record_version EXCEPT ![self] = local_record_version[self]]
                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AcquireLock",
                                                          pc        |->  "csection",
                                                          next_time_tick |->  next_time_tick[self],
                                                          cached_lock_lookup_time_ |->  cached_lock_lookup_time_[self],
                                                          cached_lock_duration_ |->  cached_lock_duration_[self],
                                                          cached_lock_version |->  cached_lock_version[self],
                                                          owner     |->  owner[self],
                                                          record_version |->  record_version[self] ] >>
                                                      \o stack[self]]
              /\ next_time_tick' = [next_time_tick EXCEPT ![self] = NULL]
              /\ cached_lock_lookup_time_' = [cached_lock_lookup_time_ EXCEPT ![self] = NULL]
              /\ cached_lock_duration_' = [cached_lock_duration_ EXCEPT ![self] = NULL]
              /\ cached_lock_version' = [cached_lock_version EXCEPT ![self] = NULL]
              /\ pc' = [pc EXCEPT ![self] = "validate"]
              /\ UNCHANGED << shared_counter, time_now, active_writer, 
                              local_record_version, local_counter, 
                              re_upsert_err, lock_owner, lock_released, 
                              lock_record_version, lock_duration, owner_, 
                              record_version_, cached_lock_lookup_time, 
                              cached_lock_duration, local_count_value >>

csection(self) == /\ pc[self] = "csection"
                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CriticalSection",
                                                           pc        |->  "release",
                                                           local_count_value |->  local_count_value[self] ] >>
                                                       \o stack[self]]
                  /\ local_count_value' = [local_count_value EXCEPT ![self] = NULL]
                  /\ pc' = [pc EXCEPT ![self] = "read"]
                  /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                  local_record_version, local_counter, 
                                  re_upsert_err, lock_owner, lock_released, 
                                  lock_record_version, lock_duration, owner_, 
                                  record_version_, cached_lock_lookup_time, 
                                  cached_lock_duration, owner, record_version, 
                                  next_time_tick, cached_lock_lookup_time_, 
                                  cached_lock_duration_, cached_lock_version >>

release(self) == /\ pc[self] = "release"
                 /\ IF TIME_TICK_UNIT = 0
                       THEN /\ Assert(lock_owner = self, 
                                      "Failure of assertion at line 172, column 22.")
                       ELSE /\ TRUE
                 /\ IF (lock_owner = self /\ lock_record_version = (local_record_version[self]))
                       THEN /\ lock_released' = TRUE
                       ELSE /\ TRUE
                            /\ UNCHANGED lock_released
                 /\ pc' = [pc EXCEPT ![self] = "loop_count"]
                 /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                 local_record_version, local_counter, 
                                 re_upsert_err, lock_owner, 
                                 lock_record_version, lock_duration, stack, 
                                 owner_, record_version_, 
                                 cached_lock_lookup_time, cached_lock_duration, 
                                 owner, record_version, next_time_tick, 
                                 cached_lock_lookup_time_, 
                                 cached_lock_duration_, cached_lock_version, 
                                 local_count_value >>

loop_count(self) == /\ pc[self] = "loop_count"
                    /\ local_counter' = [local_counter EXCEPT ![self] = local_counter[self] + 1]
                    /\ pc' = [pc EXCEPT ![self] = "writerloop"]
                    /\ UNCHANGED << shared_counter, time_now, active_writer, 
                                    local_record_version, re_upsert_err, 
                                    lock_owner, lock_released, 
                                    lock_record_version, lock_duration, stack, 
                                    owner_, record_version_, 
                                    cached_lock_lookup_time, 
                                    cached_lock_duration, owner, 
                                    record_version, next_time_tick, 
                                    cached_lock_lookup_time_, 
                                    cached_lock_duration_, cached_lock_version, 
                                    local_count_value >>

Writer(self) == writerloop(self) \/ lock(self) \/ csection(self)
                   \/ release(self) \/ loop_count(self)

tick == /\ pc[TIME_TICK_UNIT] = "tick"
        /\ IF TIME_TICK_UNIT > 0 /\ active_writer > 0
              THEN /\ time_now' = time_now + TIME_TICK_UNIT
                   /\ pc' = [pc EXCEPT ![TIME_TICK_UNIT] = "tick"]
              ELSE /\ pc' = [pc EXCEPT ![TIME_TICK_UNIT] = "Done"]
                   /\ UNCHANGED time_now
        /\ UNCHANGED << shared_counter, active_writer, local_record_version, 
                        local_counter, re_upsert_err, lock_owner, 
                        lock_released, lock_record_version, lock_duration, 
                        stack, owner_, record_version_, 
                        cached_lock_lookup_time, cached_lock_duration, owner, 
                        record_version, next_time_tick, 
                        cached_lock_lookup_time_, cached_lock_duration_, 
                        cached_lock_version, local_count_value >>

Timer == tick

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Timer
           \/ (\E self \in ProcSet:  \/ DynamodbUpsertLock(self)
                                     \/ AcquireLock(self) \/ CriticalSection(self))
           \/ (\E self \in WRITERS: Writer(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in WRITERS : /\ WF_vars(Writer(self))
                                 /\ WF_vars(AcquireLock(self))
                                 /\ WF_vars(CriticalSection(self))
                                 /\ WF_vars(DynamodbUpsertLock(self))
        /\ WF_vars(Timer)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Sun Apr 04 16:13:06 PDT 2021 by qph
\* Created Sun Mar 28 23:02:14 PDT 2021 by qph
