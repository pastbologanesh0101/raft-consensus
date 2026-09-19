# Raft Consensus

[![tests](https://github.com/pastbologanesh0101/raft-consensus/actions/workflows/tests.yml/badge.svg)](https://github.com/pastbologanesh0101/raft-consensus/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)

A from-scratch, pure-stdlib implementation of the [Raft consensus
algorithm](https://raft.github.io/), simulated across in-process nodes.
No sockets, no threads, no external dependencies -- nodes talk to each
other by exchanging plain RPC message objects through a controllable,
deterministic simulated network. That determinism is the point: every
election, replication, partition and recovery in this repo runs off a
seeded random source and a logical tick counter, so the exact same
sequence of events reproduces on every run.

Raft has two core jobs, and this repo implements both from the paper:

1. **Leader election** -- the cluster always agrees on at most one leader
   per term.
2. **Log replication** -- the leader replicates a log of client commands
   to followers, and an entry is only considered *committed* once a
   majority of the cluster has it durably in their log.

## How Raft works

### Leader election

Every node is a **follower**, **candidate**, or **leader**. Time is
divided into **terms**, monotonically increasing integers that act as a
logical clock and tie-break stale information.

- A follower that doesn't hear from a leader (or grant a vote) within its
  **election timeout** assumes there is no viable leader, increments its
  term, becomes a **candidate**, votes for itself, and sends
  `RequestVote` RPCs to every peer.
- A node grants a vote only if:
  - the requester's term is at least as high as its own,
  - it hasn't already voted for someone else in that term (**a node
    votes for at most once per term**), and
  - the requester's log is **at least as up-to-date** as its own (higher
    last-log term, or same term and an index at least as large). This is
    what stops a node with a stale/shorter log from ever winning an
    election over a node whose log is more complete -- see
    `test_candidate_with_stale_log_loses_to_candidate_with_up_to_date_log`.
- A candidate that collects votes from a **majority** of the cluster
  (itself included) becomes the leader for that term and starts sending
  `AppendEntries` heartbeats to establish authority and reset followers'
  timeouts.
- Election timeouts are **randomized** (via an injectable/seedable
  `random.Random` per node) specifically so that simultaneous timeouts
  are rare. When a **split vote** does happen (no candidate reaches a
  majority), every candidate's election simply times out again with a
  freshly randomized timeout and tries a new term -- see
  `test_split_vote_eventually_resolves_via_new_randomized_timeout`.
- A node always steps down to follower the instant it sees a strictly
  higher term, from any RPC.

### Log replication

- Client writes are only accepted by the current leader (`propose()`).
  The leader appends the command to its own log tagged with its current
  term, then replicates it to followers via `AppendEntries`.
- Each `AppendEntries` carries `prev_log_index`/`prev_log_term` so a
  follower can verify continuity. If the follower's log doesn't have a
  matching entry there, it rejects the RPC; the leader then decrements
  (or jumps back to a returned `conflict_index`) and retries with an
  earlier point until the logs align.
- If a follower has **conflicting** entries at/after the point of
  agreement (left over from a previous leader), it **truncates** them
  and appends the leader's entries in their place -- see
  `test_followers_truncate_conflicting_entries_from_a_new_leader`.
- The leader tracks `match_index` per follower and advances its
  `commit_index` to the highest index replicated on a **majority** of
  nodes (itself included) -- the actual quorum-commit rule, including
  the Raft safety refinement that a leader only commits entries from
  its **own current term** directly (never an older-term entry via an
  indirect majority). See
  `test_commit_requires_exact_majority_threshold_in_five_node_cluster`,
  which drives a 5-node cluster to exactly 2-of-5 reachable (no commit)
  and then exactly 3-of-5 reachable (commits) to pin down the threshold.

### Safety across failures and partitions

- **At most one leader per term**, even while the cluster is split by a
  network partition: a partitioned-off minority can never collect a
  majority of votes, so it can time out and call elections forever
  without ever becoming leader (`test_minority_partition_cannot_elect_a_leader`).
- After a partition **heals**, the higher term(s) that circulated during
  the split propagate everywhere, stale leaders step down, and the
  cluster converges on a single leader and a single consistent committed
  log -- no split-brain, no forked logs at the same index
  (`test_cluster_converges_to_single_consistent_log_after_partition_heals`).

## The simulation harness

- `raft.Log` / `raft.LogEntry` -- the replicated log (1-indexed, matching
  the paper).
- `raft.RaftNode` -- the state machine: timers (`tick()`), `RequestVote`
  and `AppendEntries` handlers, and the commit-index/quorum logic. A
  `RaftNode` never touches the network directly -- it calls an injected
  `send_fn(receiver_id, message)` -- which is what makes it unit-testable
  in complete isolation (several tests construct bare `RaftNode`s and
  call the RPC handlers directly with hand-crafted messages).
- `raft.Network` -- a controllable network you can:
  - `drop_between(a, b)` / `restore_between(a, b)` -- sever or restore a
    specific pairwise link,
  - `set_delay(a, b, ticks)` -- delay messages between a pair,
  - `partition([group_a, group_b, ...])` / `heal()` -- split the cluster
    into isolated groups that can't exchange any messages, then
    reconnect everyone.
- `raft.Cluster` -- wires a set of `RaftNode`s to a `Network`, owns the
  logical clock and the pending-message queue (messages are delivered on
  a future tick, so `set_delay` has real effect), and exposes helpers
  like `run_until_leader()`, `run_until_committed()`, `kill()`/`revive()`,
  and `propose()`.

Because everything advances on a logical `tick()` rather than wall-clock
time, and every source of randomness is a seeded `random.Random` passed
in from the outside, two runs with the same seeds produce **identical**
event sequences -- there is nothing timing-sensitive or flaky here.

## Running the demo

```bash
python3 demo.py
```

The demo stands up a 5-node cluster, elects a leader, submits a few
client writes and waits for them to commit, **kills the leader** and
watches a new election happen and the cluster keep making progress,
then **partitions** the cluster into a majority and a minority group,
shows the minority can't elect a leader, heals the partition, and
verifies the whole cluster converges on one leader and one consistent
committed log.

### Example output (abridged)

```
========================================================================
Step 1: Bootstrapping a 5-node cluster and holding an election
========================================================================
  [tick   10] N2 -> candidate (term 1)
  [tick   12] N2 -> leader (term 1)

Leader elected: N2 (term 1) at tick 12

========================================================================
Step 2: Submitting client writes and watching them commit
========================================================================
Client write proposed: 'SET x=1' (accepted=True)
  [tick   14] N2 committed up to index 1
  committed=True at tick 14
...

========================================================================
Step 3: Killing the leader and observing a new election
========================================================================
Killing leader N2...
  [tick   33] N1 -> candidate (term 2)
  [tick   33] N5 -> candidate (term 2)
  [tick   35] N1 -> leader (term 2)

New leader elected: N1 (term 2) at tick 35
Client write after failover accepted=True

========================================================================
Step 4: Simulating a network partition
========================================================================
Partitioning cluster into majority group ['N1', 'N3', 'N4'] and minority group ['N5']
Running 60 ticks while partitioned...
  [tick   48] N5 -> candidate (term 3)

Leader(s) while partitioned: ['N1']
(the minority side cannot elect a leader -- it can never reach quorum)
Client write during partition accepted=True

========================================================================
Step 5: Healing the partition and confirming convergence
========================================================================
Partition healed. Running 60 more ticks...
  [tick  102] N1 -> follower (term 5)
  [tick  124] N4 -> leader (term 7)

Leader(s) after healing: ['N4']
Committed log prefixes match across all alive nodes (no split-brain): True
Exactly one leader cluster-wide: True
```

Note the isolated minority node (`N5`) keeps calling elections and
racing its own term number upward the whole time it's cut off (it can
never get a majority of votes) -- this is expected Raft behavior, and is
exactly why a healed partition needs a round of term catch-up (you can
see `N1` step down and `N4` win a fresh election at the higher term)
before the cluster settles back into a single leader.

## Running the tests

```bash
python3 -m unittest discover -s tests -v
```

20 unit tests cover, among other things: a lone node becoming leader
with no peers, exactly-one-leader elections in 3- and 5-node clusters,
strictly increasing terms across elections, a node never voting twice in
one term, a stale-log candidate losing to an up-to-date one, majority
quorum commit thresholds pinned down exactly (2-of-3 and 3-of-5),
leader-failure recovery, partition/minority isolation, post-partition
convergence with no split-brain, and follower log truncation on conflict.

All randomness in the tests goes through a seeded `random.Random`
derived deterministically (via `hashlib`, not Python's per-process
`hash()`) from a fixed seed and node id, so the suite is deterministic
across runs and across machines -- it was run repeatedly (including with
different `PYTHONHASHSEED` values) while developing this to confirm none
of the timing-sensitive election tests are flaky.

## Project layout

```
raft/
  log.py       -- Log / LogEntry
  rpc.py       -- RequestVote*/AppendEntries* message types
  node.py      -- RaftNode: the follower/candidate/leader state machine
  network.py   -- Network: drops, delays, partitions
  cluster.py   -- Cluster: wiring + logical clock + convenience helpers
tests/
  test_raft.py -- unit tests
demo.py        -- end-to-end scripted demo
```

## License

MIT -- see [LICENSE](LICENSE).
