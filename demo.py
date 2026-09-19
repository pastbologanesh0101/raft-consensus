#!/usr/bin/env python3
"""Demo: stand up a 5-node Raft cluster, elect a leader, replicate some
client writes, kill the leader and watch a new election happen, then
simulate a network partition and heal it, printing state transitions and
commit progress throughout.

Run with: python3 demo.py
"""

import random

from raft import Cluster, NodeState


def banner(text: str) -> None:
    print()
    print("=" * 72)
    print(text)
    print("=" * 72)


def make_cluster(node_ids, seed_base=1000, network=None):
    def rng_factory(node_id):
        # Deterministic but distinct per-node randomness, seeded from the
        # node's position so demo runs are reproducible.
        return random.Random(seed_base + node_ids.index(node_id))

    def on_event(kind, *args):
        if kind == "state":
            node_id, term, state = args
            print(f"  [tick {cluster.clock:>4}] {node_id} -> {state} (term {term})")
        elif kind == "commit":
            node_id, commit_index = args
            print(f"  [tick {cluster.clock:>4}] {node_id} committed up to index {commit_index}")

    cluster = Cluster(
        node_ids,
        network=network,
        rng_factory=rng_factory,
        election_timeout_range=(10, 20),
        heartbeat_interval=3,
        on_event=on_event,
    )
    return cluster


def print_status(cluster):
    for nid in cluster.node_ids:
        node = cluster.nodes[nid]
        status = "DEAD" if not node.alive else node.state.value
        print(
            f"    {nid}: {status:<10} term={node.current_term:<3} "
            f"log_len={len(node.log):<3} commit={node.commit_index}"
        )


def main():
    node_ids = ["N1", "N2", "N3", "N4", "N5"]

    banner("Step 1: Bootstrapping a 5-node cluster and holding an election")
    cluster = make_cluster(node_ids)
    leader = cluster.run_until_leader(max_ticks=200)
    assert leader is not None, "cluster failed to elect a leader"
    print(f"\nLeader elected: {leader.id} (term {leader.current_term}) at tick {cluster.clock}")
    print_status(cluster)

    banner("Step 2: Submitting client writes and watching them commit")
    for cmd in ["SET x=1", "SET y=2", "SET z=3"]:
        ok = cluster.propose(cmd)
        print(f"\nClient write proposed: {cmd!r} (accepted={ok})")
        committed = cluster.run_until_committed(leader.log.last_index(), max_ticks=100)
        print(f"  committed={committed} at tick {cluster.clock}")
    print()
    print_status(cluster)

    banner("Step 3: Killing the leader and observing a new election")
    dead_leader_id = leader.id
    print(f"Killing leader {dead_leader_id}...")
    cluster.kill(dead_leader_id)
    new_leader = cluster.run_until_leader(max_ticks=200)
    assert new_leader is not None, "cluster failed to elect a new leader after failover"
    print(f"\nNew leader elected: {new_leader.id} (term {new_leader.current_term}) at tick {cluster.clock}")
    ok = cluster.propose("SET after_failure=true")
    print(f"Client write after failover accepted={ok}")
    cluster.run_until_committed(new_leader.log.last_index(), max_ticks=100)
    print_status(cluster)

    banner("Step 4: Simulating a network partition")
    alive_ids = [n.id for n in cluster.alive_nodes()]
    majority = alive_ids[: (len(alive_ids) // 2) + 1]
    minority = [n for n in alive_ids if n not in majority]
    print(f"Partitioning cluster into majority group {majority} and minority group {minority}")
    cluster.network.partition([majority, minority])

    print("Running 60 ticks while partitioned...")
    cluster.run(60)
    print_status(cluster)
    leaders_during_partition = cluster.leaders()
    print(f"\nLeader(s) while partitioned: {[l.id for l in leaders_during_partition]}")
    print("(the minority side cannot elect a leader -- it can never reach quorum)")

    majority_leader = next((l for l in leaders_during_partition if l.id in majority), None)
    if majority_leader:
        ok = cluster.propose("SET during_partition=true")
        print(f"\nClient write during partition accepted={ok}")
        cluster.run_until_committed(majority_leader.log.last_index(), max_ticks=100)

    banner("Step 5: Healing the partition and confirming convergence")
    cluster.network.heal()
    print("Partition healed. Running 60 more ticks...")
    cluster.run(60)
    print_status(cluster)

    final_leaders = cluster.leaders()
    print(f"\nLeader(s) after healing: {[l.id for l in final_leaders]}")

    alive = cluster.alive_nodes()
    min_commit = min(n.commit_index for n in alive)
    logs_match = True
    for i in range(1, min_commit + 1):
        terms = {n.log.get(i).term for n in alive}
        commands = {n.log.get(i).command for n in alive}
        if len(terms) > 1 or len(commands) > 1:
            logs_match = False
            break
    print(f"Committed log prefixes match across all alive nodes (no split-brain): {logs_match}")
    print(f"Exactly one leader cluster-wide: {len(final_leaders) == 1}")


if __name__ == "__main__":
    main()
