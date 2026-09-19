"""Unit tests for the from-scratch Raft implementation.

All timing is driven by a seeded random.Random per node (via rng_factory)
and a logical tick counter (Cluster.tick()) -- there is no real-time sleep
and no threading anywhere in this codebase, so these tests are fully
deterministic. They are safe to run repeatedly (or in a loop) without any
flakiness from real clock jitter.
"""

import hashlib
import random
import unittest

from raft import (
    AppendEntriesRequest,
    Cluster,
    LogEntry,
    NodeState,
    RaftNode,
    RequestVoteRequest,
)


def _deterministic_seed(seed_base, node_id) -> int:
    # Python's built-in hash() for str is randomized per-process (unless
    # PYTHONHASHSEED is fixed), which would make "seeded" tests flaky
    # across separate test runs. hashlib is stable across processes, so
    # this gives the exact same per-node RNG stream every single run.
    digest = hashlib.md5(f"{seed_base}:{node_id}".encode()).hexdigest()
    return int(digest[:8], 16)


def rng_factory(seed_base):
    def factory(node_id):
        return random.Random(_deterministic_seed(seed_base, node_id))
    return factory


def make_cluster(node_ids, seed_base=42, **kwargs):
    kwargs.setdefault("election_timeout_range", (10, 20))
    kwargs.setdefault("heartbeat_interval", 3)
    return Cluster(node_ids, rng_factory=rng_factory(seed_base), **kwargs)


def make_node(node_id, peer_ids, seed=1, **kwargs):
    sent = []
    kwargs.setdefault("election_timeout_range", (10, 20))
    kwargs.setdefault("heartbeat_interval", 3)
    node = RaftNode(
        node_id,
        peer_ids,
        send_fn=lambda receiver, payload: sent.append((receiver, payload)),
        rng=random.Random(seed),
        **kwargs,
    )
    return node, sent


class SingleNodeTests(unittest.TestCase):
    def test_single_node_with_no_peers_becomes_leader_immediately(self):
        node, _ = make_node("N1", [])
        self.assertEqual(node.state, NodeState.LEADER)
        self.assertEqual(node.current_term, 0)

    def test_single_node_commits_writes_immediately(self):
        node, _ = make_node("N1", [])
        ok = node.propose("SET x=1")
        self.assertTrue(ok)
        self.assertEqual(node.commit_index, 1)
        self.assertEqual(node.committed_entries()[0].command, "SET x=1")


class ElectionTests(unittest.TestCase):
    def test_three_node_cluster_elects_exactly_one_leader(self):
        cluster = make_cluster(["N1", "N2", "N3"], seed_base=1)
        leader = cluster.run_until_leader(max_ticks=200)
        self.assertIsNotNone(leader)
        leaders = cluster.leaders()
        self.assertEqual(len(leaders), 1)

    def test_five_node_cluster_elects_exactly_one_leader(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=2)
        leader = cluster.run_until_leader(max_ticks=200)
        self.assertIsNotNone(leader)
        self.assertEqual(len(cluster.leaders()), 1)

    def test_election_is_deterministic_given_a_seed(self):
        cluster_a = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=7)
        leader_a = cluster_a.run_until_leader(max_ticks=200)

        cluster_b = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=7)
        leader_b = cluster_b.run_until_leader(max_ticks=200)

        self.assertEqual(leader_a.id, leader_b.id)
        self.assertEqual(leader_a.current_term, leader_b.current_term)
        self.assertEqual(cluster_a.clock, cluster_b.clock)

    def test_term_numbers_strictly_increase_across_repeated_elections(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=3)
        seen_terms = []
        for _ in range(4):
            leader = cluster.run_until_leader(max_ticks=300)
            self.assertIsNotNone(leader)
            seen_terms.append(leader.current_term)
            cluster.kill(leader.id)
            if len(cluster.alive_nodes()) < 3:
                break
        self.assertEqual(seen_terms, sorted(set(seen_terms)))
        self.assertTrue(all(b > a for a, b in zip(seen_terms, seen_terms[1:])))

    def test_node_never_votes_twice_in_the_same_term(self):
        follower, _ = make_node("F", ["A", "B"])
        req_a = RequestVoteRequest(term=1, candidate_id="A", last_log_index=0, last_log_term=0)
        req_b = RequestVoteRequest(term=1, candidate_id="B", last_log_index=0, last_log_term=0)

        resp_a = follower.handle_request_vote_request(req_a)
        resp_b = follower.handle_request_vote_request(req_b)

        self.assertTrue(resp_a.vote_granted)
        self.assertFalse(resp_b.vote_granted)
        self.assertEqual(follower.voted_for, "A")

    def test_node_can_regrant_same_vote_if_request_repeated(self):
        follower, _ = make_node("F", ["A"])
        req_a = RequestVoteRequest(term=1, candidate_id="A", last_log_index=0, last_log_term=0)
        resp1 = follower.handle_request_vote_request(req_a)
        resp2 = follower.handle_request_vote_request(req_a)
        self.assertTrue(resp1.vote_granted)
        self.assertTrue(resp2.vote_granted)

    def test_candidate_with_stale_log_loses_to_candidate_with_up_to_date_log(self):
        # Follower has an existing log entry: term=2 at index 1.
        follower, _ = make_node("F", ["A", "B"])
        follower.log.append(LogEntry(term=2, command="SET a=1"))
        follower.current_term = 2

        # Candidate A has a stale/shorter log (term=1) -- must lose.
        stale_request = RequestVoteRequest(term=3, candidate_id="A", last_log_index=0, last_log_term=1)
        stale_resp = follower.handle_request_vote_request(stale_request)
        self.assertFalse(stale_resp.vote_granted)

        # Candidate B has a log at least as up to date (term=2, same index) -- must win.
        follower2, _ = make_node("F2", ["A", "B"])
        follower2.log.append(LogEntry(term=2, command="SET a=1"))
        follower2.current_term = 2
        up_to_date_request = RequestVoteRequest(term=3, candidate_id="B", last_log_index=1, last_log_term=2)
        up_to_date_resp = follower2.handle_request_vote_request(up_to_date_request)
        self.assertTrue(up_to_date_resp.vote_granted)

    def test_split_vote_eventually_resolves_via_new_randomized_timeout(self):
        # A narrow timeout window (5-6 ticks) makes it likely that several
        # nodes become candidates on the same or adjacent ticks, producing
        # a split vote in the first round or two. The retry with a freshly
        # randomized timeout must still converge on exactly one leader.
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=99, election_timeout_range=(5, 6))
        leader = cluster.run_until_leader(max_ticks=500)
        self.assertIsNotNone(leader)
        self.assertEqual(len(cluster.leaders()), 1)


class ReplicationTests(unittest.TestCase):
    def test_log_entries_replicate_and_commit_after_majority_in_three_node_cluster(self):
        cluster = make_cluster(["N1", "N2", "N3"], seed_base=10)
        leader = cluster.run_until_leader(max_ticks=200)
        ok = cluster.propose("SET x=1")
        self.assertTrue(ok)
        committed = cluster.run_until_committed(leader.log.last_index(), max_ticks=100)
        self.assertTrue(committed)
        # Exact quorum check: at least 2 of 3 nodes (leader + >=1 follower)
        # must have replicated the entry for it to be committed.
        replicated_count = sum(
            1 for n in cluster.nodes.values() if n.log.last_index() >= 1 and n.log.get(1).command == "SET x=1"
        )
        self.assertGreaterEqual(replicated_count, 2)

    def test_commit_requires_exact_majority_threshold_in_five_node_cluster(self):
        # A wide election timeout keeps the disconnected followers from
        # starting their own elections mid-test -- this test is about the
        # commit-quorum threshold, not re-election, so it isolates that.
        cluster = make_cluster(
            ["N1", "N2", "N3", "N4", "N5"], seed_base=11, election_timeout_range=(40, 60)
        )
        leader = cluster.run_until_leader(max_ticks=200)
        others = [nid for nid in cluster.node_ids if nid != leader.id]

        # Cut the leader off from 3 of its 4 followers, leaving only
        # leader + 1 follower = 2 of 5 reachable -- NOT a majority (need 3).
        unreachable = others[:3]
        for peer in unreachable:
            cluster.network.drop_between(leader.id, peer)

        cluster.propose("SET x=1")
        cluster.run(5)
        self.assertEqual(leader.commit_index, 0, "must not commit without a majority (2 of 5)")

        # Restore one more link: leader + 2 followers = 3 of 5 -- majority.
        cluster.network.restore_between(leader.id, unreachable[0])
        cluster.run(80)
        self.assertGreaterEqual(leader.commit_index, 1, "must commit once a majority (3 of 5) is reachable")

    def test_leader_failure_triggers_new_election_and_cluster_keeps_making_progress(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=12)
        leader = cluster.run_until_leader(max_ticks=200)
        cluster.propose("SET x=1")
        cluster.run_until_committed(leader.log.last_index(), max_ticks=100)

        old_leader_id = leader.id
        old_term = leader.current_term
        cluster.kill(old_leader_id)

        new_leader = cluster.run_until_leader(max_ticks=300)
        self.assertIsNotNone(new_leader)
        self.assertNotEqual(new_leader.id, old_leader_id)
        self.assertGreater(new_leader.current_term, old_term)

        ok = cluster.propose("SET y=2")
        self.assertTrue(ok)
        committed = cluster.run_until_committed(new_leader.log.last_index(), max_ticks=100)
        self.assertTrue(committed)

    def test_followers_truncate_conflicting_entries_from_a_new_leader(self):
        follower, sent = make_node("F", ["L"])
        # Follower has stray uncommitted entries from an old leader/term.
        follower.log.append(LogEntry(term=1, command="OLD-A"))
        follower.log.append(LogEntry(term=1, command="OLD-B"))
        follower.current_term = 1

        # A new leader for term 2 sends an AppendEntries that conflicts at
        # index 2 (different term) with a correct entry to replace it.
        req = AppendEntriesRequest(
            term=2,
            leader_id="L",
            prev_log_index=1,
            prev_log_term=1,
            entries=[LogEntry(term=2, command="NEW-B")],
            leader_commit=0,
        )
        resp = follower.handle_append_entries_request(req)
        self.assertTrue(resp.success)
        self.assertEqual(len(follower.log), 2)
        self.assertEqual(follower.log.get(2).command, "NEW-B")
        self.assertEqual(follower.log.get(2).term, 2)

    def test_follower_rejects_append_entries_on_log_mismatch(self):
        follower, _ = make_node("F", ["L"])
        follower.current_term = 1
        # Follower's log is empty, but leader claims a predecessor at index 5.
        req = AppendEntriesRequest(term=1, leader_id="L", prev_log_index=5, prev_log_term=1, entries=[], leader_commit=0)
        resp = follower.handle_append_entries_request(req)
        self.assertFalse(resp.success)


class PartitionTests(unittest.TestCase):
    def test_minority_partition_cannot_elect_a_leader(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=20)
        cluster.run_until_leader(max_ticks=200)

        majority = ["N1", "N2", "N3"]
        minority = ["N4", "N5"]
        cluster.network.partition([majority, minority])
        cluster.run(120)

        minority_leaders = [n for n in cluster.leaders() if n.id in minority]
        self.assertEqual(len(minority_leaders), 0, "a 2-node minority can never reach a 3-of-5 majority")

    def test_at_most_one_leader_per_term_even_across_a_partition(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=21)
        cluster.run_until_leader(max_ticks=200)

        cluster.network.partition([["N1", "N2", "N3"], ["N4", "N5"]])
        cluster.run(150)

        leaders_by_term = {}
        for kind, node_id, term, state in cluster.events:
            if kind == "state" and state == "leader":
                leaders_by_term.setdefault(term, set()).add(node_id)
        for term, leader_ids in leaders_by_term.items():
            self.assertEqual(len(leader_ids), 1, f"term {term} had multiple leaders: {leader_ids}")

    def test_cluster_converges_to_single_consistent_log_after_partition_heals(self):
        cluster = make_cluster(["N1", "N2", "N3", "N4", "N5"], seed_base=22)
        leader = cluster.run_until_leader(max_ticks=200)
        cluster.propose("SET a=1")
        cluster.run_until_committed(leader.log.last_index(), max_ticks=100)

        majority = ["N1", "N2", "N3"]
        minority = ["N4", "N5"]
        # Make sure the current leader ends up on the majority side so it
        # can keep committing writes during the partition.
        if leader.id in minority:
            majority, minority = minority, majority
        cluster.network.partition([majority, minority])
        cluster.run(80)

        maj_leader = next((n for n in cluster.leaders() if n.id in majority), None)
        self.assertIsNotNone(maj_leader)
        cluster.propose("SET b=2")
        cluster.run_until_committed(maj_leader.log.last_index(), max_ticks=100)

        cluster.network.heal()
        cluster.run(80)

        self.assertEqual(len(cluster.leaders()), 1, "no split-brain / forked leaders after healing")

        alive = cluster.alive_nodes()
        min_commit = min(n.commit_index for n in alive)
        self.assertGreaterEqual(min_commit, 1)
        for i in range(1, min_commit + 1):
            terms = {n.log.get(i).term for n in alive}
            commands = {n.log.get(i).command for n in alive}
            self.assertEqual(len(terms), 1, f"log entry {i} diverged in term across nodes: {terms}")
            self.assertEqual(len(commands), 1, f"log entry {i} diverged in command across nodes: {commands}")


class NetworkPrimitiveTests(unittest.TestCase):
    def test_dropped_messages_are_not_delivered(self):
        cluster = make_cluster(["N1", "N2", "N3"], seed_base=30)
        cluster.network.drop_between("N1", "N2")
        cluster.network.drop_between("N1", "N3")
        cluster.network.drop_between("N2", "N3")
        # No node can reach any other -- nobody should ever become leader.
        cluster.run(60)
        self.assertEqual(len(cluster.leaders()), 0)

    def test_restore_between_reenables_delivery(self):
        cluster = make_cluster(["N1", "N2", "N3"], seed_base=31)
        cluster.network.drop_between("N1", "N2")
        cluster.network.drop_between("N1", "N3")
        cluster.network.drop_between("N2", "N3")
        cluster.run(30)
        self.assertEqual(len(cluster.leaders()), 0)

        cluster.network.restore_between("N1", "N2")
        cluster.network.restore_between("N1", "N3")
        cluster.network.restore_between("N2", "N3")
        leader = cluster.run_until_leader(max_ticks=200)
        self.assertIsNotNone(leader)


if __name__ == "__main__":
    unittest.main()
