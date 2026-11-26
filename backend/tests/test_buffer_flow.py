"""受理缓冲、幂等认领、并发席位和边缘顺序转投测试。"""
from __future__ import annotations

import threading

import pytest

from app.services.buffer_flow import buffer_flow


@pytest.fixture(autouse=True)
def reset_buffer():
    buffer_flow.reset()
    yield
    buffer_flow.reset()


def test_batch_overflow_has_itemized_retryable_receipts_and_isolated_capacity():
    pid = "DATA-0003"
    other = "DATA-0002"
    assert buffer_flow.set_capacity(pid, 2)["ok"] is True
    before_other = buffer_flow.snapshot(other)

    result = buffer_flow.accept_orders(pid, [
        {"client_ref": "A", "payload": "a"},
        {"client_ref": "B", "payload": "b"},
        {"client_ref": "C", "payload": "c"},
        {"client_ref": "D", "payload": "d"},
    ])

    assert result["ok"] is False
    assert [item["received"] for item in result["items"]] == [True, True, False, False]
    assert [item["retryable"] for item in result["items"]] == [False, False, True, True]
    assert result["state"]["occupied_seats"] == 2
    assert result["state"]["available_seats"] == 0
    assert buffer_flow.snapshot(other)["capacity"] == before_other["capacity"]
    assert buffer_flow.snapshot(other)["occupied_seats"] == 0

    ledger = buffer_flow.ledger(pid)
    assert len(ledger["accepted"]) == 2
    assert {item["client_ref"] for item in ledger["pending"]} == {"C", "D"}


def test_lost_confirmation_replay_claims_existing_result_without_second_seat():
    pid = "DATA-0003"
    buffer_flow.set_capacity(pid, 1)

    first = buffer_flow.accept_orders(pid, [{"client_ref": "A", "payload": "same"}])
    receipt = first["items"][0]
    assert receipt["received"] is True
    seat_id = receipt["seat_id"]

    replay = buffer_flow.accept_orders(pid, [{"client_ref": "A", "payload": "same"}])
    replayed = replay["items"][0]

    assert replayed["receipt_id"] == receipt["receipt_id"]
    assert replayed["claimed_existing"] is True
    assert replayed["duplicate"] is True
    assert replayed["seat_id"] == seat_id
    assert buffer_flow.snapshot(pid)["occupied_seats"] == 1
    seats = [s for s in __import__("app.store", fromlist=["store"]).store.rows("buffer_seats")
             if s.get("client_ref") == "A" and s.get("status") == "占用中"]
    assert len(seats) == 1


def test_pause_distinguishes_accepted_and_pending_and_downstream_only_consumes_eligible():
    pid = "DATA-0003"
    buffer_flow.set_capacity(pid, 1)
    buffer_flow.accept_orders(pid, [{"client_ref": "A", "payload": "a"}])
    buffer_flow.set_paused(pid, True)
    paused = buffer_flow.accept_orders(pid, [{"client_ref": "B", "payload": "b"}])

    assert paused["items"][0]["received"] is False
    assert paused["items"][0]["retryable"] is True
    ledger = buffer_flow.ledger(pid)
    assert [x["client_ref"] for x in ledger["accepted"]] == ["A"]
    assert [x["client_ref"] for x in ledger["pending"]] == ["B"]

    # 暂停受理不影响已受理方按资格转投；待续发 B 不进入下游。
    relay = buffer_flow.edge_relay(pid)
    assert relay["ok"] is True
    assert relay["relay_order"][0]["client_ref"] == "A"
    downstream_refs = {r["client_ref"] for r in buffer_flow.downstream_records(pid)}
    assert downstream_refs == {"A"}


def test_edge_relay_preserves_order_writes_result_back_and_releases_seat_once():
    pid = "DATA-0003"
    buffer_flow.set_capacity(pid, 3)
    buffer_flow.accept_orders(pid, [
        {"client_ref": "A", "payload": "seq=1"},
        {"client_ref": "B", "payload": "seq=2"},
    ])

    relay = buffer_flow.edge_relay(pid)
    assert [x["client_ref"] for x in relay["relay_order"]] == ["A", "B"]
    ledger = buffer_flow.ledger(pid)
    assert all(item["result_written_back"] is True for item in ledger["processed"])
    assert all(item["seat_released"] is True for item in ledger["processed"])
    assert buffer_flow.snapshot(pid)["occupied_seats"] == 0

    second = buffer_flow.edge_relay(pid)
    assert second["relayed_count"] == 0
    records = buffer_flow.downstream_records(pid)
    assert [r["client_ref"] for r in records] == ["A", "B"]
    assert sum(1 for r in records if r["client_ref"] == "A") == 1
    assert sum(1 for r in records if r["client_ref"] == "B") == 1


def test_race_one_seat_then_recover_release_and_other_deals_once():
    pid = "DATA-0003"
    buffer_flow.reset(pid)
    buffer_flow.set_capacity(pid, 1)
    barrier = threading.Barrier(2)
    outcomes: list[dict] = []
    outcome_lock = threading.Lock()

    def worker(ref: str):
        barrier.wait(timeout=5)
        result = buffer_flow.accept_orders(
            pid, [{"client_ref": ref, "payload": f"payload-{ref}"}], window_id=f"window-{ref}"
        )
        with outcome_lock:
            outcomes.append(result["items"][0])

    threads = [threading.Thread(target=worker, args=(ref,)) for ref in ("A", "B")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    assert len(outcomes) == 2
    assert sum(1 for item in outcomes if item["received"] is True) == 1
    loser = next(item for item in outcomes if item["received"] is False)
    winner_ref = next(item["client_ref"] for item in outcomes if item["received"] is True)
    assert loser["retryable"] is True
    assert buffer_flow.snapshot(pid)["occupied_seats"] == 1

    # 答复中断后，胜方窗口用同一试单号只能认领原结果。
    winner_replay = buffer_flow.accept_orders(
        pid, [{"client_ref": winner_ref, "payload": f"payload-{winner_ref}"}]
    )["items"][0]
    assert winner_replay["claimed_existing"] is True
    assert buffer_flow.snapshot(pid)["occupied_seats"] == 1

    continued = buffer_flow.continue_flow(pid)
    assert continued["ok"] is True
    retried = continued["items"][0]
    assert retried["client_ref"] != winner_ref
    assert retried["received"] is True

    ledger = buffer_flow.ledger(pid)
    assert len(ledger["processed"]) == 1
    assert ledger["processed"][0]["client_ref"] == winner_ref
    assert len(ledger["accepted"]) == 1
    assert ledger["accepted"][0]["client_ref"] == retried["client_ref"]

    buffer_flow.edge_relay(pid)
    records = buffer_flow.downstream_records(pid)
    assert {r["client_ref"] for r in records} == {"A", "B"}
    assert sum(1 for r in records if r["client_ref"] == "A") == 1
    assert sum(1 for r in records if r["client_ref"] == "B") == 1


def test_router_exposes_buffer_endpoints():
    from app.routers.buffer_flow import router

    paths = {getattr(route, "path", "") for route in router.routes}
    assert "/api/data_stream/buffer/accept" in paths
    assert "/api/data_stream/buffer/ledger" in paths
    assert "/api/edge_cache/buffer/relay" in paths
