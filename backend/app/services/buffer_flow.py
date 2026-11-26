"""管道受理缓冲、幂等认领与边缘顺序转投。

这个服务维护的是真实的内存队列席位：只有受理成功的试单才会写入占用中的
席位记录。未超容量失败、暂停失败的试单只形成可续办联单，不占席位。

所有状态都放在 ``store`` 的内存表中，锁只负责并发正确性；进程重启后回到
示例初始状态，符合当前演示仓库的定位。
"""
from __future__ import annotations

import hashlib
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app.store import store

CST = timezone(timedelta(hours=8))

RECEIVED = "已接收"
PENDING = "待续发"
ACCEPTED = "已受理"
DELIVERED = "已转投"
CLOSED = "已销单"
FAILED = "处理失败"


def _now() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _short_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value)


class BufferFlowService:
    """按管道隔离的席位、接收联单和边缘转投协调器。"""

    MODULE = "data_stream"

    def __init__(self) -> None:
        self._locks_guard = threading.Lock()
        self._pipeline_locks: dict[str, threading.RLock] = {}

    # ------------------------------------------------------------------
    # 基础辅助
    # ------------------------------------------------------------------
    def _lock(self, pipeline_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._pipeline_locks.get(pipeline_id)
            if lock is None:
                lock = threading.RLock()
                self._pipeline_locks[pipeline_id] = lock
            return lock

    @staticmethod
    def _pipelines() -> list[dict[str, Any]]:
        return store.rows("data_stream")

    def find_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        pid = _text(pipeline_id)
        for row in self._pipelines():
            if _text(row.get("管道编号")) == pid or _text(row.get("id")) == pid:
                return row
        return None

    def pipeline_code(self, pipeline_id: str) -> str:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is not None:
            return _text(pipe.get("管道编号")) or _text(pipeline_id)
        return _text(pipeline_id)

    def _state_row(self, pipeline_id: str) -> dict[str, Any]:
        pid = self.pipeline_code(pipeline_id)
        rows = store.rows("buffer_pipeline_state")
        for row in rows:
            if row.get("pipeline_id") == pid:
                return row

        pipe = self.find_pipeline(pid)
        capacity = 3
        if pipe is not None:
            raw_capacity = pipe.get("缓冲席位", pipe.get("buffer_capacity"))
            try:
                if raw_capacity not in (None, ""):
                    capacity = max(0, int(raw_capacity))
                else:
                    raw_size = _text(pipe.get("缓冲大小")).upper().replace("KB", "").replace("MB", "")
                    if raw_size.isdigit():
                        capacity = max(1, min(int(raw_size), 1024))
            except (TypeError, ValueError):
                capacity = 3

        row = {
            "pipeline_id": pid,
            "capacity": capacity,
            "paused": False,
            "relay_paused": False,
            "next_sequence": 0,
            "seat_cursor": 0,
            "created_at": _now(),
            "updated_at": _now(),
        }
        rows.append(row)
        return row

    @staticmethod
    def _all_receipts() -> list[dict[str, Any]]:
        return store.rows("buffer_receipts")

    def _pipeline_receipts(self, pipeline_id: str) -> list[dict[str, Any]]:
        pid = self.pipeline_code(pipeline_id)
        return [row for row in store.rows("buffer_receipts") if row.get("pipeline_id") == pid]

    def _active_seats(self, pipeline_id: str) -> list[dict[str, Any]]:
        pid = self.pipeline_code(pipeline_id)
        return [
            seat for seat in store.rows("buffer_seats")
            if seat.get("pipeline_id") == pid and seat.get("status") == "占用中"
        ]

    def _find_receipt(self, pipeline_id: str, client_ref: str) -> dict[str, Any] | None:
        pid = self.pipeline_code(pipeline_id)
        ref = _text(client_ref)
        for row in store.rows("buffer_receipts"):
            if row.get("pipeline_id") == pid and _text(row.get("client_ref")) == ref:
                return row
        return None

    @staticmethod
    def _payload_text(payload: Any) -> str:
        if isinstance(payload, str):
            return payload
        if payload is None:
            return ""
        try:
            import json

            return json.dumps(payload, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return str(payload)

    @staticmethod
    def _present(row: dict[str, Any]) -> dict[str, Any]:
        """补充常见字段别名，同时保留中文字段和权威的 received/retryable。"""
        item = dict(row)
        retryable = bool(item.get("retryable"))
        received = bool(item.get("received"))
        item["received"] = received
        item["retryable"] = retryable
        item["accepted"] = received or bool(item.get("accepted"))
        item["can_retry"] = retryable
        item["retriable"] = retryable
        item["canRetry"] = retryable
        item["acceptance_status"] = "accepted" if received else "retry_pending"
        return item

    def _ensure_ref(self, order: dict[str, Any], index: int, batch_seed: str) -> str:
        ref = _text(
            order.get("client_ref")
            or order.get("clientRef")
            or order.get("request_id")
            or order.get("client_request_id")
            or order.get("requestId")
            or order.get("idempotency_key")
            or order.get("idempotencyKey")
            or order.get("key")
            or order.get("试单号")
            or order.get("编号")
        )
        if ref:
            return ref
        payload = self._payload_text(order.get("payload", order.get("content", order.get("内容", ""))))
        digest = hashlib.sha1(f"{batch_seed}:{index}:{payload}".encode("utf-8")).hexdigest()[:10].upper()
        return f"AUTO-{digest}"

    def snapshot(self, pipeline_id: str) -> dict[str, Any]:
        pid = self.pipeline_code(pipeline_id)
        state = self._state_row(pid)
        receipts = self._pipeline_receipts(pid)
        active_seats = self._active_seats(pid)
        capacity = int(state.get("capacity", 0))
        accepted = [
            row for row in receipts
            if row.get("received") is True and row.get("seat_released") is not True
        ]
        pending = [
            row for row in receipts
            if row.get("received") is not True and row.get("retryable") is True
        ]
        processed = [row for row in receipts if row.get("status") in (DELIVERED, CLOSED)]
        failed = [row for row in receipts if row.get("status") == FAILED]
        pipe = self.find_pipeline(pid) or {}
        pipeline_stopped = pipe.get("status") == "已停用"
        manual_paused = bool(state.get("paused"))
        effective_paused = manual_paused or pipeline_stopped
        return {
            "pipeline_id": pid,
            "管道编号": pid,
            "capacity": capacity,
            "缓冲席位容量": capacity,
            "occupied_seats": len(active_seats),
            "已占用席位": len(active_seats),
            "available_seats": max(0, capacity - len(active_seats)),
            "剩余席位": max(0, capacity - len(active_seats)),
            "over_capacity": len(active_seats) > capacity,
            "超容占用": len(active_seats) > capacity,
            "paused": effective_paused,
            "manual_paused": manual_paused,
            "pipeline_stopped": pipeline_stopped,
            "受理已暂停": effective_paused,
            "relay_paused": bool(state.get("relay_paused")),
            "转投已暂停": bool(state.get("relay_paused")),
            "accepted_count": len(accepted),
            "pending_count": len(pending),
            "processed_count": len(processed),
            "failed_count": len(failed),
            "active_seat_ids": [seat.get("seat_id") for seat in active_seats],
            "updated_at": state.get("updated_at"),
        }

    def list_pipeline_states(self) -> list[dict[str, Any]]:
        result = []
        for pipe in self._pipelines():
            pid = _text(pipe.get("管道编号"))
            if not pid:
                continue
            result.append({
                **self.snapshot(pid),
                "name": pipe.get("管道编号"),
                "status": pipe.get("status"),
                "运行状态": pipe.get("运行状态") or pipe.get("status"),
            })
        return result

    def ledger(self, pipeline_id: str | None = None) -> dict[str, Any]:
        if pipeline_id:
            pid = self.pipeline_code(pipeline_id)
            with self._lock(pid):
                raw_items = sorted(self._pipeline_receipts(pid), key=lambda r: int(r.get("id", 0)))
                items = [self._present(row) for row in raw_items]
                return {
                    "ok": True,
                    "message": "接收联单已读取",
                    "state": self.snapshot(pid),
                    "pipelines": self.list_pipeline_states(),
                    "items": items,
                    "receipts": items,
                    "接收联单": items,
                    "accepted": [r for r in items if r.get("received") and not r.get("seat_released")],
                    "pending": [r for r in items if not r.get("received") and r.get("retryable")],
                    "processed": [r for r in items if r.get("status") in (DELIVERED, CLOSED)],
                    "downstream": self.downstream_records(pid),
                    "edge_logs": self.edge_logs(pid),
                }

        raw_items = sorted(store.rows("buffer_receipts"), key=lambda r: int(r.get("id", 0)))
        items = [self._present(row) for row in raw_items]
        return {
            "ok": True,
            "message": "全部管道接收联单已读取",
            "state": None,
            "pipelines": self.list_pipeline_states(),
            "items": items,
            "receipts": items,
            "接收联单": items,
            "accepted": [r for r in items if r.get("received") and not r.get("seat_released")],
            "pending": [r for r in items if not r.get("received") and r.get("retryable")],
            "processed": [r for r in items if r.get("status") in (DELIVERED, CLOSED)],
            "downstream": self.downstream_records(),
            "edge_logs": self.edge_logs(),
        }

    # ------------------------------------------------------------------
    # 容量、暂停和重置
    # ------------------------------------------------------------------
    def set_capacity(
        self,
        pipeline_id: str,
        capacity: int | None = None,
        *,
        remaining: int | None = None,
    ) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在"}
        pid = _text(pipe.get("管道编号"))
        with self._lock(pid):
            state = self._state_row(pid)
            occupied = len(self._active_seats(pid))
            if remaining is not None:
                try:
                    target = occupied + max(0, int(remaining))
                except (TypeError, ValueError):
                    return {"ok": False, "message": "剩余席位必须是整数"}
            else:
                try:
                    target = int(capacity)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    return {"ok": False, "message": "缓冲席位容量必须是非负整数"}
            if target < 0:
                return {"ok": False, "message": "缓冲席位容量不能为负数"}
            over_occupied = target < occupied
            state["capacity"] = target
            state["updated_at"] = _now()
            pipe["缓冲席位"] = target
            pipe["buffer_capacity"] = target
            message = f"管道 {pid} 缓冲席位容量已调整为 {target}"
            if over_occupied:
                message += f"；当前已占用 {occupied} 席，既有受理结果保留，在席位降到新容量前不能新增受理"
            return {
                "ok": True,
                "message": message,
                "warning": "CAPACITY_BELOW_OCCUPIED" if over_occupied else None,
                "state": self.snapshot(pid),
            }

    def set_paused(
        self,
        pipeline_id: str,
        paused: bool | None = None,
        *,
        relay_paused: bool | None = None,
    ) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在"}
        pid = _text(pipe.get("管道编号"))
        with self._lock(pid):
            state = self._state_row(pid)
            if paused is not None:
                state["paused"] = bool(paused)
            if relay_paused is not None:
                state["relay_paused"] = bool(relay_paused)
            state["updated_at"] = _now()
            return {
                "ok": True,
                "message": (
                    f"管道 {pid} 受理已{'暂停' if state['paused'] else '恢复'}，"
                    f"边缘转投已{'暂停' if state['relay_paused'] else '恢复'}"
                ),
                "state": self.snapshot(pid),
            }

    def reset(self, pipeline_id: str | None = None) -> dict[str, Any]:
        pid = self.pipeline_code(pipeline_id) if pipeline_id else None
        lock = self._lock(pid) if pid else threading.Lock()
        with lock:
            for table in ("buffer_receipts", "buffer_seats", "buffer_relay_logs", "buffer_downstream_records"):
                rows = store.rows(table)
                if pid:
                    rows[:] = [row for row in rows if row.get("pipeline_id") != pid]
                else:
                    rows.clear()
            states = store.rows("buffer_pipeline_state")
            if pid:
                states[:] = [row for row in states if row.get("pipeline_id") != pid]
            else:
                states.clear()
            return {"ok": True, "message": "受理缓冲演示状态已重置", "ledger": self.ledger(pid)}

    # ------------------------------------------------------------------
    # 受理与幂等
    # ------------------------------------------------------------------
    def accept_orders(
        self,
        pipeline_id: str,
        orders: list[dict[str, Any]],
        *,
        window_id: str | None = None,
        ledger_id: str | None = None,
    ) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在", "items": []}
        if not isinstance(orders, list) or not orders:
            return {"ok": False, "message": "没有可提交的流入试单", "items": []}

        pid = _text(pipe.get("管道编号"))
        batch_id = ledger_id or _short_id("LED")
        # 不把随机 batch_id 放入幂等键派生种子：没有显式试单号时，
        # 同一管道、同一行位置和同一内容重提仍应得到同一个 AUTO 键。
        batch_seed = pid
        items: list[dict[str, Any]] = []
        for index, raw_order in enumerate(orders, start=1):
            if isinstance(raw_order, str):
                token = raw_order.strip()
                if re.fullmatch(r"[A-Za-z0-9_\-一-龥]{1,32}", token):
                    order = {"client_ref": token, "payload": token}
                else:
                    order = {"payload": raw_order}
            elif isinstance(raw_order, dict):
                order = raw_order
            else:
                order = {"payload": raw_order}
            ref = self._ensure_ref(order, index, batch_seed)
            payload = order.get("payload", order.get("content", order.get("内容", "")))
            items.append(self._accept_one(pid, ref, payload, batch_id, window_id))

        items = [self._present(item) for item in items]
        accepted_count = sum(1 for item in items if item.get("received") is True)
        rejected_count = len(items) - accepted_count
        all_received = rejected_count == 0
        message = (
            f"接收联单 {batch_id}：{accepted_count} 条已接收并占用真实席位，"
            f"{rejected_count} 条未接收且可续发"
            if rejected_count
            else f"接收联单 {batch_id}：{accepted_count} 条全部已接收"
        )
        return {
            # ok 表示整批是否全部受理；联单本身始终完整返回。
            "ok": all_received,
            "all_received": all_received,
            "capacity_conflict": any(item.get("reason_code") == "CAPACITY_FULL" for item in items),
            "message": message,
            "ledger_id": batch_id,
            "batch_id": batch_id,
            "pipeline_id": pid,
            "items": items,
            "receipts": items,
            "接收联单": items,
            "逐条结果": items,
            "conflict": rejected_count > 0,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "retryable_count": sum(1 for item in items if item.get("retryable") is True),
            "state": self.snapshot(pid),
        }

    def _accept_one(
        self,
        pid: str,
        client_ref: str,
        payload: Any,
        batch_id: str,
        window_id: str | None,
    ) -> dict[str, Any]:
        payload_text = self._payload_text(payload)
        with self._lock(pid):
            state = self._state_row(pid)
            existing = self._find_receipt(pid, client_ref)

            if existing is not None:
                existing["attempt_count"] = int(existing.get("attempt_count", 1)) + 1
                existing["last_attempt_at"] = _now()
                existing["window_id"] = window_id or existing.get("window_id")

                if _text(existing.get("payload")) != payload_text and payload_text:
                    existing.update({
                        "received": False,
                        "retryable": False,
                        "status": "幂等键冲突",
                        "reason_code": "IDEMPOTENCY_CONFLICT",
                        "reason": "同一试单号携带了不同内容，不能覆盖已存在结果",
                        "message": "幂等键冲突：请使用新的试单号提交不同条目",
                    })
                    return dict(existing)

                if existing.get("received") is True:
                    existing["duplicate"] = True
                    existing["claimed_existing"] = True
                    existing["message"] = "已认领既有接收结果；未新增缓冲席位"
                    return dict(existing)

                # 之前因容量/暂停未受理的条目，允许用同一个试单号续办。
                receipt = existing
            else:
                receipt_id = _short_id("RCP")
                receipt = {
                    "id": max((int(r.get("id", 0)) for r in store.rows("buffer_receipts")), default=0) + 1,
                    "receipt_id": receipt_id,
                    "ledger_id": batch_id,
                    "pipeline_id": pid,
                    "client_ref": client_ref,
                    "试单号": client_ref,
                    "payload": payload_text,
                    "流入内容": payload_text,
                    "window_id": window_id,
                    "received": False,
                    "是否已接收": "否",
                    "retryable": True,
                    "是否可重试": "是",
                    "accepted": False,
                    "duplicate": False,
                    "claimed_existing": False,
                    "attempt_count": 1,
                    "sequence": None,
                    "accepted_sequence": None,
                    "seat_id": None,
                    "seat_released": False,
                    "status": PENDING,
                    "reason_code": None,
                    "reason": "",
                    "message": "试单已登记，等待受理",
                    "processing_status": "未到下游",
                    "downstream_result": None,
                    "处理结果": None,
                    "result_written_back": False,
                    "delivery_count": 0,
                    "created_at": _now(),
                    "updated_at": _now(),
                }
                store.rows("buffer_receipts").append(receipt)

            pipe = self.find_pipeline(pid) or {}
            pipeline_stopped = pipe.get("status") == "已停用"
            if state.get("paused") or pipeline_stopped:
                reason = "管道已暂停受理" if state.get("paused") else "管道已停用"
                receipt.update({
                    "received": False,
                    "是否已接收": "否",
                    "retryable": True,
                    "是否可重试": "是",
                    "status": PENDING,
                    "reason_code": "PAUSED",
                    "reason": reason,
                    "message": f"{reason}：该条保持待续发，不占席位",
                    "updated_at": _now(),
                })
                return dict(receipt)

            occupied = len(self._active_seats(pid))
            capacity = int(state.get("capacity", 0))
            if occupied >= capacity:
                receipt.update({
                    "received": False,
                    "是否已接收": "否",
                    "retryable": True,
                    "是否可重试": "是",
                    "status": PENDING,
                    "reason_code": "CAPACITY_FULL",
                    "reason": f"缓冲席位已满（容量 {capacity}，占用 {occupied}）",
                    "message": "容量冲突：未接收但可重试，未占用其他管道席位",
                    "updated_at": _now(),
                })
                return dict(receipt)

            # 真正受理：顺序号、席位记录和联单在同一管道锁内提交。
            state["next_sequence"] = int(state.get("next_sequence", 0)) + 1
            state["seat_cursor"] = int(state.get("seat_cursor", 0)) + 1
            sequence = int(state["next_sequence"])
            seat_no = int(state["seat_cursor"])
            seat_id = f"SEAT-{pid}-{seat_no:04d}-{uuid.uuid4().hex[:6].upper()}"
            seat = {
                "id": max((int(s.get("id", 0)) for s in store.rows("buffer_seats")), default=0) + 1,
                "seat_id": seat_id,
                "pipeline_id": pid,
                "seat_no": seat_no,
                "sequence": sequence,
                "receipt_id": receipt.get("receipt_id"),
                "client_ref": client_ref,
                "status": "占用中",
                "occupied_at": _now(),
                "released_at": None,
            }
            store.rows("buffer_seats").append(seat)

            receipt.update({
                "received": True,
                "是否已接收": "是",
                "retryable": False,
                "是否可重试": "否",
                "accepted": True,
                "status": ACCEPTED,
                "reason_code": None,
                "reason": "",
                "message": f"已接收并占用席位 {seat_id}",
                "sequence": sequence,
                "accepted_sequence": sequence,
                "seat_id": seat_id,
                "seat_released": False,
                "accepted_at": _now(),
                "updated_at": _now(),
            })
            state["updated_at"] = _now()
            return dict(receipt)

    def recover_accepted(self, pipeline_id: str) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在", "items": []}
        pid = _text(pipe.get("管道编号"))
        with self._lock(pid):
            items = sorted(
                [self._present(r) for r in self._pipeline_receipts(pid) if r.get("received") and not r.get("seat_released")],
                key=lambda r: int(r.get("accepted_sequence") or r.get("sequence") or 0),
            )
            return {
                "ok": True,
                "message": f"已取回 {len(items)} 条已受理方结果",
                "pipeline_id": pid,
                "items": items,
                "recovered_accepted": items,
                "state": self.snapshot(pid),
            }

    def retry_pending(
        self,
        pipeline_id: str,
        client_refs: list[str] | None = None,
        *,
        window_id: str | None = None,
    ) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在", "items": []}
        pid = _text(pipe.get("管道编号"))
        with self._lock(pid):
            pending = [
                r for r in self._pipeline_receipts(pid)
                if r.get("received") is not True and r.get("retryable") is True
            ]
            if client_refs:
                wanted = {_text(x) for x in client_refs}
                pending = [r for r in pending if _text(r.get("client_ref")) in wanted]
            pending.sort(key=lambda r: int(r.get("id", 0)))
            if not pending:
                return {
                    "ok": True,
                    "message": "没有需要续发的待续办条目",
                    "items": [],
                    "retried": [],
                    "newly_accepted": [],
                    "state": self.snapshot(pid),
                }

            results = []
            for receipt in pending:
                results.append(
                    self._accept_one(
                        pid,
                        _text(receipt.get("client_ref")),
                        receipt.get("payload", ""),
                        _text(receipt.get("ledger_id")) or _short_id("LED"),
                        window_id,
                    )
                )

            results = [self._present(item) for item in results]
            accepted = [r for r in results if r.get("received") is True]
            return {
                "ok": len(accepted) == len(results),
                "message": f"续发完成：{len(accepted)}/{len(results)} 条成交",
                "items": results,
                "retried": results,
                "newly_accepted": accepted,
                "state": self.snapshot(pid),
            }

    def continue_flow(
        self,
        pipeline_id: str,
        client_refs: list[str] | None = None,
        *,
        process_accepted_first: bool = True,
        edge_config_id: str | None = None,
        window_id: str | None = None,
    ) -> dict[str, Any]:
        """先取回/转投已受理方并释放席位，再续发待续办。"""
        recovered = self.recover_accepted(pipeline_id)
        if not recovered.get("ok"):
            return recovered

        relay_result = None
        if process_accepted_first and recovered.get("items"):
            relay_result = self.edge_relay(
                pipeline_id,
                edge_config_id=edge_config_id,
            )
            if not relay_result.get("ok") and relay_result.get("relayed_count", 0) == 0:
                # 如果转投被暂停，则席位不会释放；仍返回结果供页面处理。
                return {
                    "ok": False,
                    "message": "已取回已受理方，但边缘转投未成功，席位尚未释放",
                    "recovered_accepted": recovered.get("items"),
                    "relay": relay_result,
                    "state": self.snapshot(pipeline_id),
                }

        retry_result = self.retry_pending(pipeline_id, client_refs, window_id=window_id)
        return {
            "ok": bool(retry_result.get("ok")),
            "message": f"续发流程完成；{retry_result.get('message', '')}",
            "recovered_accepted": recovered.get("items"),
            "relay": relay_result,
            **retry_result,
        }

    # ------------------------------------------------------------------
    # 边缘缓存顺序读取、转投、结果销单
    # ------------------------------------------------------------------
    def edge_logs(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows("buffer_relay_logs")
        if pipeline_id is None:
            return list(rows)
        pid = self.pipeline_code(pipeline_id)
        return [row for row in rows if row.get("pipeline_id") == pid]

    def downstream_records(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows("buffer_downstream_records")
        if pipeline_id is None:
            return list(rows)
        pid = self.pipeline_code(pipeline_id)
        return [row for row in rows if row.get("pipeline_id") == pid]

    @staticmethod
    def _normalize_payload(payload: str) -> dict[str, Any]:
        import json

        text_value = payload.strip()
        if not text_value:
            return {"raw": "", "fields": {}}
        try:
            parsed = json.loads(text_value)
            if isinstance(parsed, dict):
                return {"raw": text_value, "fields": parsed, "field_count": len(parsed)}
        except (TypeError, ValueError):
            pass
        fields: dict[str, str] = {}
        for part in text_value.replace(";", ",").split(","):
            if "=" in part:
                key, value = part.split("=", 1)
                fields[key.strip()] = value.strip()
            elif part.strip():
                fields[f"field_{len(fields) + 1}"] = part.strip()
        return {"raw": text_value, "fields": fields, "field_count": len(fields)}

    def eligible_for_downstream(self, pipeline_id: str) -> list[dict[str, Any]]:
        """只返回已接收、具备下游消费资格且尚未成功处理的记录。"""
        pid = self.pipeline_code(pipeline_id)
        with self._lock(pid):
            items = [
                self._present(row) for row in self._pipeline_receipts(pid)
                if row.get("received") is True
                and row.get("processing_status") != "已处理"
                and row.get("status") != CLOSED
            ]
            items.sort(key=lambda r: int(r.get("accepted_sequence") or r.get("sequence") or 0))
            return items

    def downstream_consume_next(self, pipeline_id: str, edge_config_id: str | None = None) -> dict[str, Any]:
        """下游只允许通过边缘缓存按顺序消费下一条已受理记录。"""
        eligible = self.eligible_for_downstream(pipeline_id)
        if not eligible:
            return {
                "ok": True,
                "consumed": 0,
                "message": "没有具备下游消费资格的记录；待续发条目不可消费",
                "items": [],
            }
        result = self.edge_relay(pipeline_id, edge_config_id=edge_config_id, limit=1)
        result["consumed"] = result.get("relayed_count", 0)
        return result

    def edge_relay(
        self,
        pipeline_id: str,
        *,
        edge_config_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        pipe = self.find_pipeline(pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在", "items": []}
        pid = _text(pipe.get("管道编号"))

        edge_config = None
        if edge_config_id:
            for cfg in store.rows("edge_cache"):
                if _text(cfg.get("配置编号")) == _text(edge_config_id) or _text(cfg.get("id")) == _text(edge_config_id):
                    edge_config = cfg
                    break
            if edge_config is None:
                return {"ok": False, "message": f"边缘缓存配置 {edge_config_id} 不存在"}

        with self._lock(pid):
            state = self._state_row(pid)
            if state.get("relay_paused"):
                return {
                    "ok": False,
                    "retryable": True,
                    "message": "边缘转投已暂停：已受理条目保留席位，未受理条目仍不具备下游消费资格",
                    "items": [],
                    "state": self.snapshot(pid),
                }

            eligible = [
                r for r in self._pipeline_receipts(pid)
                if r.get("received") is True
                and r.get("status") in (ACCEPTED, FAILED)
                and r.get("processing_status") != "已处理"
            ]
            eligible.sort(key=lambda r: int(r.get("accepted_sequence") or r.get("sequence") or 0))
            if limit is not None:
                eligible = eligible[: max(0, int(limit))]

            forwarded: list[dict[str, Any]] = []
            relay_order: list[Any] = []
            for receipt in eligible:
                ref = _text(receipt.get("client_ref"))
                sequence = int(receipt.get("accepted_sequence") or receipt.get("sequence") or 0)
                relay_order.append({"sequence": sequence, "client_ref": ref, "receipt_id": receipt.get("receipt_id")})

                # 已成功处理的结果绝不重复消费；处理失败的条目允许再次转投。
                if receipt.get("processing_status") == "已处理":
                    forwarded.append(self._present(receipt))
                    continue

                normalized = self._normalize_payload(_text(receipt.get("payload")))
                failure_marker = "__DOWNSTREAM_FAIL__" in _text(receipt.get("payload"))
                delivered_at = _now()
                delivery_count = int(receipt.get("delivery_count", 0)) + 1
                if failure_marker:
                    result = {
                        "consumer": _text(pipe.get("输出目标")) or "downstream",
                        "status": "FAILED",
                        "sequence": sequence,
                        "normalized": normalized,
                        "processed_at": delivered_at,
                        "error": "下游处理返回失败，可重试转投",
                        "delivery_count": delivery_count,
                    }
                    receipt.update({
                        "status": FAILED,
                        "processing_status": "处理失败",
                        "downstream_result": result,
                        "处理结果": result,
                        "result_written_back": True,
                        "processed_at": delivered_at,
                        "delivery_count": delivery_count,
                        "retryable": True,
                        "是否可重试": "是",
                        "message": "下游实际处理失败，结果已销回联单；席位保留以便重试",
                        "updated_at": delivered_at,
                    })
                    store.rows("buffer_downstream_records").append({
                        "id": max((int(r.get("id", 0)) for r in store.rows("buffer_downstream_records")), default=0) + 1,
                        "pipeline_id": pid,
                        "receipt_id": receipt.get("receipt_id"),
                        "client_ref": ref,
                        "sequence": sequence,
                        "result": result,
                        "consumed_at": delivered_at,
                    })
                    continue

                checksum = hashlib.sha256(_text(receipt.get("payload")).encode("utf-8")).hexdigest()[:16]
                result = {
                    "consumer": _text(pipe.get("输出目标")) or "aggregate_sink",
                    "status": "SUCCESS",
                    "output_topic": _text(pipe.get("输出目标")),
                    "sequence": sequence,
                    "normalized_record": normalized,
                    "checksum": checksum,
                    "processed_at": delivered_at,
                    "delivery_count": delivery_count,
                }

                # 释放该受理单对应的真实席位。
                seat_id = receipt.get("seat_id")
                released = False
                for seat in store.rows("buffer_seats"):
                    if seat.get("seat_id") == seat_id and seat.get("status") == "占用中":
                        seat["status"] = "已释放"
                        seat["released_at"] = delivered_at
                        released = True
                        break

                receipt.update({
                    "status": CLOSED,
                    "processing_status": "已处理",
                    "downstream_result": result,
                    "处理结果": result,
                    "result_written_back": True,
                    "processed_at": delivered_at,
                    "closed_at": delivered_at,
                    "delivery_count": delivery_count,
                    "seat_released": released,
                    "retryable": False,
                    "是否可重试": "否",
                    "message": f"边缘缓存已按顺序转投，下游处理成功并销回联单；席位 {seat_id} 已释放",
                    "updated_at": delivered_at,
                })
                store.rows("buffer_downstream_records").append({
                    "id": max((int(r.get("id", 0)) for r in store.rows("buffer_downstream_records")), default=0) + 1,
                    "pipeline_id": pid,
                    "receipt_id": receipt.get("receipt_id"),
                    "client_ref": ref,
                    "sequence": sequence,
                    "result": result,
                    "consumed_at": delivered_at,
                })
                forwarded.append(self._present(receipt))

            log = {
                "id": max((int(r.get("id", 0)) for r in store.rows("buffer_relay_logs")), default=0) + 1,
                "edge_config_id": edge_config_id or (edge_config or {}).get("配置编号"),
                "pipeline_id": pid,
                "read_order": relay_order,
                "forward_order": [item.get("client_ref") for item in forwarded],
                "relayed_count": len(forwarded),
                "relayed_at": _now(),
            }
            store.rows("buffer_relay_logs").append(log)
            if edge_config is not None:
                edge_config["最近刷新"] = _now()
                edge_config["last_buffer_relay"] = log["relayed_at"]

            return {
                "ok": True,
                "message": f"边缘缓存已按受理顺序转投 {len(forwarded)} 条，并把实际处理结果销回联单",
                "pipeline_id": pid,
                "edge_config_id": log["edge_config_id"],
                "relay_order": relay_order,
                "items": forwarded,
                "relayed": forwarded,
                "relayed_count": len(forwarded),
                "log": log,
                "state": self.snapshot(pid),
            }


buffer_flow = BufferFlowService()
# 兼容可能按语义命名导入的调用方。
buffer_service = buffer_flow
BufferService = BufferFlowService
