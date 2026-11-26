"""浏览器受理缓冲流程接口。

前端主要使用 ``/api/data_stream/buffer/*``；同时保留 ``/api/buffer/*`` 简短
别名，方便脚本和并发测试直接调用。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, Request

from app.services.buffer_flow import buffer_flow

router = APIRouter(tags=["管道受理缓冲"])


def _pick(body: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in body and body[key] is not None:
            return body[key]
    return default


def _orders_from_body(body: dict[str, Any]) -> list[dict[str, Any]]:
    orders = _pick(body, "orders", "items", "records", "试单列表", default=[])
    if isinstance(orders, dict):
        orders = [orders]
    if not isinstance(orders, list):
        orders = []

    if not orders:
        raw_orders = _pick(body, "raw_orders", "text", "batch_text")
        if isinstance(raw_orders, str) and raw_orders.strip():
            parsed: list[dict[str, Any]] = []
            for line in raw_orders.splitlines():
                line = line.strip()
                if not line:
                    continue
                ref = ""
                payload = line
                if "|" in line:
                    ref, payload = line.split("|", 1)
                elif "\t" in line:
                    ref, payload = line.split("\t", 1)
                parsed.append({"client_ref": ref.strip(), "payload": payload.strip()})
            orders = parsed

    # 单条试单直接放在请求体里。
    if not orders and (_pick(body, "client_ref", "clientRef", "试单号") is not None or _pick(body, "payload", "content", "内容") is not None):
        orders = [{
            "client_ref": _pick(body, "client_ref", "clientRef", "request_id", "idempotency_key", "试单号"),
            "payload": _pick(body, "payload", "content", "内容", default=""),
        }]
    return orders


def _pid_from(body: dict[str, Any], path_pid: str | None = None) -> str:
    return str(path_pid or _pick(body, "pipeline_id", "pipelineId", "管道编号", "stream_id", default=""))


@router.get("/api/data_stream/buffer/pipelines")
@router.get("/api/data-stream/buffer/pipelines")
@router.get("/api/buffer/pipelines")
def buffer_pipelines() -> dict[str, Any]:
    return {"ok": True, "items": buffer_flow.list_pipeline_states()}


@router.get("/api/data_stream/buffer/ledger")
@router.get("/api/data_stream/buffer")
@router.get("/api/data_stream/buffer/receipts")
@router.get("/api/data-stream/buffer/ledger")
@router.get("/api/buffer/ledger")
@router.get("/api/buffer/receipts")
def buffer_ledger(pipeline_id: str | None = None) -> dict[str, Any]:
    return buffer_flow.ledger(pipeline_id)


@router.get("/api/data_stream/{pipeline_id}/buffer")
@router.get("/api/data_stream/{pipeline_id}/buffer/ledger")
def buffer_ledger_by_path(pipeline_id: str) -> dict[str, Any]:
    return buffer_flow.ledger(pipeline_id)


@router.post("/api/data_stream/buffer/accept")
@router.post("/api/data_stream/buffer/submit")
@router.post("/api/data_stream/buffer/orders")
@router.post("/api/data_stream/inflow")
@router.post("/api/data-stream/buffer/accept")
@router.post("/api/buffer/accept")
@router.post("/api/buffer/submit")
@router.post("/api/buffer/orders")
async def accept_buffer(
    request: Request,
    pipeline_id: str | None = None,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    x_window_id: str | None = Header(default=None, alias="X-Window-Id"),
) -> dict[str, Any]:
    body = await _json(request)
    pipeline_id = _pid_from(body, pipeline_id)
    orders = _orders_from_body(body)
    if idempotency_key and orders and isinstance(orders[0], dict):
        orders[0].setdefault("client_ref", idempotency_key)
    return buffer_flow.accept_orders(
        pipeline_id,
        orders,
        window_id=str(_pick(body, "window_id", "windowId", default=x_window_id or "") or "") or None,
        ledger_id=_pick(body, "ledger_id", "batch_id"),
    )


@router.post("/api/data_stream/{pipeline_id}/buffer")
@router.post("/api/data_stream/{pipeline_id}/buffer/accept")
async def accept_buffer_by_path(
    pipeline_id: str,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    body = await _json(request)
    orders = _orders_from_body(body)
    if idempotency_key and orders and isinstance(orders[0], dict):
        orders[0].setdefault("client_ref", idempotency_key)
    return buffer_flow.accept_orders(pipeline_id, orders, window_id=body.get("window_id"))


@router.post("/api/data_stream/buffer/capacity")
@router.post("/api/data-stream/buffer/capacity")
@router.post("/api/buffer/capacity")
async def set_capacity(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.set_capacity(
        _pid_from(body),
        _pick(body, "capacity", "buffer_capacity", "seat_count", "max_seats", "缓冲席位容量", "席位容量"),
        remaining=_pick(body, "remaining", "remaining_seats", "剩余席位"),
    )


@router.post("/api/data_stream/buffer/pause")
@router.post("/api/data-stream/buffer/pause")
@router.post("/api/buffer/pause")
async def set_pause(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.set_paused(
        _pid_from(body),
        _pick(body, "paused", "accept_paused", "受理已暂停"),
        relay_paused=_pick(body, "relay_paused", "forward_paused", "转投已暂停"),
    )


@router.post("/api/data_stream/buffer/recover")
@router.post("/api/data-stream/buffer/recover")
@router.post("/api/buffer/recover")
async def recover(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.recover_accepted(_pid_from(body))


@router.post("/api/data_stream/buffer/retry")
@router.post("/api/data-stream/buffer/retry")
@router.post("/api/buffer/retry")
async def retry(request: Request) -> dict[str, Any]:
    body = await _json(request)
    refs = _pick(body, "client_refs", "refs", "试单号列表")
    if refs is None:
        single = _pick(body, "client_ref", "clientRef", "试单号")
        refs = [single] if single else None
    return buffer_flow.retry_pending(
        _pid_from(body),
        refs,
        window_id=_pick(body, "window_id", "windowId"),
    )


@router.post("/api/data_stream/buffer/continue")
@router.post("/api/data-stream/buffer/continue")
@router.post("/api/buffer/continue")
async def continue_flow(request: Request) -> dict[str, Any]:
    body = await _json(request)
    refs = _pick(body, "client_refs", "refs", "试单号列表")
    return buffer_flow.continue_flow(
        _pid_from(body),
        refs,
        process_accepted_first=bool(_pick(body, "process_accepted_first", "relay_first", default=True)),
        edge_config_id=_pick(body, "edge_config_id", "config_id", "配置编号"),
        window_id=_pick(body, "window_id", "windowId"),
    )


@router.post("/api/edge_cache/buffer/relay")
@router.post("/api/edge-cache/buffer/relay")
@router.post("/api/data_stream/buffer/relay")
@router.post("/api/data-stream/buffer/relay")
@router.post("/api/buffer/relay")
async def edge_relay(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.edge_relay(
        _pid_from(body),
        edge_config_id=_pick(body, "edge_config_id", "config_id", "配置编号"),
        limit=_pick(body, "limit", "条数"),
    )


@router.get("/api/data_stream/buffer/downstream/eligible")
@router.get("/api/buffer/downstream/eligible")
def downstream_eligible(pipeline_id: str) -> dict[str, Any]:
    return {
        "ok": True,
        "pipeline_id": pipeline_id,
        "items": buffer_flow.eligible_for_downstream(pipeline_id),
    }


@router.post("/api/data_stream/buffer/downstream/consume-next")
@router.post("/api/buffer/downstream/consume-next")
async def downstream_consume_next(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.downstream_consume_next(
        _pid_from(body),
        edge_config_id=_pick(body, "edge_config_id", "config_id", "配置编号"),
    )


@router.get("/api/edge_cache/buffer/ledger")
@router.get("/api/edge-cache/buffer/ledger")
def edge_ledger(pipeline_id: str | None = None) -> dict[str, Any]:
    return buffer_flow.ledger(pipeline_id)


@router.post("/api/data_stream/buffer/reset")
@router.post("/api/buffer/reset")
async def reset(request: Request) -> dict[str, Any]:
    body = await _json(request)
    return buffer_flow.reset(_pid_from(body) or None)


async def _json(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {"orders": data}
