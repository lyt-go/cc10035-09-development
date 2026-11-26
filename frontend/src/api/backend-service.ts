import { fetchJson, request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type BufferReceipt = {
  id?: number
  receipt_id?: string
  ledger_id?: string
  pipeline_id: string
  client_ref: string
  payload?: string
  received: boolean
  retryable: boolean
  accepted?: boolean
  duplicate?: boolean
  claimed_existing?: boolean
  status: string
  message?: string
  reason?: string
  reason_code?: string | null
  seat_id?: string | null
  seat_released?: boolean
  sequence?: number | null
  accepted_sequence?: number | null
  attempt_count?: number
  processing_status?: string
  downstream_result?: Record<string, unknown> | null
  处理结果?: Record<string, unknown> | null
  result_written_back?: boolean
  delivery_count?: number
  [key: string]: unknown
}

export type BufferPipelineState = {
  pipeline_id: string
  管道编号?: string
  capacity: number
  occupied_seats: number
  available_seats: number
  over_capacity?: boolean
  paused: boolean
  manual_paused?: boolean
  pipeline_stopped?: boolean
  relay_paused: boolean
  accepted_count?: number
  pending_count?: number
  processed_count?: number
  status?: string
  运行状态?: string
}

export type BufferLedger = {
  ok: boolean
  message: string
  state?: BufferPipelineState
  pipelines: BufferPipelineState[]
  items: BufferReceipt[]
  receipts?: BufferReceipt[]
  accepted: BufferReceipt[]
  pending: BufferReceipt[]
  processed: BufferReceipt[]
  downstream: Array<Record<string, unknown>>
  edge_logs: Array<Record<string, unknown>>
}

export type BufferAcceptResult = {
  ok: boolean
  message: string
  ledger_id?: string
  pipeline_id?: string
  items?: BufferReceipt[]
  receipts?: BufferReceipt[]
  accepted_count?: number
  rejected_count?: number
  state?: BufferPipelineState
  recovered_accepted?: BufferReceipt[]
  relay?: BufferRelayResult
}

export type BufferRelayResult = {
  ok: boolean
  message: string
  relay_order?: Array<{ sequence: number; client_ref: string; receipt_id?: string }>
  items?: BufferReceipt[]
  relayed?: BufferReceipt[]
  relayed_count?: number
  state?: BufferPipelineState
}

async function postBuffer<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  const data = (await response.json().catch(() => ({}))) as T & { message?: string }
  if (!response.ok) {
    throw new Error(data.message || `受理缓冲接口返回 ${response.status}`)
  }
  return data
}

export function getBufferLedger(pipelineId?: string): Promise<BufferLedger> {
  const query = pipelineId ? `?pipeline_id=${encodeURIComponent(pipelineId)}` : ''
  return fetchJson<BufferLedger>(`/api/data_stream/buffer/ledger${query}`)
}

export function acceptBufferOrders(
  pipelineId: string,
  orders: Array<Record<string, unknown>>,
  extra: Record<string, unknown> = {},
): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/accept', {
    pipeline_id: pipelineId,
    orders,
    ...extra,
  })
}

export function setBufferCapacity(
  pipelineId: string,
  body: { capacity?: number; remaining?: number },
): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/capacity', {
    pipeline_id: pipelineId,
    ...body,
  })
}

export function setBufferPaused(
  pipelineId: string,
  body: { paused?: boolean; relay_paused?: boolean },
): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/pause', {
    pipeline_id: pipelineId,
    ...body,
  })
}

export function recoverAcceptedBuffer(pipelineId: string): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/recover', {
    pipeline_id: pipelineId,
  })
}

export function retryBufferPending(
  pipelineId: string,
  clientRefs?: string[],
): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/retry', {
    pipeline_id: pipelineId,
    client_refs: clientRefs,
  })
}

export function continueBufferFlow(
  pipelineId: string,
  clientRefs?: string[],
  edgeConfigId?: string,
): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/continue', {
    pipeline_id: pipelineId,
    client_refs: clientRefs,
    edge_config_id: edgeConfigId,
  })
}

export function relayEdgeBuffer(
  pipelineId: string,
  edgeConfigId?: string,
  limit?: number,
): Promise<BufferRelayResult> {
  return postBuffer<BufferRelayResult>('/api/edge_cache/buffer/relay', {
    pipeline_id: pipelineId,
    edge_config_id: edgeConfigId,
    limit,
  })
}

export function resetBufferFlow(pipelineId?: string): Promise<BufferAcceptResult> {
  return postBuffer<BufferAcceptResult>('/api/data_stream/buffer/reset', {
    pipeline_id: pipelineId,
  })
}
