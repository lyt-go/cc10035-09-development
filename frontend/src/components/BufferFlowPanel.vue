<template>
  <section
    class="buffer-console"
    data-testid="buffer-console"
    :data-context="moduleKey"
    :data-pipeline="pipelineId"
  >
    <div class="buffer-head">
      <div>
        <h3>浏览器受理缓冲流程</h3>
        <p>
          向指定管道提交流入试单，返回逐条说明「是否已接收、是否可重试」的接收联单。
          已接收条目对应真实队列席位；未接收条目不占席位，可在容量恢复后续发。
        </p>
      </div>
      <button class="btn ghost" data-testid="buffer-refresh" type="button" @click="refresh(false)">
        刷新联单
      </button>
    </div>

    <div v-if="actionError" class="buffer-alert error" data-testid="buffer-error" role="alert">
      {{ actionError }}
    </div>
    <div v-if="notice" class="buffer-alert ok" data-testid="buffer-notice" role="status">
      {{ notice }}
    </div>
    <div v-if="answerInterrupted" class="buffer-alert interrupted" data-testid="answer-interrupted" role="alert">
      <strong>答复中断：</strong>上送端没有收到确认。服务端可能已经受理并占用席位，
      请勿更换试单号；直接点击「重提同一试单（认领结果）」即可认领既有联单。
    </div>

    <div class="buffer-controls">
      <label class="buffer-field">
        <span>指定管道</span>
        <select
          v-model="pipelineId"
          aria-label="管道编号"
          data-testid="buffer-pipeline-select"
          @change="refresh(false)"
        >
          <option v-for="item in pipelines" :key="item.pipeline_id" :value="item.pipeline_id">
            {{ item.pipeline_id }}（{{ item.status || item.运行状态 || '未标记状态' }}）
          </option>
        </select>
      </label>

      <label class="buffer-field narrow">
        <span>缓冲席位容量</span>
        <input
          v-model.number="capacityInput"
          aria-label="缓冲席位容量"
          data-testid="buffer-capacity-input"
          type="number"
          min="0"
          step="1"
          @focus="capacityEditing = true"
          @blur="capacityEditing = false"
        />
      </label>
      <button class="btn" data-testid="apply-capacity" type="button" :disabled="working" @click="applyCapacity">
        调整容量
      </button>
      <button
        class="btn"
        data-testid="set-one-seat-left"
        type="button"
        :disabled="working"
        @click="setOneSeatLeft"
      >
        给当前管道仅留一个席位
      </button>
      <button
        class="btn"
        :class="{ warn: state?.manual_paused }"
        data-testid="toggle-accept-pause"
        type="button"
        :disabled="working"
        @click="toggleAcceptPause"
      >
        {{ state?.manual_paused ? '恢复受理' : '暂停受理' }}
      </button>
      <button
        class="btn"
        :class="{ warn: state?.relay_paused }"
        data-testid="toggle-relay-pause"
        type="button"
        :disabled="working"
        @click="toggleRelayPause"
      >
        {{ state?.relay_paused ? '恢复边缘转投' : '暂停边缘转投' }}
      </button>
      <button class="btn ghost danger" data-testid="reset-buffer" type="button" @click="resetFlow">
        清空本管道演示联单
      </button>
    </div>

    <div
      class="seat-status"
      data-testid="seat-status"
      :data-capacity="state?.capacity ?? 0"
      :data-occupied="state?.occupied_seats ?? 0"
      :data-available="state?.available_seats ?? 0"
    >
      <span><b>当前管道：</b>{{ pipelineId }}</span>
      <span><b>总席位：</b>{{ state?.capacity ?? '—' }}</span>
      <span><b>已受理占席：</b>{{ state?.occupied_seats ?? '—' }}</span>
      <span><b>剩余席位：</b>{{ state?.available_seats ?? '—' }}</span>
      <span v-if="state?.over_capacity" class="over-capacity" data-testid="over-capacity">
        容量已下调至当前占用数以下；既有已受理条目保留，新条目继续待续发
      </span>
      <span data-testid="accept-pause-status">
        受理：{{ state?.paused ? (state.pipeline_stopped ? '管道已停用' : '已暂停') : '开放中' }}
      </span>
      <span data-testid="relay-pause-status">转投：{{ state?.relay_paused ? '已暂停' : '运行中' }}</span>
      <span class="window-id" data-testid="window-id">窗口标识：{{ windowId }}</span>
    </div>

    <div class="other-capacities" data-testid="pipeline-capacity-list">
      <span class="other-title">各管道容量（相互隔离）：</span>
      <span
        v-for="item in pipelines"
        :key="item.pipeline_id"
        class="capacity-chip"
        :data-testid="`capacity-${item.pipeline_id}`"
        :data-pipeline="item.pipeline_id"
        :data-capacity="item.capacity"
        :data-occupied="item.occupied_seats"
        :data-available="item.available_seats"
      >
        {{ item.pipeline_id }}：容量 {{ item.capacity }} / 占用 {{ item.occupied_seats }} / 剩余 {{ item.available_seats }}
      </span>
    </div>

    <div class="submit-grid">
      <label class="buffer-field">
        <span>试单号 / 幂等键（确认丢失后仍填同一个）</span>
        <input
          v-model="orderRef"
          aria-label="试单号"
          data-testid="order-ref"
          type="text"
          placeholder="例如 A"
          autocomplete="off"
        />
      </label>
      <label class="buffer-field grow">
        <span>流入试单内容</span>
        <input
          v-model="orderPayload"
          aria-label="流入试单内容"
          data-testid="order-payload"
          type="text"
          placeholder="例如 device=DEVI-0001,temp=26.5"
          autocomplete="off"
          @keydown.enter="submitSingle(false)"
        />
      </label>
      <button class="btn primary" data-testid="submit-order" type="button" :disabled="working" @click="submitSingle(false)">
        提交流入试单
      </button>
      <button
        class="btn"
        :class="{ warn: dropNextAck }"
        data-testid="simulate-interrupt"
        type="button"
        :disabled="working"
        title="武装后，下一次提交即使收到服务端答复，也会在模拟上送端将其丢弃；服务端受理和席位不回滚"
        @click="simulateInterrupt"
      >
        {{ interruptButtonLabel }}
      </button>
      <button class="btn primary" data-testid="retry-same-order" type="button" :disabled="working" @click="submitSingle(true)">
        重提同一试单（认领结果）
      </button>
    </div>

    <div class="batch-box">
      <label class="buffer-field">
        <span>批量流入试单（每行一条；可写「试单号|内容」或「试单号,内容」，单独的 A/B/C 会作为试单号）</span>
        <textarea
          v-model="batchText"
          aria-label="批量流入试单"
          data-testid="batch-orders"
          rows="4"
          placeholder="A|温度试单A&#10;B|温度试单B&#10;C|温度试单C"
        ></textarea>
      </label>
      <div class="batch-actions">
        <button class="btn" data-testid="fill-batch-example" type="button" @click="fillExample">
          填入 A/B/C 示例
        </button>
        <button class="btn primary" data-testid="submit-batch" type="button" :disabled="working" @click="submitBatch">
          提交整批并返回接收联单
        </button>
      </div>
    </div>

    <div v-if="receiptView.length" class="receipt-ledger" data-testid="receipt-ledger">
      <div class="section-head">
        <h4>{{ receiptTitle }}</h4>
        <span v-if="lastLedgerId" data-testid="ledger-id">联单编号：{{ lastLedgerId }}</span>
      </div>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>试单号</th>
              <th>是否已接收</th>
              <th>是否可重试</th>
              <th>受理状态</th>
              <th>队列席位</th>
              <th>顺序</th>
              <th>次数/认领</th>
              <th>逐条说明</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="item in receiptView"
              :key="`${item.receipt_id || item.client_ref}-view`"
              :data-testid="`receipt-row-${item.client_ref}`"
              :data-received="String(Boolean(item.received))"
              :data-retryable="String(Boolean(item.retryable))"
              :data-status="String(item.status)"
            >
              <td>{{ item.client_ref }}</td>
              <td class="boolean" :data-testid="`received-${item.client_ref}`">
                {{ item.received ? '已接收' : '未接收' }}
              </td>
              <td class="boolean" :data-testid="`retryable-${item.client_ref}`">
                {{ item.retryable ? '可重试' : '不可重试' }}
              </td>
              <td>{{ item.status }}</td>
              <td :data-testid="`seat-${item.client_ref}`">{{ item.seat_id || '未占席' }}</td>
              <td>{{ item.accepted_sequence ?? item.sequence ?? '—' }}</td>
              <td>
                第 {{ item.attempt_count ?? 1 }} 次
                <span v-if="item.claimed_existing || item.duplicate" class="badge idempotent">已认领既有结果</span>
              </td>
              <td>{{ item.message || item.reason || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="buffer-actions-bar">
      <button class="btn" data-testid="recover-accepted" type="button" :disabled="working" @click="recoverAccepted">
        取回已受理方
      </button>
      <button class="btn" data-testid="retry-pending" type="button" :disabled="working" @click="retryPending(undefined)">
        续发全部待续办
      </button>
      <button class="btn primary" data-testid="continue-flow" type="button" :disabled="working" @click="continueFlow">
        一键续发：先取回已受理方 → 边缘转投释放席位 → 续发待续办
      </button>
      <label v-if="moduleKey === 'edge_cache'" class="buffer-field narrow inline">
        <span>关联边缘缓存配置</span>
        <select v-model="edgeConfigId" data-testid="edge-config-select">
          <option value="">不关联具体配置</option>
          <option v-for="cfg in edgeConfigs" :key="String(cfg.id)" :value="String(cfg['配置编号'])">
            {{ cfg['配置编号'] }}（{{ cfg.status }}）
          </option>
        </select>
      </label>
      <button class="btn primary" data-testid="edge-relay" type="button" :disabled="working" @click="relayAll">
        边缘缓存读取已接收条目并按顺序转投
      </button>
      <button class="btn" data-testid="edge-relay-one" type="button" :disabled="working" @click="relayOne">
        只顺序转投下一条
      </button>
    </div>

    <div v-if="lastRelay" class="buffer-alert" :class="lastRelay.ok ? 'ok' : 'error'" data-testid="relay-result">
      {{ lastRelay.message }}
      <span v-if="lastRelay.relay_order?.length">
        转投顺序：{{ lastRelay.relay_order.map((item) => `${item.sequence}.${item.client_ref}`).join(' → ') }}
      </span>
    </div>

    <h4 class="ledger-title" data-testid="buffer-ledger-title">
      实时接收联单台账（暂停或容量变化后仍分清已接收、待续发与已销单）
    </h4>
    <div class="ledger-grid">
      <section class="ledger-panel accepted" data-testid="accepted-panel">
        <h4>已接收 / 已受理 / 已占席（{{ ledger?.accepted.length ?? 0 }}）</h4>
        <p class="panel-hint">这些条目已有真实席位；暂停或容量变化不会把它们降级为待续发。</p>
        <div class="table-scroll">
          <table>
            <tbody>
              <tr
                v-for="item in ledger?.accepted ?? []"
                :key="`accepted-${item.client_ref}`"
                :data-testid="`accepted-row-${item.client_ref}`"
                :data-seat="String(item.seat_id || '')"
              >
                <td>
                  <strong>{{ item.client_ref }}</strong>
                  <div class="muted">顺序 {{ item.accepted_sequence ?? item.sequence }} · {{ item.seat_id }}</div>
                  <div>{{ item.message }}</div>
                </td>
                <td class="row-actions vertical">
                  <button class="link" type="button" :data-testid="`replay-${item.client_ref}`" @click="replayReceipt(item)">
                    丢确认后重提认领
                  </button>
                </td>
              </tr>
              <tr v-if="!(ledger?.accepted.length)">
                <td class="empty-state">暂无已受理占席条目</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="ledger-panel pending" data-testid="pending-panel">
        <h4>未接收 / 待续发 / 未占席（{{ ledger?.pending.length ?? 0 }}）</h4>
        <p class="panel-hint">这些记录尚不具备下游消费资格；容量或受理恢复后可用同一试单号续发。</p>
        <div class="table-scroll">
          <table>
            <tbody>
              <tr
                v-for="item in ledger?.pending ?? []"
                :key="`pending-${item.client_ref}`"
                :data-testid="`pending-row-${item.client_ref}`"
                :data-retryable="String(item.retryable)"
              >
                <td>
                  <strong>{{ item.client_ref }}</strong>
                  <span class="badge retry">可重试：{{ item.retryable ? '是' : '否' }}</span>
                  <div class="muted">{{ item.reason || item.message }}</div>
                </td>
                <td class="row-actions vertical">
                  <button
                    class="link"
                    type="button"
                    :data-testid="`retry-one-${item.client_ref}`"
                    @click="retryPending([item.client_ref])"
                  >
                    续发此条
                  </button>
                </td>
              </tr>
              <tr v-if="!(ledger?.pending.length)">
                <td class="empty-state">暂无待续发条目</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>

    <section class="ledger-panel processed" data-testid="processed-panel">
      <h4>已顺序转投并销回联单（{{ ledger?.processed.length ?? 0 }}）</h4>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>顺序</th>
              <th>试单号</th>
              <th>席位释放</th>
              <th>下游实际处理结果（已销回联单）</th>
              <th>交付次数</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="item in ledger?.processed ?? []"
              :key="`processed-${item.client_ref}`"
              :data-testid="`processed-row-${item.client_ref}`"
            >
              <td>{{ item.accepted_sequence ?? item.sequence }}</td>
              <td>{{ item.client_ref }}</td>
              <td :data-testid="`seat-released-${item.client_ref}`">{{ item.seat_released ? '已释放' : '未释放' }}</td>
              <td><pre class="result-json" data-testid="processing-result">{{ formatResult(item) }}</pre></td>
              <td :data-testid="`delivery-count-${item.client_ref}`">{{ deliveryCount(item.client_ref) }}</td>
            </tr>
            <tr v-if="!(ledger?.processed.length)">
              <td colspan="5" class="empty-state">边缘缓存尚未转投并销单</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="ledger-panel downstream" data-testid="downstream-panel">
      <h4>下游消费记录</h4>
      <p class="panel-hint">这里只会出现已接收且经边缘缓存转投的记录；待续发条目不可被下游消费。</p>
      <div class="downstream-refs" data-testid="downstream-refs">
        <span
          v-for="record in ledger?.downstream ?? []"
          :key="String(record.receipt_id || record.client_ref)"
          class="downstream-chip"
          :data-testid="`downstream-${record.client_ref}`"
        >
          #{{ record.sequence }} {{ record.client_ref }}：{{ String((record.result as Record<string, unknown>)?.status || '已消费') }}
        </span>
        <span v-if="!(ledger?.downstream.length)" class="muted">下游暂无消费记录</span>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import {
  acceptBufferOrders,
  continueBufferFlow,
  getBufferLedger,
  listEntries,
  recoverAcceptedBuffer,
  relayEdgeBuffer,
  resetBufferFlow,
  retryBufferPending,
  setBufferCapacity,
  setBufferPaused,
  type BufferLedger,
  type BufferPipelineState,
  type BufferReceipt,
  type BufferRelayResult,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const route = useRoute()

const queryPipeline = typeof route.query.pipeline === 'string'
  ? route.query.pipeline
  : typeof route.query.pipeline_id === 'string'
    ? route.query.pipeline_id
    : ''
const pipelineId = ref(queryPipeline || 'DATA-0003')
const ledger = ref<BufferLedger | null>(null)
const edgeConfigs = ref<EntryRow[]>([])
const edgeConfigId = ref('EDGE-0003')
const capacityInput = ref<number | null>(3)
const capacityEditing = ref(false)

const orderRef = ref('')
const orderPayload = ref('')
const batchText = ref('')
const windowId = `WIN-${Math.random().toString(36).slice(2, 8).toUpperCase()}`

const working = ref(false)
const actionError = ref('')
const notice = ref('')
const answerInterrupted = ref(false)
const dropNextAck = ref(false)
const lastLedgerId = ref('')
const receiptView = ref<BufferReceipt[]>([])
const receiptTitle = ref('接收联单（逐条说明是否已接收、是否可重试）')
const lastRelay = ref<BufferRelayResult | null>(null)

let pollTimer: number | undefined

const pipelines = computed<BufferPipelineState[]>(() => ledger.value?.pipelines ?? [])
const state = computed<BufferPipelineState | null>(() => ledger.value?.state ?? null)
const interruptButtonLabel = computed(() => {
  if (dropNextAck.value) return '取消答复中断演练（已设置下次提交丢弃答复）'
  if (receiptView.value.length || lastLedgerId.value) return '模拟答复中断/确认丢失（丢失当前答复）'
  return '模拟答复中断/确认丢失'
})

watch(
  () => ledger.value?.state?.capacity,
  (capacity) => {
    if (!capacityEditing.value && typeof capacity === 'number') {
      capacityInput.value = capacity
    }
  },
)

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : '请求失败'
}

async function refresh(showError = true): Promise<void> {
  try {
    const data = await getBufferLedger(pipelineId.value)
    ledger.value = data
    if (data.state?.pipeline_id) pipelineId.value = data.state.pipeline_id
  } catch (error) {
    if (showError) actionError.value = `受理缓冲状态读取失败：${errorText(error)}`
  }
}

async function refreshSilently(): Promise<void> {
  if (!working.value) {
    await refresh(false)
  }
}

async function withWorking(task: () => Promise<void>, options: { interruptOnThrow?: boolean } = {}) {
  working.value = true
  actionError.value = ''
  notice.value = ''
  try {
    await task()
  } catch (error) {
    if (options.interruptOnThrow) {
      answerInterrupted.value = true
      dropNextAck.value = false
    }
    actionError.value = errorText(error)
  } finally {
    working.value = false
  }
}

async function ensurePipeline(): Promise<boolean> {
  if (!pipelines.value.some((item) => item.pipeline_id === pipelineId.value)) {
    const first = pipelines.value[0]?.pipeline_id
    if (first) pipelineId.value = first
  }
  return Boolean(pipelineId.value)
}

async function submitSingle(isReplay: boolean): Promise<void> {
  const ref = orderRef.value.trim()
  if (!ref) {
    actionError.value = '请先填写试单号；丢失确认后的重提必须使用同一个试单号。'
    return
  }
  answerInterrupted.value = false
  await withWorking(async () => {
    const result = await acceptBufferOrders(
      pipelineId.value,
      [{ client_ref: ref, payload: orderPayload.value }],
      { window_id: windowId },
    )
    if (dropNextAck.value) {
      dropNextAck.value = false
      answerInterrupted.value = true
      receiptView.value = []
      lastLedgerId.value = ''
      notice.value = '已按演练在客户端丢弃本次答复；服务端联单和真实席位已提交，请用原试单号重提认领。'
      await refresh(false)
      return
    }
    lastLedgerId.value = result.ledger_id || lastLedgerId.value
    receiptTitle.value = isReplay ? '重提认领后的接收联单（同一结果，不新增席位）' : '接收联单（逐条说明是否已接收、是否可重试）'
    receiptView.value = result.items || []
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  }, { interruptOnThrow: true })
}

function simulateInterrupt(): void {
  if (dropNextAck.value) {
    dropNextAck.value = false
    answerInterrupted.value = false
    notice.value = '已取消答复中断演练。'
    return
  }

  if (receiptView.value.length || lastLedgerId.value) {
    // 模拟“已经收到响应后、上送端确认前丢失”：只清空本地答复，不武装下一次重提。
    dropNextAck.value = false
    answerInterrupted.value = true
    receiptView.value = []
    lastLedgerId.value = ''
    notice.value = '已模拟当前确认丢失：服务端联单和席位仍在，请用原试单号重提认领。'
    return
  }

  // 当前没有可丢失的答复时，武装下一次提交，在客户端丢弃其响应。
  dropNextAck.value = true
  answerInterrupted.value = true
  notice.value = '已武装答复中断：下一次提交的确认会在客户端丢失，但不会通知后端撤销已受理席位。'
}

function fillExample(): void {
  batchText.value = 'A|流入试单A\nB|流入试单B\nC|流入试单C'
}

function stableRef(text: string): string {
  let hash = 0
  for (let index = 0; index < text.length; index += 1) {
    hash = (hash * 31 + text.charCodeAt(index)) >>> 0
  }
  return `AUTO-${hash.toString(16).toUpperCase().padStart(8, '0')}`
}

function parseBatch(text: string): Array<{ client_ref: string; payload: string }> {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      if (line.includes('|')) {
        const [ref, ...rest] = line.split('|')
        return { client_ref: ref.trim(), payload: rest.join('|').trim() }
      }
      if (line.includes('\t')) {
        const [ref, ...rest] = line.split('\t')
        return { client_ref: ref.trim(), payload: rest.join('\t').trim() }
      }
      const match = line.match(/^([A-Za-z0-9_\-一-龥]{1,32})[,，:：]\s*(.+)$/)
      if (match) return { client_ref: match[1], payload: match[2] }
      if (/^[A-Za-z0-9_\-一-龥]{1,32}$/.test(line)) {
        return { client_ref: line, payload: line }
      }
      return { client_ref: stableRef(line), payload: line }
    })
}

async function submitBatch(): Promise<void> {
  let orders: Array<Record<string, unknown>> = []
  const batchInput = batchText.value.trim()
  try {
    if (batchInput.startsWith('[')) {
      const parsed = JSON.parse(batchInput) as unknown
      if (Array.isArray(parsed)) {
        orders = parsed.map((item) =>
          typeof item === 'string' ? { client_ref: item, payload: item } : (item as Record<string, unknown>),
        )
      }
    } else if (batchInput.startsWith('{')) {
      orders = [JSON.parse(batchInput) as Record<string, unknown>]
    } else {
      orders = parseBatch(batchInput)
    }
  } catch {
    actionError.value = '批量试单格式无法解析；请使用每行一条、「试单号|内容」或 JSON 数组。'
    return
  }

  if (!orders.length) {
    actionError.value = '请先输入至少一条流入试单。'
    return
  }
  answerInterrupted.value = false
  await withWorking(async () => {
    const result = await acceptBufferOrders(pipelineId.value, orders, { window_id: windowId })
    lastLedgerId.value = result.ledger_id || ''
    receiptTitle.value = '批量接收联单（压过容量的条目仍标记为可重试）'
    receiptView.value = result.items || []
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  }, { interruptOnThrow: true })
}

async function applyCapacity(): Promise<void> {
  const capacity = Number(capacityInput.value)
  if (!Number.isInteger(capacity) || capacity < 0) {
    actionError.value = '容量必须是大于等于 0 的整数。'
    return
  }
  await withWorking(async () => {
    const result = await setBufferCapacity(pipelineId.value, { capacity })
    notice.value = result.message
    await refresh(false)
  })
}

async function setOneSeatLeft(): Promise<void> {
  await withWorking(async () => {
    const result = await setBufferCapacity(pipelineId.value, { remaining: 1 })
    notice.value = result.message
    capacityInput.value = result.state?.capacity ?? capacityInput.value
    await refresh(false)
  })
}

async function toggleAcceptPause(): Promise<void> {
  const next = !ledger.value?.state?.manual_paused
  await withWorking(async () => {
    const result = await setBufferPaused(pipelineId.value, { paused: next })
    notice.value = result.message
    await refresh(false)
  })
}

async function toggleRelayPause(): Promise<void> {
  const next = !ledger.value?.state?.relay_paused
  await withWorking(async () => {
    const result = await setBufferPaused(pipelineId.value, { relay_paused: next })
    notice.value = result.message
    await refresh(false)
  })
}

async function recoverAccepted(): Promise<void> {
  await withWorking(async () => {
    const result = await recoverAcceptedBuffer(pipelineId.value)
    const items = result.recovered_accepted || result.items || []
    receiptTitle.value = '已取回的已受理方（真实席位仍占用）'
    receiptView.value = items
    notice.value = result.message
    await refresh(false)
  })
}

async function retryPending(refs?: string[]): Promise<void> {
  await withWorking(async () => {
    const result = await retryBufferPending(pipelineId.value, refs)
    receiptTitle.value = refs ? `续发结果：${refs.join(', ')}` : '续发全部待续办的结果联单'
    receiptView.value = result.items || []
    lastLedgerId.value = ''
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  })
}

async function continueFlow(): Promise<void> {
  await withWorking(async () => {
    const result = await continueBufferFlow(pipelineId.value, undefined, edgeConfigId.value || undefined)
    receiptTitle.value = '完整续发联单：先认领已受理方，释放席位后另一条目成交'
    receiptView.value = [
      ...(result.recovered_accepted || []),
      ...(result.items || []),
    ]
    lastRelay.value = result.relay || null
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  })
}

async function relayAll(): Promise<void> {
  await withWorking(async () => {
    const result = await relayEdgeBuffer(pipelineId.value, edgeConfigId.value || undefined)
    lastRelay.value = result
    receiptTitle.value = '边缘转投并销回的联单'
    receiptView.value = result.items || []
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  })
}

async function relayOne(): Promise<void> {
  await withWorking(async () => {
    const result = await relayEdgeBuffer(pipelineId.value, edgeConfigId.value || undefined, 1)
    lastRelay.value = result
    receiptTitle.value = '边缘缓存顺序转投下一条的销单结果'
    receiptView.value = result.items || []
    notice.value = result.message
    if (!result.ok) actionError.value = result.message
    await refresh(false)
  })
}

function replayReceipt(item: BufferReceipt): void {
  orderRef.value = item.client_ref
  orderPayload.value = String(item.payload ?? '')
  void submitSingle(true)
}

async function resetFlow(): Promise<void> {
  await withWorking(async () => {
    const result = await resetBufferFlow(pipelineId.value)
    notice.value = result.message
    receiptView.value = []
    lastRelay.value = null
    lastLedgerId.value = ''
    answerInterrupted.value = false
    dropNextAck.value = false
    await refresh(false)
  })
}

function formatResult(item: BufferReceipt): string {
  const result = item.处理结果 || item.downstream_result
  if (!result) return '—'
  try {
    return JSON.stringify(result, null, 2)
  } catch {
    return String(result)
  }
}

function deliveryCount(ref: string): number {
  return (ledger.value?.downstream ?? []).filter((record) => record.client_ref === ref).length
}

function onWindowFocus(): void {
  void refresh(false)
}

onMounted(async () => {
  try {
    const page = await listEntries('edge_cache', {})
    edgeConfigs.value = page.items
    const active = page.items.find((cfg) => cfg.status === '已生效')
    if (active) edgeConfigId.value = String(active['配置编号'])
  } catch {
    // 边缘配置是可选项；不阻断受理缓冲主流程。
  }
  await refresh(false)
  await ensurePipeline()
  pollTimer = window.setInterval(() => void refreshSilently(), 900)
  window.addEventListener('focus', onWindowFocus)
})

onUnmounted(() => {
  if (pollTimer) window.clearInterval(pollTimer)
  window.removeEventListener('focus', onWindowFocus)
})
</script>

<style scoped>
.buffer-console {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 16px;
  margin: 12px 0 16px;
}
.buffer-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.buffer-head h3 { margin: 0 0 6px; font-size: 18px; }
.buffer-head p { margin: 0; color: var(--muted); font-size: 13px; line-height: 1.6; }
.buffer-alert { border-radius: 8px; padding: 10px 12px; margin: 10px 0; font-size: 13px; line-height: 1.6; }
.buffer-alert.ok { background: #ecfdf3; border: 1px solid #abefc6; color: #067647; }
.buffer-alert.error { background: #fef3f2; border: 1px solid #fecdca; color: #b42318; }
.buffer-alert.interrupted { background: #fffaeb; border: 1px solid #fedf89; color: #b54708; }
.buffer-controls, .submit-grid, .buffer-actions-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: flex-end;
  margin: 12px 0;
}
.buffer-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); min-width: 220px; }
.buffer-field.narrow { min-width: 150px; }
.buffer-field.grow { flex: 1; min-width: 260px; }
.buffer-field.inline { min-width: 240px; margin: 0; }
.buffer-field input, .buffer-field select, .buffer-field textarea {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 10px;
  font-size: 13px;
  color: #1f2937;
  background: #fff;
  font-family: inherit;
}
.buffer-field textarea { width: 100%; resize: vertical; }
.btn.warn { border-color: #f79009; color: #b54708; background: #fffaeb; }
.btn.danger { color: #b42318; border-color: #fecdca; }
.seat-status {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  font-size: 13px;
}
.window-id { color: var(--muted); margin-left: auto; }
.over-capacity { color: #b54708; font-weight: 600; }
.other-capacities { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin: 10px 0; font-size: 12px; }
.other-title { color: var(--muted); }
.capacity-chip { background: #eef4ff; border: 1px solid #b2ccff; color: #1849a9; border-radius: 999px; padding: 3px 9px; }
.batch-box { border-top: 1px dashed var(--border); padding-top: 12px; margin-top: 12px; }
.batch-actions { display: flex; gap: 10px; margin-top: 8px; }
.receipt-ledger, .ledger-panel { margin-top: 14px; }
.ledger-title { margin-top: 18px; }
.section-head { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; }
h4 { margin: 0 0 8px; font-size: 15px; }
.table-scroll { overflow-x: auto; border: 1px solid var(--border); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; background: #fff; }
th, td { border-bottom: 1px solid var(--border); padding: 8px 10px; font-size: 13px; text-align: left; vertical-align: top; }
th:last-child, td:last-child { border-right: 0; }
.boolean { font-weight: 700; }
tr[data-received='true'] .boolean:first-child { color: #067647; }
tr[data-received='false'] .boolean:first-child { color: #b54708; }
.ledger-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; margin-top: 14px; }
.ledger-panel { background: #fcfcfd; border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
.ledger-panel.accepted { border-left: 4px solid #12b76a; }
.ledger-panel.pending { border-left: 4px solid #f79009; }
.ledger-panel.processed { border-left: 4px solid var(--brand); }
.panel-hint { color: var(--muted); font-size: 12px; margin: 0 0 8px; line-height: 1.5; }
.muted { color: var(--muted); font-size: 12px; }
.row-actions.vertical { display: flex; flex-direction: column; gap: 4px; min-width: 150px; }
.badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 12px; margin-left: 6px; }
.badge.idempotent { background: #eef4ff; color: #1849a9; border: 1px solid #b2ccff; }
.badge.retry { background: #fffaeb; color: #b54708; border: 1px solid #fedf89; }
.result-json { margin: 0; max-width: 520px; white-space: pre-wrap; word-break: break-word; font-size: 12px; }
.downstream-refs { display: flex; flex-wrap: wrap; gap: 8px; }
.downstream-chip { background: #ecfdf3; border: 1px solid #abefc6; color: #067647; border-radius: 6px; padding: 5px 8px; font-size: 12px; }
.empty-state { text-align: center; color: var(--muted); padding: 16px; }
@media (max-width: 900px) {
  .window-id { margin-left: 0; width: 100%; }
  .buffer-field, .buffer-field.grow { min-width: 100%; }
}
</style>
