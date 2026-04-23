/**
 * AI 日报生成脚本 — SQLite 主读版 (v6-result-schema)
 *
 * 变更说明 (v6):
 * - 统一 result schema：与 Hermes report result 顶层规范对齐
 * - 字段迁移到 metrics/artifacts/warnings/error/debug
 * - status 字段：ok/warning/no_content/failed
 * - 文件命名：report_<run_id>.result.json
 */

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import initSqlJs from 'sql.js';

const SCRIPT_DIR = path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:\/)/, '$1'));
const ENV_PATH = path.join(SCRIPT_DIR, '.env');
const LOG_DIR = path.join(SCRIPT_DIR, 'logs');

// ══════════════════════════════════════════════════════════════
// 一、配置收口
// ══════════════════════════════════════════════════════════════

let PATHS = {};

const REPORT = {
  TARGET_CATEGORIES: ['🚀技术突破','🔥今日热点','🏢企业动态','🛠️工具/教程','💼商业模式'],
  PASS_RELEVANCE:    ['高','中'],
  SECTION_NAMES:     ['今日热点','技术突破','企业动态','商业模式','工具/教程'],
  SECTION_QUOTAS:   { '今日热点':3,'技术突破':3,'企业动态':3,'商业模式':2,'工具/教程':2 },
  LLM_CAP:          20,
  TIER_PRIORITY:    { '官方':0,'研究':1,'媒体':2,'其他':3,'社区':4,null:99 },
  // v2 规则选刊阈值
  QUALITY_SCORE_MIN: 0.3,
  V2_FIELDS_REQUIRED: ['quality_score','event_key','content_type'],
};

let FEATURES = {
  COVER_IMAGE: false,
  NOTIFY: false,
  BITABLE_SYNC: false,
};

let BITABLE = {
  APP_TOKEN: '',
  TABLE_ID: '',
  FIELD_OC_STATUS: '',
};

const REQUIRED_FIELDS = [
  'id','title','url','platform','published_at','summary_raw',
  'category','ai_relevance','source_tier','fingerprint',
  'hermes_status',
  'openclaw_status','openclaw_selected_at','openclaw_doc_id','openclaw_doc_url',
  'created_at','updated_at',
];
const SCHEMA_VERSION = '1.0';

// ══════════════════════════════════════════════════════════════
// 二、内部状态（不直接暴露，构建 result 时使用）
// ══════════════════════════════════════════════════════════════

const _ = {
  run_id: '',
  started_at: '',
  date: '',
  config: null,
  candidates_before_filter: 0,
  candidates_after_filter: 0,
  candidates_after_quality_gate: 0,
  candidates_after_event_dedup: 0,
  candidates_for_llm: 0,
  stages: {
    sqlite_open_ms:0, sqlite_read_ms:0, filter_ms:0,
    dedupe_ms:0, quality_gate_ms:0, event_dedup_ms:0, quota_ms:0,
    rank_ms:0, llm_ms:0,
    docx_create_ms:0, docx_write_ms:0,
    cover_ms:0, sqlite_wb_ms:0, bitable_ms:0, total_ms:0,
  },
  llm_input_items_count: 0,
  llm_input_chars: 0,
  llm_output_chars: 0,
  llm_retry_count: 0,
  section_target_counts: {},
  section_actual_counts: {},
  llm_success: false,
  docx_created: false,
  sqlite_read_ok: false,
  sqlite_writeback_ok: false,
  bitable_sync_ok: false,
  notify_sent: false,
  user_token_status: 'missing',
  doc_id: '',
  doc_url: '',
  cover_status: 'skipped',
  selected_record_ids: [],
  selected_items_count: 0,
  warnings: [],     // [[code, message, stage], ...]
  error: null,      // {code, message, stage, retryable}
};

function _warn(code, message, stage, severity='non_blocking') {
  _.warnings.push([code, message, stage, severity]);
  console.warn(`[warn] [${code}] ${message} (stage=${stage})`);
}

function _err(code, message, stage, retryable = false) {
  _.error = { code, message, stage, retryable };
}

// ══════════════════════════════════════════════════════════════
// 三、Result 构建（统一 schema）
// ══════════════════════════════════════════════════════════════

function buildResult(t0) {
  const totalMs = Math.round(performance.now() - t0);
  _.stages.total_ms = totalMs;

  // Determine status
  const nonBlockingWarnings = _.warnings.filter(([, , , sev]) => sev !== 'blocking');
  let status;
  if (_.error) {
    status = 'failed';
  } else if (_.candidates_for_llm === 0) {
    status = 'no_content';
  } else if (nonBlockingWarnings.length > 0) {
    status = 'warning';
  } else {
    status = 'ok';
  }

  const warnings = _.warnings.map(([code, message, stage, severity]) => ({ code, message, stage, severity }));

  return {
    // ── 统一顶层 ────────────────────────────────────
    schema_version:   '1.0',
    system:           'openclaw',
    job_type:         'report',
    run_id:           _.run_id,
    success:          _.error === null,
    status,
    started_at:       _.started_at,
    finished_at:      new Date().toISOString(),
    duration_ms:       totalMs,
    timezone:         (_.config?.timezone) || 'Asia/Shanghai',
    source_of_truth:  'sqlite',
    sqlite_path:      PATHS.SQLITE_PATH || null,
    bitable_mode:     FEATURES.BITABLE_SYNC ? 'synced_view' : 'none',
    error:            _.error,
    warnings,
    blocking_error_code:    _.error?.code ?? null,
    non_blocking_warning_codes: _.warnings.filter(([, , , sev]) => sev !== 'blocking').map(([code]) => code),
    metrics: {
      candidates_before_filter:         _.candidates_before_filter,
      candidates_after_filter:          _.candidates_after_filter,
      candidates_after_quality_gate:   _.candidates_after_quality_gate,
      candidates_after_event_dedup:    _.candidates_after_event_dedup,
      candidates_for_llm:             _.candidates_for_llm,
      llm_input_items_count:          _.llm_input_items_count,
      llm_input_chars:                _.llm_input_chars,
      llm_output_chars:               _.llm_output_chars,
      llm_retry_count:                _.llm_retry_count,
      section_target_counts:           _.section_target_counts,
      section_actual_counts:          _.section_actual_counts,
      selected_items_count:      _.selected_items_count,
      llm_success:              _.llm_success,
      docx_created:            _.docx_created,
      sqlite_read_ok:          _.sqlite_read_ok,
      sqlite_writeback_ok:     _.sqlite_writeback_ok,
      bitable_sync_ok:         _.bitable_sync_ok,
      notify_sent:              _.notify_sent,
      user_token_status:        _.user_token_status,
      stage_durations_ms:      { ..._.stages },
    },
    artifacts: {
      doc_id:              _.doc_id || null,
      doc_url:             _.doc_url || null,
      cover_status:        _.cover_status,
      selected_record_ids: _.selected_record_ids,
      result_path:         path.join(LOG_DIR, `report_${_.run_id}.result.json`),
      log_path:            path.join(LOG_DIR, `run-${(_.date||'').replace(/-/g,'')}.log`),
    },
    debug: {
      llm_model:              'MiniMax-M2.7',
      schema_version_expected: SCHEMA_VERSION,
      node_version:           process.version,
      hostname:              (process.env.COMPUTERNAME || process.env.HOSTNAME || 'unknown'),
    },
  };
}

function writeResult(result) {
  const outPath = path.join(LOG_DIR, `report_${result.run_id}.result.json`);
  try {
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2), 'utf8');
    console.log(`[info] result written: ${outPath}`);
  } catch (e) {
    console.error('[warn] failed to write result:', e.message);
  }
}

function finish(ctx, runId, t0) {
  const result = buildResult(t0);
  writeResult(result);
  console.log(`[info] === ${runId} DONE success=${result.success} status=${result.status} duration_ms=${result.duration_ms} ===`);
  process.exitCode = 0;
}

function fatal(code, message, stage, retryable = false) {
  _err(code, message, stage, retryable);
  const result = buildResult(0);
  writeResult(result);
  console.error(`[fatal] [${code}] ${message}`);
  process.exitCode = 1;
}

// ══════════════════════════════════════════════════════════════
// 四、主入口
// ══════════════════════════════════════════════════════════════

main().catch((error) => {
  console.error(`[fatal] ${error.stack || error.message || String(error)}`);
  _err('UNCAUGHT_ERROR', error.stack || error.message, 'unknown', false);
  const result = buildResult(0);
  writeResult(result);
  process.exitCode = 1;
});

async function main() {
  const t0 = performance.now();
  ensureDir(LOG_DIR);
  ensureDir(path.join(os.tmpdir()));
  loadEnvFile(ENV_PATH);
  PATHS = resolvePaths();
  FEATURES = resolveFeatures();
  BITABLE = resolveBitableConfig();
  const config = loadConfig();
  _.config = config;

  // ── run_id & 时序 ──────────────────────────────────
  const runId = `openclaw_${new Date().toISOString().replace(/[-:T]/g,'').replace(/\.\d{3}Z$/,'')}`;
  _.run_id = runId;
  _.started_at = new Date().toISOString();

  const reportDate = resolveReportDate(config.timezone);
  const cutoffDate = endOfPreviousDay(reportDate, config.timezone);
  const reportDateText = formatDateForPrompt(reportDate, config.timezone);
  const cutoffIsoDate = formatDateForPrompt(cutoffDate, config.timezone);
  _.date = reportDateText;

  console.log(`[info] === ${runId} ===`);
  console.log(`[info] report_date=${reportDateText} cutoff=${cutoffIsoDate}`);
  console.log(`[info] schema_version=${SCHEMA_VERSION}`);

  // ── User Token ────────────────────────────────────
  const tokenInfo = getStoredUserTokenInfo();
  _.user_token_status = tokenInfo.status;
  console.log(`[info] user_token_status=${tokenInfo.status}`);

  // ── 打开 SQLite ───────────────────────────────────
  const tSqliteOpen = performance.now();
  let db;
  try {
    if (!PATHS.SQLITE_PATH) {
      fatal('SQLITE_PATH_MISSING', 'OPENCLAW_SQLITE_PATH is not configured', 'sqlite_open', false);
      return;
    }
    db = await openSqlite(PATHS.SQLITE_PATH);
    _.sqlite_read_ok = true;
  } catch (e) {
    fatal('SQLITE_OPEN_FAILED', `SQLite open failed: ${e.message}`, 'sqlite_open', true);
    return;
  }
  _.stages.sqlite_open_ms = Math.round(performance.now() - tSqliteOpen);
  console.log(`[info] sqlite_open_ms=${_.stages.sqlite_open_ms}`);

  // ── Schema 验证 ───────────────────────────────────
  try {
    validateSchema(db);
    console.log(`[info] schema validated OK (version=${SCHEMA_VERSION})`);
  } catch (e) {
    db.close();
    fatal('SCHEMA_VALIDATION_FAILED', `Schema validation failed: ${e.message}`, 'schema_validation', false);
    return;
  }

  // ── 读取候选 ─────────────────────────────────────
  const tRead = performance.now();
  let candidates;
  try {
    candidates = await readCandidatesFromSqlite(db, cutoffDate);
  } catch (e) {
    db.close();
    fatal('SQLITE_READ_FAILED', `SQLite read failed: ${e.message}`, 'sqlite_read', true);
    return;
  }
  _.stages.sqlite_read_ms = Math.round(performance.now() - tRead);
  _.candidates_before_filter = candidates.total;
  _.candidates_after_filter = candidates.filtered;
  console.log(`[info] candidates: before_filter=${candidates.total} filtered=${candidates.filtered}`);
  console.log(`[info] sqlite_read_ms=${_.stages.sqlite_read_ms}`);

  if (candidates.rows.length === 0) {
    db.close();
    finish({ candidates }, runId, t0);
    return;
  }

  // ── 规则选刊（前置 pipeline） ────────────────────
  const finalCandidates = applyRuleSelection(candidates.rows, REPORT.SECTION_NAMES);
  _.candidates_for_llm = finalCandidates.length;
  console.log(`[info] candidates_for_llm=${finalCandidates.length}`);

  if (finalCandidates.length === 0) {
    db.close();
    finish({ candidates }, runId, t0);
    return;
  }

  // ── LLM 输入压缩 ─────────────────────────────────
  const sourcePayload = finalCandidates.map((r, i) => ({
    id: i+1,
    title: r.title,
    platform: r.platform || '',
    published_at: r.published_at,
    publish_date: r.publish_date,
    short_summary: (r.summary||'').substring(0, 150),
    category: r.category_clean,
    quality_score: r.quality_score,
    event_key: r.event_key,
    content_type: r.content_type,
  }));
  _.llm_input_items_count = sourcePayload.length;
  const llmInputJson = JSON.stringify(sourcePayload);
  _.llm_input_chars = llmInputJson.length;

  // ── LLM 生成 ─────────────────────────────────────
  const tLlm = performance.now();
  let report;
  let llmOutputRaw = '';
  let attempts = 1;
  try {
    report = await generateDailyReport(config, reportDate, cutoffIsoDate, sourcePayload);
    llmOutputRaw = JSON.stringify(report);
    _.llm_success = true;
  } catch (e) {
    // 重试一次
    attempts = 2;
    try {
      report = await generateDailyReport(config, reportDate, cutoffIsoDate, sourcePayload);
      llmOutputRaw = JSON.stringify(report);
      _.llm_success = true;
    } catch (e2) {
      db.close();
      fatal('LLM_FAILED', `LLM generation failed: ${e2.message}`, 'llm', true);
      return;
    }
  }
  _.llm_retry_count = attempts - 1;
  _.llm_output_chars = llmOutputRaw.length;
  _.stages.llm_ms = Math.round(performance.now() - tLlm);
  console.log(`[info] llm_ms=${_.stages.llm_ms}`);

  // ── 飞书 Docx ────────────────────────────────────
  const feishu = new FeishuClient(config, tokenInfo.status === 'ok' ? tokenInfo.accessToken : null);
  let docId = '';
  const tDocxCreate = performance.now();
  try {
    const created = await feishu.createDocument(report.title);
    docId = created.documentId;
    _.doc_id = docId;
    _.doc_url = `${trimTrailingSlash(config.feishuBaseUrl)}/docx/${docId}`;
    _.docx_created = true;
  } catch (e) {
    db.close();
    fatal('DOCX_CREATE_FAILED', `Docx creation failed: ${e.message}`, 'docx_create', true);
    return;
  }
  _.stages.docx_create_ms = Math.round(performance.now() - tDocxCreate);
  console.log(`[info] docx_create_ms=${_.stages.docx_create_ms} url=${_.doc_url}`);

  const tDocxWrite = performance.now();
  try {
    const markdown = renderMarkdown(report, reportDateText, cutoffIsoDate);
    await writeMarkdownToDocx(feishu, docId, markdown);
  } catch (e) {
    db.close();
    fatal('DOCX_WRITE_FAILED', `docx write failed: ${e.message}`, 'docx_write', true);
    return;
  }
  _.stages.docx_write_ms = Math.round(performance.now() - tDocxWrite);
  console.log(`[info] docx_write_ms=${_.stages.docx_write_ms}`);

  // ── 封面图 ───────────────────────────────────────
  let coverStatus = 'skipped';
  if (FEATURES.COVER_IMAGE && tokenInfo.status === 'ok') {
    const tCover = performance.now();
    try {
      const coverPath = await generateCoverImage(report, reportDateText, cutoffIsoDate);
      if (coverPath) {
        await feishu.insertImageBlockAtTop(docId, coverPath);
        coverStatus = 'success';
        console.log(`[info] cover inserted`);
      } else {
        coverStatus = 'failed';
      }
    } catch (e) {
      _warn('COVER_FAILED', `cover failed: ${e.message}`, 'cover', 'non_blocking');
      coverStatus = 'failed';
    }
    _.stages.cover_ms = Math.round(performance.now() - tCover);
  }
  _.cover_status = coverStatus;
  console.log(`[info] cover_ms=${_.stages.cover_ms} status=${coverStatus}`);

  // ── 通知 ─────────────────────────────────────────
  if (FEATURES.NOTIFY && tokenInfo.status === 'ok') {
    try {
      await feishu.sendDirectMessage(config.feishuNotifyOpenId, [
        `《${report.title}》已生成`, `日期:${reportDateText}`, `链接:${_.doc_url}`,
      ].join('\n'));
      _.notify_sent = true;
      console.log(`[info] notify sent`);
    } catch (e) {
      _warn('NOTIFY_FAILED', `notify failed: ${e.message}`, 'notify');
    }
  }

  // ── SQLite 写回 ─────────────────────────────────
  const tWb = performance.now();
  const usedIds = finalCandidates.map(r => r.id);
  _.selected_record_ids = usedIds;
  _.selected_items_count = usedIds.length;
  db.close();

  try {
    await writebackSqlitePy(usedIds, _.doc_id, _.doc_url);
    _.sqlite_writeback_ok = true;
    console.log(`[info] sqlite_writeback_ok records=${usedIds.length}`);
  } catch (e) {
    fatal('SQLITE_WRITEBACK_FAILED', `sqlite writeback failed: ${e.message}`, 'sqlite_writeback', true);
    return;
  }
  _.stages.sqlite_wb_ms = Math.round(performance.now() - tWb);
  console.log(`[info] sqlite_wb_ms=${_.stages.sqlite_wb_ms}`);

  // ── 多维表同步 ───────────────────────────────────
  if (FEATURES.BITABLE_SYNC && tokenInfo.status === 'ok') {
    const tBitable = performance.now();
    try {
      await syncToBitable(config, usedIds, tokenInfo);
      _.bitable_sync_ok = true;
      console.log(`[info] bitable_sync_ok`);
    } catch (e) {
      _warn('BITABLE_SYNC_FAILED', `bitable sync failed: ${e.message}`, 'bitable_sync');
    }
    _.stages.bitable_ms = Math.round(performance.now() - tBitable);
    console.log(`[info] bitable_ms=${_.stages.bitable_ms}`);
  }

  // ── 一致性断言 ────────────────────────────────────
  const nonBlockingWarnings = _.warnings.filter(([, , , sev]) => sev !== 'blocking');
  let computedStatus;
  if (_.error) {
    computedStatus = 'failed';
  } else if (_.candidates_for_llm === 0) {
    computedStatus = 'no_content';
  } else if (nonBlockingWarnings.length > 0) {
    computedStatus = 'warning';
  } else {
    computedStatus = 'ok';
  }
  if (_.sqlite_writeback_ok) {
    if (!_.doc_id || !_.doc_url) {
      throw new Error('ASSERTION FAILED: sqlite_writeback_ok=true but doc_id/doc_url is empty');
    }
  }
  if (computedStatus === 'ok') {
    if (!_.docx_created) {
      throw new Error('ASSERTION FAILED: status=ok but docx_created=false');
    }
    if (!_.sqlite_writeback_ok) {
      throw new Error('ASSERTION FAILED: status=ok but sqlite_writeback_ok=false');
    }
    if (_.selected_items_count <= 0) {
      throw new Error('ASSERTION FAILED: status=ok but selected_items_count=0');
    }
  }

  finish({ candidates }, runId, t0);
}

// ══════════════════════════════════════════════════════════════
// 五、Schema 验证
// ══════════════════════════════════════════════════════════════

function validateSchema(db) {
  const result = db.exec("PRAGMA table_info(materials)");
  if (!result.length) throw new Error('materials table not found in SQLite');
  const existingFields = new Set(result[0].values.map(r => r[1]));
  const missing = REQUIRED_FIELDS.filter(f => !existingFields.has(f));
  if (missing.length) {
    throw new Error(
      `Schema version mismatch: missing required fields: [${missing.join(', ')}]. ` +
      `OpenClaw requires schema version >= ${SCHEMA_VERSION}. ` +
      `Please sync with Hermes schema or upgrade OpenClaw.`
    );
  }
}

// ══════════════════════════════════════════════════════════════
// 六、SQLite 读取 & 筛选
// ══════════════════════════════════════════════════════════════

async function openSqlite(winPath) {
  const SQL = await initSqlJs();
  const fileBuffer = fs.readFileSync(winPath);
  return new SQL.Database(fileBuffer);
}

async function readCandidatesFromSqlite(db, cutoffDate) {
  const primaryStart = new Date(cutoffDate);
  primaryStart.setHours(0, 0, 0, 0);
  const primaryEnd = new Date(cutoffDate);
  primaryEnd.setHours(23, 59, 59, 999);
  const primaryStartMs = primaryStart.getTime();
  const primaryEndMs = primaryEnd.getTime();
  console.log(`[info] primary_window=${primaryStart.toISOString()} ~ ${primaryEnd.toISOString()}`);

  const catP = REPORT.TARGET_CATEGORIES.map(() => '?').join(',');
  const relP = REPORT.PASS_RELEVANCE.map(() => '?').join(',');

  // v2: 必须有 ingest_version/v2/quality_score/event_key/content_type
  const tFilter = performance.now();
  const sql = `
    SELECT id, title, url, platform, published_at, summary_raw, category,
           ai_relevance, source_tier, fingerprint, hermes_status,
           quality_score, event_key, content_type, fetched_at
    FROM materials
    WHERE openclaw_status = 'pending'
      AND ai_relevance IN (${relP})
      AND title IS NOT NULL AND title != ''
      AND url IS NOT NULL AND url != ''
      AND published_at IS NOT NULL
      AND category IN (${catP})
      AND ingest_version = 'v2'
      AND quality_score IS NOT NULL
      AND event_key IS NOT NULL
      AND content_type IS NOT NULL
  `;
  const step1 = db.exec(sql, [...REPORT.PASS_RELEVANCE, ...REPORT.TARGET_CATEGORIES]);
  const step1Rows = step1.length > 0 ? step1[0].values : [];
  const filteredRows = step1Rows.filter(r => {
    const ts = r[14];  // fetched_at at index 14
    return ts >= primaryStartMs && ts <= primaryEndMs;
  });
  _.stages.filter_ms = Math.round(performance.now() - tFilter);
  console.log(`[info] step1_base_filter=${step1Rows.length} step2_time_window=${filteredRows.length}`);

  return {
    total: step1Rows.length,
    filtered: filteredRows.length,
    rows: filteredRows,  // raw rows with v2 fields [id,title,url,platform,pub_at,summary_raw,category,ai_rel,source_tier,fp,hermes_status,quality_score,event_key,content_type]
  };
}

// ══════════════════════════════════════════════════════════════
// 六-2、规则选刊（前置于 LLM 的确定性 pipeline）
// ══════════════════════════════════════════════════════════════
// row layout: [id,title,url,platform,pub_at,summary_raw,category,ai_rel,source_tier,fp,hermes_status,quality_score,event_key,content_type]
// indexes:     0   1     2   3        4         5           6        7       8           9   10            11             12          13

function applyRuleSelection(rawRows, sectionNames) {
  // 1) quality_score 阈值过滤
  const tQ = performance.now();
  const afterQuality = rawRows.filter(r => (r[11] ?? 0) >= REPORT.QUALITY_SCORE_MIN);
  _.candidates_after_quality_gate = afterQuality.length;
  _.stages.quality_gate_ms = Math.round(performance.now() - tQ);
  console.log(`[info] quality_gate: ${rawRows.length} -> ${afterQuality.length}`);

  // 2) event_key 去重（同类 event 只保留最高 quality_score 的那条）
  const tE = performance.now();
  const seenEvent = new Map();
  for (const r of afterQuality) {
    const ek = r[12];
    const existing = seenEvent.get(ek);
    if (!existing || (r[11] ?? 0) > (existing[11] ?? 0)) {
      seenEvent.set(ek, r);
    }
  }
  const afterEventDedup = [...seenEvent.values()];
  _.candidates_after_event_dedup = afterEventDedup.length;
  _.stages.event_dedup_ms = Math.round(performance.now() - tE);
  console.log(`[info] event_dedup: ${afterQuality.length} -> ${afterEventDedup.length}`);

  // 3) source_tier 排序
  const tR = performance.now();
  afterEventDedup.sort((a, b) => (REPORT.TIER_PRIORITY[a[8]] ?? 99) - (REPORT.TIER_PRIORITY[b[8]] ?? 99));
  _.stages.rank_ms = Math.round(performance.now() - tR);

  // 4) 分类配额分配
  const tQ2 = performance.now();
  const sectionQuotas = { ...REPORT.SECTION_QUOTAS };
  _.section_target_counts = { ...sectionQuotas };

  const bySection = {};
  for (const name of sectionNames) bySection[name] = [];

  for (const r of afterEventDedup) {
    // 保留斜杠 '/' 分词：'🛠️工具/教程' -> '工具/教程' -> ['工具','教程']
    const catClean = (r[6]||'').replace(/[^\u4e00-\u9fa5/]/g,'').trim();
    // 按 '/' 分词，只要任意分词词段与 sectionNames 匹配即归类
    const segments = catClean.split('/');
    const matched = sectionNames.find(n => segments.some(seg => seg === n || n.includes(seg) || seg.includes(n))) || null;
    const section = matched || '其他';
    if (!bySection[section]) bySection[section] = [];
    bySection[section].push(r);
  }

  const allocated = [];
  const sectionActualCounts = {};
  for (const name of sectionNames) {
    const quota = sectionQuotas[name] ?? 0;
    const pool = bySection[name] || [];
    const picked = pool.slice(0, quota);
    for (const r of picked) allocated.push(r);
    sectionActualCounts[name] = picked.length;
  }

  // 如果配额未填满，回退到按 tier 补录
  if (allocated.length < REPORT.LLM_CAP) {
    const allocatedIds = new Set(allocated.map(r => r[0]));
    const remaining = afterEventDedup.filter(r => !allocatedIds.has(r[0]));
    const needed = REPORT.LLM_CAP - allocated.length;
    for (const r of remaining.slice(0, needed)) allocated.push(r);
  }

  // 最终截断
  const finalCandidates = allocated.slice(0, REPORT.LLM_CAP);
  _.stages.quota_ms = Math.round(performance.now() - tQ2);

  // 实际每 section 条数
  const actualCounts = {};
  for (const name of sectionNames) actualCounts[name] = 0;
  for (const r of finalCandidates) {
    const catClean = (r[6]||'').replace(/[^\u4e00-\u9fa5/]/g,'').trim();
    const segments = catClean.split('/');
    const matched = sectionNames.find(n => segments.some(seg => seg === n || n.includes(seg) || seg.includes(n))) || '其他';
    if (actualCounts[matched] !== undefined) actualCounts[matched]++;
  }
  _.section_actual_counts = actualCounts;

  // 5) 映射到 LLM 输入格式
  const mapped = finalCandidates.map(r => {
    const pubDate = r[4] ? new Date(r[4]) : null;
    return {
      id: r[0], title: r[1]||'', url: r[2]||'', platform: r[3]||'',
      published_at: r[4],
      publish_date: pubDate ? formatDateForPrompt(pubDate, 'Asia/Shanghai') : '',
      summary: r[5]||'', category: r[6]||'',
      category_clean: (r[6]||'').replace(/[^\u4e00-\u9fa5/]/g,'').trim(),
      ai_relevance: r[7]||'', source_tier: r[8]||'',
      fingerprint: r[9]||'', hermes_status: r[10]||'',
      quality_score: r[11] ?? null,
      event_key: r[12] || '',
      content_type: r[13] || '',
    };
  });

  return mapped;
}

// ══════════════════════════════════════════════════════════════
// 七、Python 持久化写回
// ══════════════════════════════════════════════════════════════

async function writebackSqlitePy(recordIds, docId, docUrl) {
  if (!recordIds.length) return;
  const { stdout, stderr } = await new Promise((resolve, reject) => {
    const proc = spawn(PATHS.PYTHON_BIN, [PATHS.PY_WRITE_BACK, JSON.stringify(recordIds), docId, docUrl]);
    let out = '', err = '';
    proc.stdout.on('data', d => (out += d.toString()));
    proc.stderr.on('data', d => (err += d.toString()));
    proc.on('error', reject);
    proc.on('close', code => code === 0 ? resolve({ stdout: out, stderr: err }) : reject(new Error(`python exited ${code}: ${err}`)));
  });
  if (stdout.trim()) console.log(`[info] python_writeback: ${stdout.trim().split('\n').slice(-2).join(' ')}`);
}

// ══════════════════════════════════════════════════════════════
// 八、多维表同步
// ══════════════════════════════════════════════════════════════

async function syncToBitable(config, recordIds, tokenInfo) {
  if (!recordIds.length || tokenInfo.status !== 'ok') return;
  if (!BITABLE.APP_TOKEN || !BITABLE.TABLE_ID || !BITABLE.FIELD_OC_STATUS) {
    throw new Error('bitable sync is enabled but FEISHU_BITABLE_* is not fully configured');
  }
  await updateBitableStatus(config, recordIds, '已入日报', tokenInfo.accessToken);
}

async function updateBitableStatus(config, recordIds, status, token) {
  if (!recordIds.length) return;
  const listData = await bitableRequest(config, 'GET',
    `/open-apis/bitable/v1/apps/${BITABLE.APP_TOKEN}/tables/${BITABLE.TABLE_ID}/fields`, null, token);
  const ocField = (listData?.items || []).find(f => f.field_name === 'OpenClaw状态');
  if (!ocField) { _warn('BITABLE_FIELD_MISSING', 'OpenClaw状态 field not found', 'bitable_sync'); return; }
  const statusOption = (ocField?.property?.options || []).find(o => o.name === status);
  if (!statusOption) { _warn('BITABLE_OPTION_MISSING', `Option "${status}" not found`, 'bitable_sync'); return; }
  for (const chunk of chunkArray(recordIds, 10)) {
    const records = chunk.map(rid => ({ record_id: rid, fields: { [BITABLE.FIELD_OC_STATUS]: statusOption.id } }));
    await bitableRequest(config, 'PUT',
      `/open-apis/bitable/v1/apps/${BITABLE.APP_TOKEN}/tables/${BITABLE.TABLE_ID}/records`,
      { records }, token);
  }
}

async function bitableRequest(config, method, pathStr, body, tokenOverride) {
  const token = tokenOverride || getStoredUserTokenInfo().accessToken;
  if (!token) throw new Error('No user token');
  const url = `https://open.feishu.cn${pathStr}`;
  const opts = { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json; charset=utf-8' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  const json = await res.json().catch(() => null);
  if (!res.ok || json?.code !== 0) throw new Error(`Bitable API ${method} ${pathStr}: ${JSON.stringify(json)}`);
  return json.data;
}

// ══════════════════════════════════════════════════════════════
// 九、LLM 生成
// ══════════════════════════════════════════════════════════════

async function generateDailyReport(config, reportDate, cutoffIsoDate, sourceItems) {
  const systemPrompt = [
    '你是 AI 行业研究分析师，负责撰写行业研究简报风格的中文 AI 日报。',
    '只能基于我提供的素材写作，严禁编造事实。所有正文必须是中文。',
    '栏目固定为：今日热点、技术突破、企业动态、商业模式、工具/教程（不带emoji）。',
    '每条内容都必须使用"摘要"和"分析"两个字段。',
    '商业模式栏目请从投资视角切入，分析要点包括：市场规模与增速、收入模式、竞争壁垒、估值逻辑等核心维度。',
    '摘要控制在120-180字，分析不做硬性字数限制但须条理清晰、有投资判断力。',
    '其余栏目保持研究简报风格，每条摘要控制在80-120字。',
    `日报日期为 ${formatDateForPrompt(reportDate, config.timezone)}，素材时间最多到 ${cutoffIsoDate}。`,
    '请优先选择最有行业代表性的内容，避免重复，控制每个栏目 1 到 3 条。',
    '返回严格 JSON，不要输出 Markdown，不要输出解释。',
  ].join('\n');

  const userPrompt = {
    report_date: formatDateForPrompt(reportDate, config.timezone),
    cutoff_date: cutoffIsoDate,
    required_schema: {
      title: 'CaptainLabs AI 日报',
      sections: REPORT.SECTION_NAMES.map(name => ({
        name,
        items: [{ title:'string', summary:'string', analysis:'string', source_name:'string', source_url:'string', source_date:'YYYY-MM-DD' }],
      })),
    },
    source_items: sourceItems,
  };

  const body = {
    model: 'MiniMax-M2.7', max_tokens: 8192, temperature: 0.3,
    system: systemPrompt,
    messages: [{ role: 'user', content: JSON.stringify(userPrompt) }],
  };

  let lastError;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const response = await fetch(`${trimTrailingSlash(config.minimaxBaseUrl)}/anthropic/v1/messages`, {
        method: 'POST',
        headers: { 'Content-Type':'application/json','x-api-key': config.minimaxApiKey,'anthropic-version':'2023-06-01' },
        body: JSON.stringify(body),
      });
      const json = await response.json().catch(() => null);
      if (!response.ok) throw new Error(`MiniMax HTTP ${response.status}: ${JSON.stringify(json)}`);
      const text = extractAssistantText(json);
      const parsed = typeof text === 'object' ? text : parseJsonBlock(text);
      const { valid, errors } = validateReportSchema(parsed);
      if (!valid) {
        const errMsg = `Schema 校验失败: ${errors.join('; ')}`;
        console.warn(`[warn] validate attempt ${attempt+1}: ${errMsg}`);
        if (attempt === 0) { body.temperature = 0.1; lastError = new Error(errMsg); continue; }
        throw new Error(errMsg);
      }
      return normalizeReport(parsed, reportDate, cutoffIsoDate);
    } catch (e) {
      lastError = e;
      console.warn(`[warn] generate attempt ${attempt+1} failed: ${e.message}`);
      if (attempt === 0) body.temperature = 0.1;
    }
  }
  throw lastError;
}

function validateReportSchema(raw) {
  const errors = [];
  if (!raw || typeof raw !== 'object') { errors.push('报告 JSON 解析失败或为空'); return { valid: false, errors }; }
  if (!raw.title || typeof raw.title !== 'string') errors.push('缺少 title');
  const requiredSections = ['今日热点','技术突破','企业动态','商业模式','工具'];
  const sectionNames = (raw.sections || []).map(s => s.name || '');
  for (const req of requiredSections) {
    if (!sectionNames.some(n => n.includes(req) || req.includes(n))) errors.push(`缺少栏目: ${req}`);
  }
  for (const section of (raw.sections || [])) {
    for (const item of (section.items || [])) {
      if (!item.title) errors.push('资讯缺少 title');
      if (!item.summary) errors.push('资讯缺少 summary');
      if (!item.analysis) errors.push('资讯缺少 analysis');
    }
  }
  return { valid: errors.length === 0, errors };
}

function normalizeReport(raw, reportDate, cutoffIsoDate) {
  const sectionMap = new Map((raw.sections || []).map(s => [s.name?.replace(/[^\u4e00-\u9fa5]/g,'').trim(), s]));
  return {
    title: (raw.title || 'CaptainLabs AI 日报').trim(),
    sections: REPORT.SECTION_NAMES.map(name => {
      let matched = sectionMap.get(name);
      if (!matched) {
        for (const [k, v] of sectionMap) {
          if (k.includes(name) || name.includes(k)) { matched = v; break; }
        }
      }
      const items = Array.isArray(matched?.items) ? matched.items : [];
      return {
        name: `【${name}】`,
        items: items
          .map(item => ({
            title: (item.title||'').trim(),
            summary: (item.summary||'').trim(),
            analysis: (item.analysis||'').trim(),
            source_name: (item.source_name||item.source_platform||'').trim(),
            source_url: (item.source_url||'').trim(),
            source_date: (item.source_date||cutoffIsoDate).trim(),
          }))
          .filter(item => item.title && item.summary && item.analysis)
          .slice(0, 3),
      };
    }),
  };
}

function renderMarkdown(report, reportDateText, cutoffIsoDate) {
  const lines = [`# ${report.title}`,'',`更新时间:${reportDateText}`,`信源截止:${cutoffIsoDate}`,''];
  for (const section of report.sections) {
    lines.push(`## ${section.name}`,'');
    if (!section.items.length) { lines.push('暂无符合条件的条目。',''); continue; }
    for (const item of section.items) {
      lines.push(`### ${item.title}`,'');
      lines.push(`摘要:${item.summary}`,'');
      lines.push(`分析:${item.analysis}`,'');
      lines.push(`来源:${item.source_name||''} | ${item.source_date} | ${item.source_url}`,'');
      lines.push('');
    }
  }
  return lines.join('\n');
}

async function writeMarkdownToDocx(feishu, documentId, markdown) {
  const blocks = [];
  for (const paragraph of markdown.split(/\r?\n\r?\n/).map(p => p.trim()).filter(Boolean)) {
    if (paragraph.startsWith('# ')) continue;
    if (paragraph.startsWith('## ')) { blocks.push(textBlock(`【${paragraph.slice(3)}】`, true)); continue; }
    if (paragraph.startsWith('### ')) { blocks.push(textBlock(paragraph.slice(4), true)); continue; }
    blocks.push(textBlock(paragraph, false));
  }
  for (const chunk of chunkArray(blocks, 20)) {
    await feishu.appendBlocks(documentId, documentId, chunk);
  }
}

function textBlock(content, bold) {
  return { block_type: 2, text: { elements: [{ text_run: { content, text_element_style: bold ? { bold: true } : undefined } }] } };
}

// ══════════════════════════════════════════════════════════════
// 十、FeishuClient
// ══════════════════════════════════════════════════════════════

class FeishuClient {
  constructor(config, userToken = null) { this.config = config; this.userToken = userToken; }

  async createDocument(title) {
    const data = await this.request('/open-apis/docx/v1/documents', { method:'POST', body:{title} });
    const documentId = data?.document?.document_id || data?.document_id;
    if (!documentId) throw new Error(`Unexpected create document response: ${JSON.stringify(data)}`);
    return { documentId };
  }

  async appendBlocks(documentId, blockId, children) {
    return this.request(`/open-apis/docx/v1/documents/${documentId}/blocks/${blockId}/children`, { method:'POST', body:{children} });
  }

  async insertImageBlockAtTop(documentId, imgPath) {
    const blockResp = await this.request(`/open-apis/docx/v1/documents/${documentId}/blocks/${documentId}/children`, {
      method:'POST', body:{ children:[{ block_type:27, image:{} }], index:0 },
    });
    if (!blockResp?.children?.[0]?.block_id) throw new Error('Failed to create image block');
    const blockId = blockResp.children[0].block_id;
    const buf = fs.readFileSync(imgPath);
    const boundary = '----CoverImg' + Math.random().toString(36).slice(2);
    const parts = [
      `--${boundary}\r\nContent-Disposition: form-data; name="file_name"\r\n\r\ncover.png`,
      `--${boundary}\r\nContent-Disposition: form-data; name="parent_type"\r\n\r\ndocx_image`,
      `--${boundary}\r\nContent-Disposition: form-data; name="parent_node"\r\n\r\n${blockId}`,
      `--${boundary}\r\nContent-Disposition: form-data; name="size"\r\n\r\n${buf.length}`,
      `--${boundary}\r\nContent-Disposition: form-data; name="extra"\r\n\r\n${JSON.stringify({ drive_route_token: documentId })}`,
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="cover.png"\r\nContent-Type: image/png\r\n\r\n`,
    ];
    const body = Buffer.concat([Buffer.from(parts.join('\r\n')), buf, Buffer.from(`\r\n--${boundary}--`)]);
    const uploadResp = await this.uploadMultipart('/open-apis/drive/v1/medias/upload_all', body, boundary);
    const fileToken = uploadResp?.file_token || uploadResp?.data?.file_token;
    if (!fileToken) throw new Error('Image upload failed: ' + JSON.stringify(uploadResp));
    await this.request(`/open-apis/docx/v1/documents/${documentId}/blocks/${blockId}`, {
      method:'PATCH', body:{ replace_image:{ token:fileToken, width:1080, height:2420, align:2 } },
    });
  }

  async uploadMultipart(pathname, body, boundary) {
    const response = await fetch(`https://open.feishu.cn${pathname}`, {
      method:'POST',
      headers:{ Authorization:`Bearer ${this.userToken}`, 'Content-Type':`multipart/form-data; boundary=${boundary}`, 'Content-Length':body.length },
      body,
    });
    return response.json().catch(() => null);
  }

  async ensureNotifyChat(openId) {
    const existing = await this.findChatByOpenId(openId);
    if (existing) return existing;
    const data = await this.request('/open-apis/im/v1/chats?user_id_type=open_id', {
      method:'POST', body:{ name:this.config.notifyChatName, chat_type:'group', user_id_list:[openId] },
    });
    const chatId = data?.chat_id;
    if (!chatId) throw new Error(`Unexpected chat create response: ${JSON.stringify(data)}`);
    return chatId;
  }

  async findChatByOpenId(openId) {
    let pageToken = '';
    do {
      const query = pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : '';
      let data;
      try { data = await this.request(`/open-apis/im/v1/chats?page_size=100${query}`, { method:'GET' }); }
      catch { return null; }
      const items = Array.isArray(data?.items) ? data.items : [];
      for (const item of items) {
        try {
          const members = await this.request(`/open-apis/im/v1/chats/${item.chat_id}/members?page_size=100&member_id_type=open_id`, { method:'GET' });
          if ((members?.items||[]).some(m => m.member_id === openId)) return item.chat_id;
        } catch { continue; }
      }
      pageToken = data?.page_token || '';
      if (!data?.has_more) break;
    } while (pageToken);
    return null;
  }

  async sendChatTextMessage(chatId, text) {
    return this.request('/open-apis/im/v1/messages?receive_id_type=chat_id', {
      method:'POST', body:{ receive_id:chatId, msg_type:'text', content:JSON.stringify({text}) },
    });
  }

  async sendDirectMessage(openId, text) {
    return this.request('/open-apis/im/v1/messages?receive_id_type=open_id', {
      method:'POST', body:{ receive_id:openId, msg_type:'text', content:JSON.stringify({text}) },
    });
  }

  async request(pathname, { method, body }) {
    const token = this.userToken || await this._getUserToken();
    const response = await fetch(`https://open.feishu.cn${pathname}`, {
      method, headers:{ Authorization:`Bearer ${token}`,'Content-Type':'application/json; charset=utf-8' },
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok || payload?.code !== 0) throw new Error(`Feishu API failed: ${method} ${pathname} ${JSON.stringify(payload)}`);
    return payload.data;
  }

  async _getUserToken() {
    if (this.userToken) return this.userToken;
    const info = getStoredUserTokenInfo();
    if (info.status !== 'ok') throw new Error(`User token unavailable: ${info.status}`);
    this.userToken = info.accessToken;
    return this.userToken;
  }
}

// ══════════════════════════════════════════════════════════════
// 十一、Token 健康检查
// ══════════════════════════════════════════════════════════════

function getStoredUserTokenInfo() {
  try {
    const encPath = path.join(os.homedir(), 'AppData', 'Local', PATHS.TOKEN_STORE_DIR);
    const keyFile = path.join(encPath, 'master.key');
    if (!fs.existsSync(keyFile)) return { status:'missing', accessToken:null, expiresAt:null, reason:'key file not found' };
    const masterKey = fs.readFileSync(keyFile);
    const files = fs.readdirSync(encPath).filter(f => f.endsWith('.enc')).sort();
    let best = null;
    for (const fn of files) {
      try {
        const enc = fs.readFileSync(path.join(encPath, fn));
        const decipher = crypto.createDecipheriv('aes-256-gcm', masterKey, enc.slice(0,12));
        decipher.setAuthTag(enc.slice(12,28));
        const data = JSON.parse(Buffer.concat([decipher.update(enc.slice(28)), decipher.final()]).toString('utf8'));
        if (!best || data.expiresAt > best.expiresAt) best = data;
      } catch (e) {}
    }
    if (!best) return { status:'missing', accessToken:null, expiresAt:null, reason:'no token in store' };
    if (best.expiresAt < Date.now()) return { status:'expired', accessToken:null, expiresAt:best.expiresAt, reason:'token expired' };
    return { status:'ok', accessToken:best.accessToken, expiresAt:best.expiresAt, reason:null };
  } catch (e) {
    return { status:'invalid', accessToken:null, expiresAt:null, reason:e.message };
  }
}

// ══════════════════════════════════════════════════════════════
// 十二、封面图
// ══════════════════════════════════════════════════════════════

async function generateCoverImage(report, dateText, cutoffIsoDate) {
  if (!PATHS.COVER_SCRIPT) return null;
  if (!fs.existsSync(PATHS.COVER_SCRIPT)) { console.warn('[warn] Cover script not found:', PATHS.COVER_SCRIPT); return null; }
  const coverData = {
    report_date: dateText, cutoff_date: cutoffIsoDate,
    sections: report.sections.map(sec => ({
      name: sec.name.replace(/^【|】$/g,''),
      items: sec.items.slice(0,3).map(item => ({ title:item.title||'', summary:(item.summary||'').replace(/\n/g,' '), source_name:item.source_name||'' })),
    })),
  };
  const outPath = path.join(os.tmpdir(), 'ai_daily_newsletter.png');
  const jsonPath = path.join(os.tmpdir(), 'cover_data.json');
  try {
    fs.writeFileSync(jsonPath, JSON.stringify(coverData), 'utf8');
    const proc = spawn('python', [PATHS.COVER_SCRIPT,'--json','@'+jsonPath,'--output',outPath], { stdio:['ignore','pipe','pipe'] });
    let err = '';
    proc.stderr.on('data', d => (err += d.toString()));
    const code = await new Promise(res => { proc.on('close', c => res(c)); proc.on('error', () => res(1)); });
    try { fs.unlinkSync(jsonPath); } catch (_) {}
    if (code === 0 && fs.existsSync(outPath)) { console.log('[info] Cover generated:', outPath); return outPath; }
    console.warn('[warn] Cover failed (exit=' + code + '):', err.substring(0,200)); return null;
  } catch (e) {
    console.warn('[warn] Cover error:', e.message);
    try { fs.unlinkSync(jsonPath); } catch (_) {}
    return null;
  }
}

// ══════════════════════════════════════════════════════════════
// 十三、工具函数
// ══════════════════════════════════════════════════════════════

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
  return chunks;
}

function ensureDir(dirPath) { fs.mkdirSync(dirPath, { recursive: true }); }

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const line of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const sep = t.indexOf('=');
    if (sep <= 0) continue;
    const k = t.slice(0,sep).trim(), v = t.slice(sep+1).trim();
    if (!(k in process.env)) process.env[k] = v;
  }
}

function resolvePaths() {
  return {
    SQLITE_PATH:     process.env.OPENCLAW_SQLITE_PATH || '',
    PY_WRITE_BACK:   process.env.OPENCLAW_WRITEBACK_SCRIPT || path.join(SCRIPT_DIR, 'writeback_sqlite.py'),
    COVER_SCRIPT:    process.env.OPENCLAW_COVER_SCRIPT || '',
    TOKEN_STORE_DIR: process.env.OPENCLAW_TOKEN_STORE_DIR || 'openclaw-feishu-uat',
    PYTHON_BIN:      process.env.OPENCLAW_PYTHON_BIN || 'python',
  };
}

function resolveFeatures() {
  return {
    COVER_IMAGE: (process.env.FEATURES_COVER_IMAGE || '').toLowerCase() === 'true',
    NOTIFY:      (process.env.FEATURES_NOTIFY || '').toLowerCase() === 'true',
    BITABLE_SYNC:(process.env.FEATURES_BITABLE_SYNC || '').toLowerCase() === 'true',
  };
}

function resolveBitableConfig() {
  return {
    APP_TOKEN: process.env.FEISHU_BITABLE_APP_TOKEN || '',
    TABLE_ID: process.env.FEISHU_BITABLE_TABLE_ID || '',
    FIELD_OC_STATUS: process.env.FEISHU_BITABLE_FIELD_OC_STATUS || '',
  };
}

function loadConfig() {
  const openClawHome = path.join(os.homedir(), '.openclaw');
  return {
    feishuAppId:        process.env.FEISHU_APP_ID || '',
    feishuAppSecret:    process.env.FEISHU_APP_SECRET || '',
    feishuBaseUrl:      process.env.FEISHU_BASE_URL || 'https://your-tenant.feishu.cn',
    feishuNotifyOpenId: process.env.FEISHU_NOTIFY_OPEN_ID || '',
    timezone:           process.env.REPORT_TIMEZONE || 'Asia/Shanghai',
    minimaxApiKey:     process.env.MINIMAX_API_KEY || readMiniMaxApiKey(openClawHome),
    minimaxBaseUrl:    process.env.MINIMAX_BASE_URL || 'https://api.minimaxi.com',
    openClawHome,
  };
}

function readMiniMaxApiKey(openClawHome) {
  const authProfilesPath = path.join(openClawHome, 'agents','main','agent','auth-profiles.json');
  const raw = JSON.parse(fs.readFileSync(authProfilesPath, 'utf8'));
  const key = raw?.profiles?.['minimax:cn']?.key;
  if (!key) throw new Error(`MiniMax API key not found in ${authProfilesPath}`);
  return key;
}

function resolveReportDate(timezone) {
  const cliDate = process.argv.find(a => a.startsWith('--report-date='))?.split('=')[1];
  const raw = process.env.REPORT_DATE || cliDate;
  if (raw) return new Date(`${raw}T09:00:00+08:00`);
  const now = new Date();
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: timezone, year:'numeric', month:'2-digit', day:'2-digit' }).formatToParts(now);
  const y = parts.find(p => p.type==='year')?.value;
  const mo = parts.find(p => p.type==='month')?.value;
  const d = parts.find(p => p.type==='day')?.value;
  return new Date(`${y}-${mo}-${d}T09:00:00+08:00`);
}

function endOfPreviousDay(reportDate, timezone) {
  const dateText = formatDateForPrompt(reportDate, timezone);
  return new Date(new Date(`${dateText}T00:00:00+08:00`).getTime() - 1);
}

function formatDateForPrompt(date, timezone) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: timezone, year:'numeric', month:'2-digit', day:'2-digit' }).format(date);
}

function trimTrailingSlash(v) { return v.replace(/\/+$/, ''); }

function extractAssistantText(payload) {
  const textBlocks = (payload?.content || []).filter(p => p?.type === 'text');
  if (!textBlocks.length) return '';
  const raw = textBlocks.map(p => p?.text || '').join('\n').trim();
  if (raw.startsWith('{')) {
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed === 'string' && parsed.startsWith('{')) return JSON.parse(parsed);
      return parsed;
    } catch {}
  }
  return sanitizeForJson(raw);
}

function sanitizeForJson(text) {
  const cleaned = text.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '').replace(/\\\\([\s\S])/g, '\\$1');
  try { return JSON.parse(cleaned); } catch {}
  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const slice = cleaned.slice(firstBrace, lastBrace + 1).replace(/,(\s*[\]\}])/g, '$1');
    try { return JSON.parse(slice); } catch {}
  }
  return cleaned;
}

function parseJsonBlock(text) {
  const fenced = text.match(/```json\s*([\s\S]*?)```/i);
  let candidate = fenced ? fenced[1].trim() : text;
  const firstBrace = candidate.indexOf('{');
  const lastBrace = candidate.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) candidate = candidate.slice(firstBrace, lastBrace + 1);
  candidate = candidate.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
  candidate = candidate.replace(/'([^']*)'/g, (m, g) => '"' + g.replace(/"/g, '\\"') + '"');
  candidate = candidate.replace(/,(\s*[\]\}])/g, '$1');
  try { return JSON.parse(candidate); }
  catch (e) { throw new Error('JSON parse failed: ' + e.message + ' | sample: ' + candidate.slice(0,150)); }
}
