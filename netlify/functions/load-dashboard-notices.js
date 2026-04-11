const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const partnerSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "partner" }
  }
);

const alignmentSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "alignment" }
  }
);

const VALID_SECTIONS = ["base", "early", "middle", "late"];
const PARTNER_ACTIVE_WINDOW_SECONDS = 300;
const LOW_QI_WARNING_PERCENT = 20;
const MAX_NOTICES = 10;

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
}

function parseCookies(cookieHeader = "") {
  const cookies = {};

  cookieHeader.split(";").forEach((part) => {
    const trimmed = part.trim();
    if (!trimmed) return;

    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) return;

    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();

    cookies[key] = decodeURIComponent(value);
  });

  return cookies;
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeLower(value, fallback = "") {
  return safeText(value, fallback).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function safeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;

  if (typeof value === "number") {
    return value !== 0;
  }

  const text = safeLower(value);

  if (["true", "1", "yes", "y", "on", "active", "started"].includes(text)) {
    return true;
  }

  if (["false", "0", "no", "n", "off", "inactive", "stopped"].includes(text)) {
    return false;
  }

  return fallback;
}

function toTitle(value, fallback = "Base") {
  const text = safeLower(value);
  if (!text) return fallback;
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function titleize(value, fallback = "Unknown") {
  const text = safeText(value);
  if (!text) return fallback;

  return text
    .replace(/_/g, " ")
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function sameValue(a, b) {
  return safeLower(a) === safeLower(b);
}

function pickFirst(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      return value;
    }
  }
  return "";
}

function uniqueTextList(values = []) {
  return [...new Set((values || []).map((value) => safeText(value)).filter(Boolean))];
}

function uniquePayloadList(values = []) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    const payload = value && typeof value === "object" ? value : null;
    if (!payload) continue;

    const sortedPayload = {};
    for (const key of Object.keys(payload).sort()) {
      sortedPayload[key] = payload[key];
    }

    const cacheKey = JSON.stringify(sortedPayload);
    if (!seen.has(cacheKey)) {
      seen.add(cacheKey);
      result.push(payload);
    }
  }

  return result;
}

function roundNumber(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Number(number.toFixed(digits));
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-GB").format(safeNumber(value, 0));
}

function formatPercent(value) {
  return `${Math.round(safeNumber(value, 0))}%`;
}

function formatMultiplier(value) {
  const n = safeNumber(value, 1);
  const rounded = Number(n.toFixed(2));
  return `x${rounded}`;
}

function isProgressedStatus(status) {
  const normalized = safeLower(status);

  return [
    "opened",
    "under_comprehension",
    "comprehended",
    "completed",
    "ready_to_complete",
    "in_progress"
  ].includes(normalized);
}

function hasAnySectionProgress(row) {
  return VALID_SECTIONS.some((section) =>
    isProgressedStatus(row?.[`${section}_status`])
  );
}

function hasActiveSection(row) {
  return VALID_SECTIONS.some((section) => {
    const status = safeLower(row?.[`${section}_status`]);
    return (
      status === "under_comprehension" ||
      status === "in_progress" ||
      status === "ready_to_complete"
    );
  });
}

function hasKnownCurrentSection(row) {
  const currentSection = safeLower(row?.current_section);
  return VALID_SECTIONS.includes(currentSection);
}

function pickRelevantRealmRow(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;

  const activeRow = rows.find((row) => hasActiveSection(row));
  if (activeRow) return activeRow;

  const currentSectionRow = rows.find((row) => hasKnownCurrentSection(row));
  if (currentSectionRow) return currentSectionRow;

  const progressedRow = rows.find((row) => hasAnySectionProgress(row));
  if (progressedRow) return progressedRow;

  return rows[0];
}

function isPartnerActive(lastPresenceAt) {
  if (!lastPresenceAt) return false;

  const presenceMs = new Date(lastPresenceAt).getTime();
  if (!Number.isFinite(presenceMs)) return false;

  const ageSeconds = (Date.now() - presenceMs) / 1000;
  return ageSeconds >= 0 && ageSeconds <= PARTNER_ACTIVE_WINDOW_SECONDS;
}

function getPartnerIdentityFromPartnership(partnership, currentAvatarKey) {
  if (!partnership) {
    return {
      avatarKey: "",
      username: ""
    };
  }

  if (sameValue(partnership.requester_avatar_key, currentAvatarKey)) {
    return {
      avatarKey: safeText(partnership.recipient_avatar_key),
      username: safeText(partnership.recipient_username)
    };
  }

  return {
    avatarKey: safeText(partnership.requester_avatar_key),
    username: safeText(partnership.requester_username)
  };
}

function getMemberPrimaryId(memberRow) {
  return safeText(
    pickFirst(memberRow?.member_id, memberRow?.id),
    ""
  );
}

function getMemberIdentifiers(memberRow) {
  return uniqueTextList([
    memberRow?.member_id,
    memberRow?.id,
    memberRow?.sl_avatar_key,
    memberRow?.sl_username
  ]);
}

function matchBondMemberRowToMember(row, memberRow) {
  if (!row || !memberRow) return false;

  const rowIdentifiers = uniqueTextList([
    row?.member_id,
    row?.cultivation_member_id,
    row?.member_uuid,
    row?.sl_avatar_key,
    row?.sl_username
  ]);

  const memberIdentifiers = getMemberIdentifiers(memberRow);

  if (!rowIdentifiers.length || !memberIdentifiers.length) return false;

  return rowIdentifiers.some((rowValue) =>
    memberIdentifiers.some((memberValue) => sameValue(rowValue, memberValue))
  );
}

function sortBondBookRows(rows = []) {
  return [...rows].sort((a, b) => {
    const aVolume = safeNumber(
      pickFirst(a?.bond_volume_number, a?.volume_number, a?.volume_index),
      0
    );
    const bVolume = safeNumber(
      pickFirst(b?.bond_volume_number, b?.volume_number, b?.volume_index),
      0
    );

    if (aVolume !== bVolume) return aVolume - bVolume;

    const aBook = safeNumber(
      pickFirst(a?.bond_book_number, a?.book_number, a?.book_index),
      0
    );
    const bBook = safeNumber(
      pickFirst(b?.bond_book_number, b?.book_number, b?.book_index),
      0
    );

    if (aBook !== bBook) return aBook - bBook;

    const aTime = new Date(a?.updated_at || a?.completed_at || 0).getTime();
    const bTime = new Date(b?.updated_at || b?.completed_at || 0).getTime();

    return bTime - aTime;
  });
}

function isBondRowClosed(row) {
  const displayState = safeLower(row?.display_state);
  const status = safeLower(row?.status);

  if (displayState === "pair_completed") return true;
  if (displayState === "completed") return true;
  if (status === "completed" && displayState !== "ready_for_completion") return true;

  return false;
}

function isBondRowLocked(row) {
  const displayState = safeLower(row?.display_state);
  const status = safeLower(row?.status);

  return displayState === "locked" || status === "locked";
}

function pickFocusBondBookRow(rows = []) {
  if (!Array.isArray(rows) || !rows.length) return null;

  const sorted = sortBondBookRows(rows);

  const actionable = sorted.find((row) => !isBondRowLocked(row) && !isBondRowClosed(row));
  if (actionable) return actionable;

  const notLocked = sorted.find((row) => !isBondRowLocked(row));
  if (notLocked) return notLocked;

  return sorted[sorted.length - 1] || null;
}

function buildBondBookStateSummary(row) {
  if (!row) return null;

  return {
    member_id: safeText(pickFirst(row.member_id, row.cultivation_member_id, row.member_uuid)) || null,
    sl_avatar_key: safeText(row.sl_avatar_key) || null,
    sl_username: safeText(row.sl_username) || null,
    bond_volume_number: safeNumber(
      pickFirst(row.bond_volume_number, row.volume_number, row.volume_index),
      0
    ),
    bond_book_number: safeNumber(
      pickFirst(row.bond_book_number, row.book_number, row.book_index),
      0
    ),
    display_state: safeText(row.display_state) || null,
    status: safeText(row.status) || null,
    offering_complete: Boolean(row.offering_complete),
    minutes_accumulated: safeNumber(
      pickFirst(row.minutes_accumulated, row.shared_minutes_accumulated, row.accumulated_minutes),
      0
    ),
    qi_accumulated: safeNumber(
      pickFirst(row.qi_accumulated, row.shared_qi_accumulated, row.accumulated_qi),
      0
    ),
    started_at: row.started_at || null,
    last_progress_at: pickFirst(row.last_progress_at, row.updated_at) || null,
    paused_at: row.paused_at || null,
    completed_at: row.completed_at || null,
    updated_at: row.updated_at || null
  };
}

function deriveDashboardBondSession({
  focusIsActive,
  currentMemberRow,
  partnerMemberRow,
  memberBookRows
}) {
  if (!focusIsActive) return null;

  const yourRows = (memberBookRows || []).filter((row) =>
    matchBondMemberRowToMember(row, currentMemberRow)
  );

  const partnerRows = (memberBookRows || []).filter((row) =>
    matchBondMemberRowToMember(row, partnerMemberRow)
  );

  const yourFocusRow = pickFocusBondBookRow(yourRows);
  const partnerFocusRow = pickFocusBondBookRow(partnerRows);

  const yourState = safeLower(pickFirst(yourFocusRow?.display_state, yourFocusRow?.status));
  const partnerState = safeLower(pickFirst(partnerFocusRow?.display_state, partnerFocusRow?.status));

  let derivedStatus = "idle";

  if (
    ["active", "in_progress", "under_comprehension", "ready_to_complete"].includes(yourState) ||
    ["active", "in_progress", "under_comprehension", "ready_to_complete"].includes(partnerState)
  ) {
    derivedStatus = "active";
  } else if (yourState === "paused" || partnerState === "paused") {
    derivedStatus = "paused";
  }

  return {
    partnership_id: safeText(
      pickFirst(
        yourFocusRow?.partnership_id,
        partnerFocusRow?.partnership_id
      )
    ) || null,
    status: derivedStatus,
    pause_reason: safeText(
      pickFirst(yourFocusRow?.pause_reason, partnerFocusRow?.pause_reason)
    ) || null,
    partner_a_ready: Boolean(yourFocusRow?.offering_complete),
    partner_b_ready: Boolean(partnerFocusRow?.offering_complete),
    partner_a_meditating: safeLower(currentMemberRow?.v2_cultivation_status) === "cultivating",
    partner_b_meditating: safeLower(partnerMemberRow?.v2_cultivation_status) === "cultivating",
    last_progress_tick_at: pickFirst(
      yourFocusRow?.last_progress_at,
      partnerFocusRow?.last_progress_at,
      yourFocusRow?.updated_at,
      partnerFocusRow?.updated_at
    ) || null,
    started_at: pickFirst(yourFocusRow?.started_at, partnerFocusRow?.started_at) || null,
    paused_at: pickFirst(yourFocusRow?.paused_at, partnerFocusRow?.paused_at) || null,
    stopped_at: null,
    current_member_book_state: buildBondBookStateSummary(yourFocusRow),
    partner_member_book_state: buildBondBookStateSummary(partnerFocusRow)
  };
}

function normalizeBondSessionStatus(status, fallback = "idle") {
  const normalized = safeLower(status, fallback);

  if (normalized === "active") return "active";
  if (normalized === "paused") return "paused";
  if (normalized === "idle") return "idle";

  return safeLower(fallback, "idle") || "idle";
}

function mapBondStateDisplayLabel(status) {
  const normalized = normalizeBondSessionStatus(status, "idle");

  if (normalized === "active") return "Active";
  if (normalized === "paused") return "Paused";
  return "Dormant";
}

function deriveEffectiveBondStatus({ focusIsActive, bondRow, bondSessionRow }) {
  if (!focusIsActive) return "idle";

  const sessionStatus = normalizeBondSessionStatus(bondSessionRow?.status, "");
  if (sessionStatus && ["idle", "paused", "active"].includes(sessionStatus)) {
    return sessionStatus;
  }

  const storedBondStatus = normalizeBondSessionStatus(bondRow?.status, "idle");
  if (storedBondStatus && ["idle", "paused", "active"].includes(storedBondStatus)) {
    return storedBondStatus;
  }

  return "idle";
}

function deriveEffectiveBondPauseReason({ bondRow, bondSessionRow }) {
  return safeText(
    pickFirst(
      bondSessionRow?.pause_reason,
      bondRow?.pause_reason
    )
  ) || null;
}

function isOpenPartnershipStatus(status) {
  const normalized = safeLower(status);
  return normalized === "active" || normalized === "pending";
}

function matchPartnershipBySelectedValue(row, selectedValue) {
  if (!row || !selectedValue) return false;

  return (
    sameValue(row?.id, selectedValue) ||
    sameValue(row?.partnership_id, selectedValue)
  );
}

function resolveSelectedOpenPartnership(selectedPartnershipUuid, rows) {
  if (!selectedPartnershipUuid || !Array.isArray(rows) || !rows.length) {
    return null;
  }

  return (
    rows.find((row) => matchPartnershipBySelectedValue(row, selectedPartnershipUuid)) ||
    null
  );
}

function groupOpenPartnershipRows(rows, currentAvatarKey) {
  const activeRows = [];
  const pendingIncomingRows = [];
  const pendingOutgoingRows = [];

  for (const row of rows || []) {
    const status = safeLower(row?.status);

    if (status === "active") {
      activeRows.push(row);
      continue;
    }

    if (status === "pending") {
      if (sameValue(row?.recipient_avatar_key, currentAvatarKey)) {
        pendingIncomingRows.push(row);
      } else if (sameValue(row?.requester_avatar_key, currentAvatarKey)) {
        pendingOutgoingRows.push(row);
      }
    }
  }

  return {
    activeRows,
    pendingIncomingRows,
    pendingOutgoingRows
  };
}

function resolveFocusPartnership({
  selectedPartnershipUuid,
  selectedPartnership,
  openRows,
  activeRows,
  pendingIncomingRows,
  pendingOutgoingRows
}) {
  if (selectedPartnership && isOpenPartnershipStatus(selectedPartnership.status)) {
    return selectedPartnership;
  }

  const selectedOpenRow = resolveSelectedOpenPartnership(selectedPartnershipUuid, openRows);
  if (selectedOpenRow) {
    return selectedOpenRow;
  }

  if (activeRows.length) return activeRows[0];
  if (pendingIncomingRows.length) return pendingIncomingRows[0];
  if (pendingOutgoingRows.length) return pendingOutgoingRows[0];

  return null;
}

function buildAlignmentRpcPayloadVariants(memberId, slAvatarKey) {
  return uniquePayloadList([
    memberId ? { p_member_id: memberId } : null,
    slAvatarKey ? { p_sl_avatar_key: slAvatarKey } : null,
    memberId && slAvatarKey ? { p_member_id: memberId, p_sl_avatar_key: slAvatarKey } : null,
    memberId ? { member_id: memberId } : null,
    slAvatarKey ? { sl_avatar_key: slAvatarKey } : null,
    memberId && slAvatarKey ? { member_id: memberId, sl_avatar_key: slAvatarKey } : null,
    {}
  ]);
}

function isRetryableAlignmentRpcError(error) {
  const message = safeLower(error?.message);

  return (
    message.includes("could not find the function") ||
    (message.includes("function") && message.includes("does not exist")) ||
    message.includes("schema cache") ||
    message.includes("no function matches") ||
    (message.includes("unexpected") && message.includes("argument")) ||
    message.includes("parameter") ||
    (message.includes("named") && message.includes("argument"))
  );
}

async function callAlignmentRpc(functionName, payloadVariants = [{}]) {
  let lastError = null;

  for (const payload of payloadVariants) {
    const { data, error } = await alignmentSupabase.rpc(functionName, payload);

    if (!error) {
      return data;
    }

    lastError = error;

    if (!isRetryableAlignmentRpcError(error)) {
      break;
    }
  }

  throw new Error(`Failed alignment RPC ${functionName}: ${lastError?.message || "Unknown error"}`);
}

async function loadMemberAlignmentDashboardState(memberId, slAvatarKey) {
  const data = await callAlignmentRpc(
    "load_member_alignment_dashboard_state",
    buildAlignmentRpcPayloadVariants(memberId, slAvatarKey)
  );

  if (Array.isArray(data)) {
    return data[0] || null;
  }

  return data || null;
}

async function getLiveMemberMeditationPreview(memberId, slAvatarKey) {
  const data = await callAlignmentRpc(
    "get_live_member_meditation_preview",
    buildAlignmentRpcPayloadVariants(memberId, slAvatarKey)
  );

  if (Array.isArray(data)) {
    return data[0] || null;
  }

  return data || null;
}

function defaultPathNameFromKey(pathKey) {
  const key = safeLower(pathKey);

  if (key === "yin") return "Yin Path";
  if (key === "yang") return "Yang Path";
  if (key === "balance") return "Balance Path";

  return "Unaligned";
}

function buildAlignmentStateSummary({ dashboardState, previewState }) {
  const pathKey = safeText(
    pickFirst(
      dashboardState?.current_path_key,
      dashboardState?.revealed_path_key,
      dashboardState?.path_key,
      dashboardState?.dominant_path_key,
      previewState?.current_path_key,
      previewState?.revealed_path_key,
      previewState?.path_key,
      previewState?.dominant_path_key
    ),
    "unaligned"
  );

  const pathName = safeText(
    pickFirst(
      dashboardState?.current_path_name,
      dashboardState?.revealed_path_name,
      dashboardState?.path_name,
      previewState?.current_path_name,
      previewState?.revealed_path_name,
      previewState?.path_name
    ),
    defaultPathNameFromKey(pathKey)
  );

  const pathState = safeText(
    pickFirst(
      dashboardState?.path_state,
      dashboardState?.current_path_state,
      dashboardState?.reveal_state,
      previewState?.path_state,
      previewState?.current_path_state
    ),
    pathKey !== "unaligned" ? "revealed" : "unaligned"
  );

  const pathRevealed = safeBoolean(
    pickFirst(
      dashboardState?.path_revealed,
      dashboardState?.is_path_revealed,
      previewState?.path_revealed,
      previewState?.is_path_revealed
    ),
    pathKey !== "unaligned"
  );

  return {
    path_key: pathKey,
    path_name: pathName,
    path_state: pathState,
    path_revealed: pathRevealed,

    yin_total: safeNumber(
      pickFirst(dashboardState?.yin_total, previewState?.yin_total),
      0
    ),
    yang_total: safeNumber(
      pickFirst(dashboardState?.yang_total, previewState?.yang_total),
      0
    ),
    balance_total: safeNumber(
      pickFirst(dashboardState?.balance_total, previewState?.balance_total),
      0
    ),

    hour_group: safeText(
      pickFirst(
        dashboardState?.hour_group,
        dashboardState?.current_hour_group,
        previewState?.hour_group,
        previewState?.current_hour_group
      )
    ) || null,

    phase_name: safeText(
      pickFirst(dashboardState?.phase_name, previewState?.phase_name)
    ) || null,

    force_name: safeText(
      pickFirst(dashboardState?.force_name, previewState?.force_name)
    ) || null,

    phenomenon_name: safeText(
      pickFirst(dashboardState?.phenomenon_name, previewState?.phenomenon_name)
    ) || null,

    effective_bias: safeText(
      pickFirst(dashboardState?.effective_bias, previewState?.effective_bias)
    ) || null,

    qi_multiplier: roundNumber(
      pickFirst(
        dashboardState?.qi_multiplier,
        dashboardState?.current_qi_multiplier,
        previewState?.qi_multiplier,
        previewState?.qi_reward_multiplier,
        1
      ),
      2
    ),

    cp_multiplier: roundNumber(
      pickFirst(
        dashboardState?.cp_multiplier,
        dashboardState?.current_cp_multiplier,
        previewState?.cp_multiplier,
        previewState?.cp_reward_multiplier,
        1
      ),
      2
    ),

    aligned_bonus_available: safeBoolean(
      pickFirst(
        dashboardState?.aligned_bonus_available,
        dashboardState?.bonus_available,
        previewState?.aligned_bonus_available,
        previewState?.bonus_available
      ),
      false
    ),

    aligned_bonus_window_active: safeBoolean(
      pickFirst(
        dashboardState?.aligned_bonus_window_active,
        dashboardState?.in_match_window,
        previewState?.aligned_bonus_window_active,
        previewState?.in_match_window
      ),
      false
    ),

    conversion_target_path_key: safeText(
      pickFirst(
        dashboardState?.conversion_target_path_key,
        dashboardState?.drift_target_path_key,
        previewState?.conversion_target_path_key,
        previewState?.drift_target_path_key
      )
    ) || null,

    conversion_target_path_name: safeText(
      pickFirst(
        dashboardState?.conversion_target_path_name,
        dashboardState?.drift_target_path_name,
        previewState?.conversion_target_path_name,
        previewState?.drift_target_path_name
      )
    ) || null,

    conversion_cost_per_minute: safeNumber(
      pickFirst(
        dashboardState?.conversion_cost_per_minute,
        previewState?.conversion_cost_per_minute
      ),
      0
    )
  };
}

function normalizeResonancePayload(raw) {
  if (!raw || typeof raw !== "object") {
    return {
      active: false,
      reason: "unavailable",
      bondRuntimeActive: false,
      bondSessionStatus: "unknown",
      partnerMeditating: false,
      partnerInRange: false,
      sameRegion: false
    };
  }

  const source =
    raw.runtime ||
    raw.resonance ||
    raw.state ||
    raw.data ||
    raw;

  return {
    active: Boolean(
      source.resonance_active ??
      source.active ??
      source.is_active
    ),
    reason: safeText(
      source.resonance_reason ??
      source.reason,
      "unavailable"
    ),
    bondRuntimeActive: Boolean(
      source.bond_runtime_active ??
      source.bond_active
    ),
    bondSessionStatus: safeText(
      source.bond_session_status ??
      source.bond_status,
      "unknown"
    ),
    partnerMeditating: Boolean(source.partner_meditating),
    partnerInRange: Boolean(source.partner_in_range),
    sameRegion: Boolean(source.same_region)
  };
}

function getBaseUrl(event) {
  const configured =
    safeText(process.env.URL) ||
    safeText(process.env.DEPLOY_PRIME_URL);

  if (configured) {
    return configured.replace(/\/+$/, "");
  }

  const host =
    event?.headers?.["x-forwarded-host"] ||
    event?.headers?.host ||
    event?.headers?.Host;

  const proto =
    event?.headers?.["x-forwarded-proto"] ||
    "https";

  if (host) {
    return `${proto}://${host}`;
  }

  throw new Error("unable_to_resolve_base_url");
}

async function fetchInternalJson(event, path, options = {}) {
  const baseUrl = getBaseUrl(event);
  const incomingCookie =
    event.headers.cookie ||
    event.headers.Cookie ||
    "";

  const response = await fetch(`${baseUrl}${path}`, {
    method: options.method || "GET",
    headers: {
      "Accept": "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(incomingCookie ? { "Cookie": incomingCookie } : {}),
      ...(options.headers || {})
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  const text = await response.text();
  let payload = null;

  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = null;
  }

  if (!response.ok) {
    throw new Error(payload?.error || payload?.message || `internal_request_failed_${response.status}`);
  }

  return payload;
}

async function loadWorldCelestialState(event) {
  try {
    const payload = await fetchInternalJson(
      event,
      "/.netlify/functions/load-world-celestial-state"
    );

    if (payload?.success && payload?.world) {
      return payload.world;
    }

    return null;
  } catch (error) {
    console.error("load-dashboard-notices celestial fetch error:", error);
    return null;
  }
}

async function loadResonanceState(event, memberRow, focusPartnership) {
  const slAvatarKey = safeText(memberRow?.sl_avatar_key);
  const slUsername = safeText(memberRow?.sl_username);
  const partnershipUuid = safeText(focusPartnership?.id);

  if (!slAvatarKey) {
    return normalizeResonancePayload(null);
  }

  try {
    const payload = await fetchInternalJson(
      event,
      "/.netlify/functions/load-resonance",
      {
        method: "POST",
        body: {
          sl_avatar_key: slAvatarKey,
          sl_username: slUsername,
          partnership_uuid: partnershipUuid || ""
        }
      }
    );

    return normalizeResonancePayload(payload);
  } catch (error) {
    console.error("load-dashboard-notices resonance fetch error:", error);
    return normalizeResonancePayload(null);
  }
}

async function loadRealmLibraryRows(slAvatarKey, realmName) {
  const { data, error } = await supabase
    .from("member_library_view")
    .select(`
      id,
      sl_avatar_key,
      sl_username,
      volume_number,
      realm_name,
      item_name,
      volume_status,
      current_section,
      base_status,
      early_status,
      middle_status,
      late_status,
      updated_at
    `)
    .eq("sl_avatar_key", slAvatarKey)
    .eq("realm_name", realmName)
    .order("updated_at", { ascending: false })
    .order("volume_number", { ascending: false });

  if (error) {
    throw new Error(`Failed to load realm library rows: ${error.message}`);
  }

  return data || [];
}

async function resolveActiveRealmBook(memberRow) {
  const stageKey = safeText(memberRow?.v2_active_stage_key);
  const parsedVolume = stageKey ? safeNumber(stageKey.split(":")[0], 0) : 0;

  const fallback = {
    active_realm_volume_number: parsedVolume,
    active_realm_book_name: "",
    active_section_key: safeText(memberRow?.current_section, "base")
  };

  try {
    const realmRows = await loadRealmLibraryRows(
      safeText(memberRow?.sl_avatar_key),
      safeText(memberRow?.realm_name)
    );

    const relevantRow = pickRelevantRealmRow(realmRows);
    if (!relevantRow) return fallback;

    return {
      active_realm_volume_number: safeNumber(relevantRow.volume_number, 0),
      active_realm_book_name: safeText(relevantRow.item_name),
      active_section_key: safeText(relevantRow.current_section, "base")
    };
  } catch (error) {
    console.error("load-dashboard-notices active realm book error:", error);
    return fallback;
  }
}

async function loadOpenPartnershipRows(slAvatarKey) {
  const { data, error } = await supabase
    .schema("partner")
    .from("cultivation_partnerships")
    .select("*")
    .or(`requester_avatar_key.eq.${slAvatarKey},recipient_avatar_key.eq.${slAvatarKey}`)
    .in("status", ["pending", "active"])
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false });

  if (error) {
    throw new Error(`Partnership lookup error: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

async function loadSelectedPartnershipUuid(memberId) {
  if (!memberId) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from("member_selected_partnerships")
    .select("selected_partnership_id, updated_at")
    .eq("member_id", memberId)
    .order("updated_at", { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(`Selected partnership lookup error: ${error.message}`);
  }

  if (!Array.isArray(data) || !data.length) return null;

  return safeText(data[0]?.selected_partnership_id) || null;
}

async function loadPartnershipByUuidForMember(partnershipUuid, currentAvatarKey) {
  if (!partnershipUuid || !currentAvatarKey) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from("cultivation_partnerships")
    .select("*")
    .eq("id", partnershipUuid)
    .or(`requester_avatar_key.eq.${currentAvatarKey},recipient_avatar_key.eq.${currentAvatarKey}`)
    .maybeSingle();

  if (error) {
    throw new Error(`Selected partnership record lookup error: ${error.message}`);
  }

  return data || null;
}

async function loadMembersByAvatarKeys(avatarKeys) {
  const cleanKeys = [...new Set((avatarKeys || []).map((key) => safeText(key)).filter(Boolean))];
  if (!cleanKeys.length) return {};

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .in("sl_avatar_key", cleanKeys);

  if (error) {
    throw new Error(`Partner batch lookup error: ${error.message}`);
  }

  const map = {};
  for (const row of data || []) {
    const key = safeText(row?.sl_avatar_key);
    if (key) map[key] = row;
  }

  return map;
}

async function loadPartnerByAvatarKey(slAvatarKey) {
  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Partner lookup error: ${error.message}`);
  }

  return data || null;
}

async function loadPartnerBondByContext(partnership) {
  if (!partnership) return null;

  const partnershipUuid = safeText(partnership.id);
  if (!partnershipUuid) return null;

  const { data, error } = await partnerSupabase
    .from("partner_bonds")
    .select("*")
    .eq("partnership_id", partnershipUuid)
    .maybeSingle();

  if (error) {
    throw new Error(`Partner schema bond lookup error: ${error.message}`);
  }

  return data || null;
}

async function loadPartnerBondMemberBookRowsByContext(partnership) {
  if (!partnership) return [];

  const partnershipUuid = safeText(partnership.id);
  if (!partnershipUuid) return [];

  const { data, error } = await partnerSupabase
    .from("partner_bond_member_book_states")
    .select("*")
    .eq("partnership_uuid", partnershipUuid);

  if (error) {
    throw new Error(`Partner schema member book state lookup error: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

async function loadDashboardResourceTotals(memberAvatarKey, partnerAvatarKey = null) {
  if (!memberAvatarKey) return null;

  const { data, error } = await supabase.rpc(
    "get_dashboard_resource_totals",
    {
      p_member_avatar_key: memberAvatarKey,
      p_partner_avatar_key: partnerAvatarKey || null
    }
  );

  if (error) {
    throw new Error(`Dashboard resource totals lookup error: ${error.message}`);
  }

  if (Array.isArray(data)) {
    return data[0] || null;
  }

  return data || null;
}

function buildCurrentCultivationFocus(memberActiveRealm) {
  if (
    safeNumber(memberActiveRealm?.active_realm_volume_number, 0) > 0 &&
    safeText(memberActiveRealm?.active_realm_book_name)
  ) {
    return `Volume ${memberActiveRealm.active_realm_volume_number} • ${memberActiveRealm.active_realm_book_name}`;
  }

  return safeText(memberActiveRealm?.active_realm_book_name);
}

function buildMeditationContextText({ alignment, world }) {
  const phaseName = safeText(
    pickFirst(
      world?.phase_name,
      alignment?.phase_name
    )
  );

  const hourGroup = safeText(alignment?.hour_group);
  const hourGroupLabel = hourGroup ? titleize(hourGroup) : "";
  const pathName = safeText(alignment?.path_name, "their path");
  const alignedBonusActive = Boolean(alignment?.aligned_bonus_window_active);
  const pathRevealed = Boolean(alignment?.path_revealed);

  if (phaseName && alignedBonusActive && pathRevealed && hourGroupLabel) {
    return {
      title: "Meditation aligned with the heavens",
      bodyTail: `beneath ${phaseName}. ${pathName} is currently favored during ${hourGroupLabel}.`
    };
  }

  if (phaseName && hourGroupLabel) {
    return {
      title: "Meditation in progress",
      bodyTail: `beneath ${phaseName}. The current flow rests in ${hourGroupLabel}.`
    };
  }

  if (phaseName) {
    return {
      title: "Meditation in progress",
      bodyTail: `beneath ${phaseName}.`
    };
  }

  if (alignedBonusActive && pathRevealed && hourGroupLabel) {
    return {
      title: "Meditation aligned with the heavens",
      bodyTail: `during ${hourGroupLabel}. ${pathName} is currently receiving a favored current.`
    };
  }

  if (hourGroupLabel) {
    return {
      title: "Meditation in progress",
      bodyTail: `during ${hourGroupLabel}.`
    };
  }

  return {
    title: "Meditation in progress",
    bodyTail: "within the current celestial flow."
  };
}

function buildResonanceBody(resonanceState, focusPartnerName) {
  const partnerName = safeText(focusPartnerName, "your focused partner");
  const reason = safeLower(resonanceState?.reason);

  if (reason.includes("partner") && reason.includes("range")) {
    return `${partnerName} is in range and the shared field is responding.`;
  }

  if (reason.includes("same_region")) {
    return `${partnerName} remains within the same region and the shared current is stable.`;
  }

  if (reason.includes("meditating")) {
    return `${partnerName} is actively meditating and the shared current is resonating.`;
  }

  return `${partnerName} is sustaining your shared resonance channel.`;
}

function pushNotice(notices, notice) {
  if (!notice) return;

  const title = safeText(notice.title);
  const body = safeText(notice.body);

  if (!title || !body) return;

  notices.push({
    tone: safeText(notice.tone, "info"),
    priority: safeNumber(notice.priority, 0),
    title,
    body,
    type: safeText(notice.type) || null,
    action_label: safeText(notice.action_label) || null,
    action_href: safeText(notice.action_href) || null
  });
}

function dedupeAndSortNotices(notices = []) {
  const seen = new Set();
  const deduped = [];

  for (const notice of notices) {
    const key = [
      safeText(notice.type),
      safeText(notice.title),
      safeText(notice.body)
    ].join("::");

    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(notice);
  }

  return deduped
    .sort((a, b) => b.priority - a.priority)
    .slice(0, MAX_NOTICES);
}

function buildNoticeFeed({
  memberRow,
  focusPartnership,
  focusPartnerMemberRow,
  focusPartnerOnline,
  pendingIncomingCount,
  resonanceState,
  bondStatus,
  bondStateLabel,
  bondPauseReason,
  bondTitle,
  bondPercent,
  currentCultivationFocus,
  sharedQiPercent,
  sharedCultivationPointsCapped,
  alignmentState,
  worldState
}) {
  const notices = [];

  const memberName = safeText(
    pickFirst(memberRow?.character_name, memberRow?.sl_username),
    "Your cultivator"
  );

  const partnerName = safeText(
    pickFirst(focusPartnerMemberRow?.sl_username, getPartnerIdentityFromPartnership(focusPartnership, memberRow?.sl_avatar_key).username),
    "your focused partner"
  );

  const focusActive = safeLower(focusPartnership?.status) === "active";
  const pathRevealed = Boolean(alignmentState?.path_revealed);
  const alignedBonusWindowActive = Boolean(alignmentState?.aligned_bonus_window_active);
  const conversionCost = safeNumber(alignmentState?.conversion_cost_per_minute, 0);

  if (alignedBonusWindowActive && pathRevealed) {
    pushNotice(notices, {
      tone: "success",
      priority: 110,
      type: "aligned_bonus_window_active",
      title: "Aligned bonus window active",
      body: `${alignmentState.path_name} is currently favored during ${titleize(alignmentState.hour_group, "the current hour")}. Qi ${formatMultiplier(alignmentState.qi_multiplier)} • CP ${formatMultiplier(alignmentState.cp_multiplier)}.`,
      action_label: "Open Shrine",
      action_href: "/cultivation.html"
    });
  }

  if (sharedQiPercent <= LOW_QI_WARNING_PERCENT) {
    pushNotice(notices, {
      tone: "danger",
      priority: 100,
      type: "low_qi_warning",
      title: "Qi is running low",
      body: `Your current vessel reserve is down to ${formatPercent(sharedQiPercent)}.`,
      action_label: "Return to Shrine",
      action_href: "/cultivation.html"
    });
  }

  if (bondStatus === "paused") {
    pushNotice(notices, {
      tone: "warn",
      priority: 94,
      type: "bond_paused",
      title: "Bond cultivation paused",
      body: safeText(
        bondPauseReason,
        `${bondTitle || "Your bond cultivation"} is paused and waiting for both sides to stabilize.`
      ),
      action_label: "Open Partnership",
      action_href: "/partnership.html"
    });
  }

  if (focusActive && !focusPartnerOnline && partnerName) {
    pushNotice(notices, {
      tone: "warn",
      priority: 88,
      type: "focused_partner_offline",
      title: "Focused partner is offline",
      body: `${partnerName} is not live right now, so your linked states may dim until they return.`,
      action_label: "Open Partnership",
      action_href: "/partnership.html"
    });
  }

  if (pendingIncomingCount > 0) {
    pushNotice(notices, {
      tone: "warn",
      priority: 84,
      type: "pending_incoming_partnership_requests",
      title: "Pending partnership request",
      body: `${pendingIncomingCount} incoming request${pendingIncomingCount === 1 ? "" : "s"} waiting in the partnership hall.`,
      action_label: "Review Requests",
      action_href: "/partnership.html"
    });
  }

  if (resonanceState?.active) {
    pushNotice(notices, {
      tone: "success",
      priority: 80,
      type: "resonance_active",
      title: "Resonance is active",
      body: buildResonanceBody(resonanceState, partnerName),
      action_label: "Open Dashboard",
      action_href: "/dashboard.html"
    });
  }

  if (bondStatus === "active") {
    pushNotice(notices, {
      tone: "success",
      priority: 78,
      type: "bond_cultivation_active",
      title: "Bond cultivation active",
      body: `${bondTitle || "Your bond cultivation"} is moving at ${formatPercent(bondPercent)} under a ${bondStateLabel.toLowerCase()} channel.`,
      action_label: "Open Partnership",
      action_href: "/partnership.html"
    });
  }

  if (safeLower(memberRow?.v2_cultivation_status) === "cultivating") {
    const meditationContext = buildMeditationContextText({
      alignment: alignmentState,
      world: worldState
    });

    pushNotice(notices, {
      tone: alignedBonusWindowActive ? "success" : "info",
      priority: 74,
      type: "meditation_active",
      title: meditationContext.title,
      body: `${memberName} is actively meditating ${meditationContext.bodyTail}`,
      action_label: "Open Shrine",
      action_href: "/cultivation.html"
    });
  }

  if (safeText(currentCultivationFocus)) {
    pushNotice(notices, {
      tone: "info",
      priority: 70,
      type: "cultivation_focus_anchored",
      title: "Cultivation focus anchored",
      body: `${currentCultivationFocus} remains your active cultivation line.`,
      action_label: "Open Archive",
      action_href: "/library.html"
    });
  }

  if (sharedCultivationPointsCapped) {
    pushNotice(notices, {
      tone: "info",
      priority: 66,
      type: "cultivation_points_capped",
      title: "Current stage cap reached",
      body: "Your Cultivation Points are capped at the current realm stage maximum.",
      action_label: "Review Progress",
      action_href: "/cultivation.html"
    });
  }

  if (focusPartnership && partnerName) {
    pushNotice(notices, {
      tone: focusPartnerOnline ? "success" : "info",
      priority: 62,
      type: "focused_link_attuned",
      title: "Focused link attuned",
      body: `${partnerName} remains your focused partner connection.`,
      action_label: "Open Partnership",
      action_href: "/partnership.html"
    });
  }

  if (worldState?.has_active_phenomenon && safeText(worldState?.phenomenon_name)) {
    pushNotice(notices, {
      tone: "info",
      priority: 60,
      type: "celestial_phenomenon_active",
      title: `${worldState.phenomenon_name} is active`,
      body: safeText(
        pickFirst(worldState?.phenomenon_omen_text, worldState?.dashboard_effect_summary),
        "A major celestial sign is moving across the heavens."
      ),
      action_label: "Open Celestial Omen",
      action_href: "/celestial-omen.html"
    });
  }

  if (safeText(worldState?.dashboard_effect_summary)) {
    pushNotice(notices, {
      tone: "info",
      priority: 56,
      type: "celestial_current_changed",
      title: "Celestial current changed",
      body: safeText(worldState.dashboard_effect_summary),
      action_label: "Open Breath of Celestial",
      action_href: "/breath-of-celestial.html"
    });
  }

  if (
    pathRevealed &&
    safeText(alignmentState?.conversion_target_path_name) &&
    conversionCost > 0
  ) {
    pushNotice(notices, {
      tone: "warn",
      priority: 52,
      type: "path_drift_conversion_cost_warning",
      title: "Path drift has a cost",
      body: `Moving toward ${alignmentState.conversion_target_path_name} would cost ${formatNumber(conversionCost)} Qi per minute.`,
      action_label: "Open Essence",
      action_href: "/profile.html"
    });
  }

  return dedupeAndSortNotices(notices);
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const cookieHeader =
      event.headers.cookie ||
      event.headers.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionCookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = cookies[sessionCookieName];

    if (!sessionToken) {
      return json(401, {
        success: false,
        error: "not_logged_in"
      });
    }

    const { data: sessionRow, error: sessionError } = await supabase
      .from("website_sessions")
      .select("*")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();

    if (sessionError) {
      console.error("Notice loader session lookup error:", sessionError);
      return json(500, {
        success: false,
        error: "server_error"
      });
    }

    if (!sessionRow) {
      return json(401, {
        success: false,
        error: "invalid_session"
      });
    }

    const { data: memberRow, error: memberError } = await supabase
      .from("cultivation_members")
      .select("*")
      .eq("sl_avatar_key", sessionRow.sl_avatar_key)
      .maybeSingle();

    if (memberError) {
      console.error("Notice loader member lookup error:", memberError);
      return json(500, {
        success: false,
        error: "server_error"
      });
    }

    if (!memberRow) {
      return json(404, {
        success: false,
        error: "member_not_found"
      });
    }

    const memberPrimaryId = getMemberPrimaryId(memberRow);
    const memberActiveRealm = await resolveActiveRealmBook(memberRow);
    const currentCultivationFocus = buildCurrentCultivationFocus(memberActiveRealm);

    const openPartnershipRows = await loadOpenPartnershipRows(memberRow.sl_avatar_key);
    const {
      activeRows,
      pendingIncomingRows,
      pendingOutgoingRows
    } = groupOpenPartnershipRows(openPartnershipRows, memberRow.sl_avatar_key);

    let selectedPartnershipUuid = null;
    let selectedPartnershipRow = null;

    try {
      selectedPartnershipUuid = await loadSelectedPartnershipUuid(memberPrimaryId);

      if (selectedPartnershipUuid) {
        selectedPartnershipRow = await loadPartnershipByUuidForMember(
          selectedPartnershipUuid,
          memberRow.sl_avatar_key
        );
      }
    } catch (selectedError) {
      console.error("Notice loader selected partnership resolution error:", selectedError);
    }

    const focusPartnership = resolveFocusPartnership({
      selectedPartnershipUuid,
      selectedPartnership: selectedPartnershipRow,
      openRows: openPartnershipRows,
      activeRows,
      pendingIncomingRows,
      pendingOutgoingRows
    });

    const focusIsActive = safeLower(focusPartnership?.status) === "active";

    const focusPartnerIdentity = getPartnerIdentityFromPartnership(
      focusPartnership,
      memberRow.sl_avatar_key
    );

    const partnerMap = await loadMembersByAvatarKeys([
      ...activeRows.map((row) =>
        getPartnerIdentityFromPartnership(row, memberRow.sl_avatar_key).avatarKey
      ),
      ...pendingIncomingRows.map((row) =>
        getPartnerIdentityFromPartnership(row, memberRow.sl_avatar_key).avatarKey
      ),
      ...pendingOutgoingRows.map((row) =>
        getPartnerIdentityFromPartnership(row, memberRow.sl_avatar_key).avatarKey
      ),
      focusPartnerIdentity.avatarKey
    ].filter(Boolean));

    const focusPartnerMemberRow =
      partnerMap[safeText(focusPartnerIdentity.avatarKey)] ||
      (focusPartnerIdentity.avatarKey
        ? await loadPartnerByAvatarKey(focusPartnerIdentity.avatarKey)
        : null);

    const focusPartnerOnline =
      Boolean(focusPartnerMemberRow) &&
      isPartnerActive(focusPartnerMemberRow.last_presence_at);

    let resourceTotals = null;
    try {
      resourceTotals = await loadDashboardResourceTotals(
        memberRow.sl_avatar_key,
        focusIsActive ? safeText(focusPartnerIdentity.avatarKey) : null
      );
    } catch (resourceTotalsError) {
      console.error("Notice loader dashboard resource totals error:", resourceTotalsError);
    }

    let focusBondRow = null;
    let focusBondSessionRow = null;
    let focusBondMemberBookRows = [];

    if (focusIsActive) {
      try {
        focusBondRow = await loadPartnerBondByContext(focusPartnership);
      } catch (bondError) {
        console.error("Notice loader bond lookup error:", bondError);
      }

      try {
        focusBondMemberBookRows = await loadPartnerBondMemberBookRowsByContext(focusPartnership);
      } catch (bondBookError) {
        console.error("Notice loader bond member book lookup error:", bondBookError);
      }

      focusBondSessionRow = deriveDashboardBondSession({
        focusIsActive,
        currentMemberRow: memberRow,
        partnerMemberRow: focusPartnerMemberRow,
        memberBookRows: focusBondMemberBookRows
      });
    }

    const effectiveBondStatus = deriveEffectiveBondStatus({
      focusIsActive,
      bondRow: focusBondRow,
      bondSessionRow: focusBondSessionRow
    });

    const effectiveBondPauseReason = deriveEffectiveBondPauseReason({
      bondRow: focusBondRow,
      bondSessionRow: focusBondSessionRow
    });

    const bondStateLabel = mapBondStateDisplayLabel(effectiveBondStatus);

    const bondTitle = safeText(
      pickFirst(
        focusBondRow?.current_stage_name,
        focusBondRow?.bond_stage_name,
        focusBondRow?.stage_name
      )
    ) || null;

    const rawBondPercent = Number(
      pickFirst(
        focusBondRow?.bond_percent,
        focusBondRow?.current_percent,
        focusBondRow?.progress_percent,
        0
      )
    );

    const bondPercent = Number.isFinite(rawBondPercent) ? rawBondPercent : 0;

    const personalQiCurrent = safeNumber(
      resourceTotals?.personal_qi_current,
      safeNumber(memberRow.qi_current, 0)
    );

    const personalQiMaximum = safeNumber(
      resourceTotals?.personal_qi_maximum,
      safeNumber(memberRow.qi_maximum, 0)
    );

    const partnerQiCurrent = safeNumber(
      resourceTotals?.partner_qi_current,
      safeNumber(focusPartnerMemberRow?.qi_current, 0)
    );

    const partnerQiMaximum = safeNumber(
      resourceTotals?.partner_qi_maximum,
      safeNumber(focusPartnerMemberRow?.qi_maximum, 0)
    );

    const sharedQiCurrent = safeNumber(
      resourceTotals?.shared_qi_current,
      personalQiCurrent + partnerQiCurrent
    );

    const sharedQiMaximum = safeNumber(
      resourceTotals?.shared_qi_maximum,
      personalQiMaximum + partnerQiMaximum
    );

    const sharedQiPercent = safeNumber(
      resourceTotals?.shared_qi_percent,
      sharedQiMaximum > 0
        ? Math.round(Math.max(0, Math.min(100, (sharedQiCurrent / sharedQiMaximum) * 100)))
        : 0
    );

    const sharedCultivationPointsCapped = Boolean(
      resourceTotals?.shared_cultivation_points_capped
    );

    let alignmentDashboardState = null;
    let alignmentMeditationPreview = null;

    const alignmentPreviewAppliesToLiveMeditation =
      safeLower(memberRow?.v2_cultivation_status) === "cultivating" &&
      effectiveBondStatus === "idle";

    try {
      alignmentDashboardState = await loadMemberAlignmentDashboardState(
        memberPrimaryId,
        memberRow.sl_avatar_key
      );

      if (alignmentPreviewAppliesToLiveMeditation) {
        alignmentMeditationPreview = await getLiveMemberMeditationPreview(
          memberPrimaryId,
          memberRow.sl_avatar_key
        );
      }
    } catch (alignmentError) {
      console.error("Notice loader alignment error:", alignmentError);
    }

    const alignmentState = buildAlignmentStateSummary({
      dashboardState: alignmentDashboardState,
      previewState: alignmentMeditationPreview
    });

    const [worldState, resonanceState] = await Promise.all([
      loadWorldCelestialState(event),
      loadResonanceState(event, memberRow, focusPartnership)
    ]);

    const notices = buildNoticeFeed({
      memberRow,
      focusPartnership,
      focusPartnerMemberRow,
      focusPartnerOnline,
      pendingIncomingCount: pendingIncomingRows.length,
      resonanceState,
      bondStatus: normalizeBondSessionStatus(effectiveBondStatus, "idle"),
      bondStateLabel,
      bondPauseReason: effectiveBondPauseReason,
      bondTitle,
      bondPercent,
      currentCultivationFocus,
      sharedQiPercent,
      sharedCultivationPointsCapped,
      alignmentState,
      worldState
    });

    return json(200, {
      success: true,
      generated_at: new Date().toISOString(),
      notices_count: notices.length,
      notices
    });
  } catch (err) {
    console.error("load-dashboard-notices error:", err);
    return json(500, {
      success: false,
      error: "server_error"
    });
  }
};