const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const librarySupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "library" } }
);

const partnerSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "partner" } }
);

const alignmentSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "alignment" } }
);

// =========================================================
// CONSTANTS
// =========================================================

const PARTNER_ACTIVE_WINDOW_SECONDS = 90;
const VALID_CULTIVATION_MODES = ["idle", "personal", "bond", "realm"];
const VALID_SECTIONS = ["base", "early", "middle", "late"];

const BOND_MANUAL_TYPES = new Set([
  "bond",
  "bond_book",
  "bond_cultivation",
  "partner_bond",
  "partnership"
]);

const REALM_INDEX_MAP = {
  mortal: 1,
  "mortal realm": 1,
  "qi gathering": 2,
  "qi gathering realm": 2,
  foundation: 3,
  "foundation realm": 3,
  "core formation": 4,
  "core formation realm": 4,
  "nascent soul": 5,
  "nascent soul realm": 5,
  "soul transformation": 6,
  "soul transformation realm": 6,
  "void refinement": 7,
  "void refinement realm": 7,
  "body integration": 8,
  "body integration realm": 8,
  mahayana: 9,
  "mahayana realm": 9,
  tribulation: 10,
  "tribulation realm": 10
};

const REALM_META_BY_INDEX = {
  1: { realm_key: "mortal", realm_name: "mortal", realm_display_name: "Mortal Realm" },
  2: { realm_key: "qi_gathering", realm_name: "qi gathering", realm_display_name: "Qi Gathering Realm" },
  3: { realm_key: "foundation", realm_name: "foundation", realm_display_name: "Foundation Realm" },
  4: { realm_key: "core_formation", realm_name: "core formation", realm_display_name: "Core Formation Realm" },
  5: { realm_key: "nascent_soul", realm_name: "nascent soul", realm_display_name: "Nascent Soul Realm" },
  6: { realm_key: "soul_transformation", realm_name: "soul transformation", realm_display_name: "Soul Transformation Realm" },
  7: { realm_key: "void_refinement", realm_name: "void refinement", realm_display_name: "Void Refinement Realm" },
  8: { realm_key: "body_integration", realm_name: "body integration", realm_display_name: "Body Integration Realm" },
  9: { realm_key: "mahayana", realm_name: "mahayana", realm_display_name: "Mahayana Realm" },
  10: { realm_key: "tribulation", realm_name: "tribulation", realm_display_name: "Tribulation Realm" }
};

const FIELD = {
  avatarKey: "sl_avatar_key",
  username: "sl_username",
  auricCurrent: "auric_current",
  auricMaximum: "auric_maximum",
  cultivationPoints: "vestiges",
  v2CultivationStatus: "v2_cultivation_status",
  v2ActiveStageKey: "v2_active_stage_key",
  v2BreakthroughGateOpen: "v2_breakthrough_gate_open",
  v2AccumulatedSeconds: "v2_accumulated_seconds",
  v2CultivationStartedAt: "v2_cultivation_started_at",
  v2SessionsToday: "v2_sessions_today",
  lastPresenceAt: "last_presence_at",
  lastHudSyncAt: "last_hud_sync_at",
  currentRegionName: "current_region_name",
  currentPositionX: "current_position_x",
  currentPositionY: "current_position_y",
  currentPositionZ: "current_position_z",
  updatedAt: "updated_at"
};

// =========================================================
// UTILITY FUNCTIONS
// =========================================================

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function safeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = safeLower(value);
  if (["true", "1", "yes", "y", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function roundNumber(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Number(number.toFixed(digits));
}

function normalize(value) {
  return safeText(value).toLowerCase();
}

function normalizeRealmKeyForDb(value) {
  return safeText(value, "mortal")
    .toLowerCase()
    .replace(/\s+realm$/i, "")
    .replace(/\s+/g, "_");
}

function normalizeRealmStageKeyForDb(value) {
  const normalized = safeText(value, "base").toLowerCase();
  return VALID_SECTIONS.includes(normalized) ? normalized : "base";
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function unwrapRpcRow(data) {
  if (Array.isArray(data)) return data[0] || null;
  return data || null;
}

function toTitle(value, fallback = "Base") {
  const text = safeLower(value);
  if (!text) return fallback;
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function pickFirstText(source, keys) {
  for (const key of keys) {
    const value = safeText(source?.[key]);
    if (value) return value;
  }
  return "";
}

function pickFirstNumber(source, keys) {
  for (const key of keys) {
    const value = toNumberOrNull(source?.[key]);
    if (value !== null) return value;
  }
  return null;
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
  return [...new Set((values || []).map((v) => safeText(v)).filter(Boolean))];
}

function uniquePayloadList(values = []) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    const payload = value && typeof value === "object" ? value : {};
    const key = JSON.stringify(payload, Object.keys(payload).sort());
    if (!seen.has(key)) {
      seen.add(key);
      result.push(payload);
    }
  }

  return result;
}

function isRecentIso(isoValue, windowSeconds = PARTNER_ACTIVE_WINDOW_SECONDS) {
  if (!isoValue) return false;
  const valueMs = new Date(isoValue).getTime();
  if (!Number.isFinite(valueMs) || valueMs <= 0) return false;
  return Date.now() - valueMs <= windowSeconds * 1000;
}

function getIsoAgeSeconds(isoValue, nowIso) {
  if (!isoValue) return null;
  const valueMs = new Date(isoValue).getTime();
  const nowMs = new Date(nowIso).getTime();
  if (!Number.isFinite(valueMs) || valueMs <= 0) return null;
  if (!Number.isFinite(nowMs) || nowMs < valueMs) return null;
  return Math.floor((nowMs - valueMs) / 1000);
}

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(safeNumber(totalSeconds, 0)));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (hours > 0 && minutes > 0) return `${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h`;
  if (minutes > 0) return `${minutes}m`;
  return "0m";
}

function finalizeRealmNameValue(realmName) {
  return safeText(realmName)
    .split("_")
    .join(" ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

// =========================================================
// REALM HELPERS
// =========================================================

function getMemberPrimaryId(member) {
  return safeText(pickFirst(member?.member_id, member?.id), "");
}

function getMemberIdentifiers(member) {
  return uniqueTextList([
    member?.member_id,
    member?.id,
    member?.sl_avatar_key,
    member?.sl_username
  ]);
}

function getCanonicalRealmIndex(member) {
  const directIndex = Number(member?.realm_index);
  if (Number.isInteger(directIndex) && directIndex >= 1) return directIndex;

  const candidates = [member?.realm_key, member?.realm_name, member?.realm_display_name];
  for (const candidate of candidates) {
    const normalizedCandidate = safeLower(candidate);
    if (
      normalizedCandidate &&
      Object.prototype.hasOwnProperty.call(REALM_INDEX_MAP, normalizedCandidate)
    ) {
      return REALM_INDEX_MAP[normalizedCandidate];
    }
  }

  return 1;
}

function getCanonicalRealmMeta(member) {
  const realmIndex = getCanonicalRealmIndex(member);
  const catalog = REALM_META_BY_INDEX[realmIndex] || REALM_META_BY_INDEX[1];

  return {
    realm_index: realmIndex,
    realm_key: safeText(member?.realm_key, catalog.realm_key),
    realm_name: safeText(member?.realm_name, catalog.realm_name),
    realm_display_name: safeText(
      member?.realm_display_name,
      catalog.realm_display_name
    )
  };
}

// =========================================================
// MEDITATION / CULTIVATION MODE HELPERS
// =========================================================

function detectMeditationState(member) {
  const status = safeLower(member?.v2_cultivation_status);
  return {
    is_active: status === "cultivating",
    raw_state: status || null,
    started_at: member?.v2_cultivation_started_at || null
  };
}

function getMemberCultivationMode(member, bondRuntime = null) {
  const runtimeStatus = safeLower(pickFirst(bondRuntime?.session_status, bondRuntime?.status));
  if (runtimeStatus === "active" || runtimeStatus === "paused") return "bond";

  const status = safeLower(member?.v2_cultivation_status);
  if (status === "cultivating") return "personal";
  if (status === "in_breakthrough") return "breakthrough";
  return "idle";
}

function getMeditationStartedAt(member) {
  return member?.v2_cultivation_started_at || null;
}

function getLiveSessionSeconds(startedAtIso, nowIso) {
  if (!startedAtIso) return 0;

  const startMs = new Date(startedAtIso).getTime();
  const nowMs = new Date(nowIso).getTime();

  if (!Number.isFinite(startMs) || startMs <= 0) return 0;
  if (!Number.isFinite(nowMs) || nowMs <= startMs) return 0;

  return Math.floor((nowMs - startMs) / 1000);
}

function buildMemberWithLiveSession(member, nowIso, bondRuntime = null) {
  const status = safeLower(member?.v2_cultivation_status);
  const isCultivating = status === "cultivating";
  const startedAt = member?.v2_cultivation_started_at || null;
  const liveSessionSeconds = isCultivating
    ? getLiveSessionSeconds(startedAt, nowIso)
    : safeNumber(member?.v2_accumulated_seconds, 0);

  return {
    ...member,
    v2_cultivation_started_at: startedAt,
    v2_accumulated_seconds: liveSessionSeconds,
    cultivation_mode: getMemberCultivationMode(member, bondRuntime)
  };
}

// =========================================================
// PARTNERSHIP / BOND HELPERS
// =========================================================

function buildPartnerPresence(partnerMember) {
  if (!partnerMember) {
    return {
      is_online: false,
      last_presence_at: null,
      last_hud_sync_at: null,
      meditation_active: false,
      cultivation_mode: "idle",
      current_region_name: "",
      current_position_x: null,
      current_position_y: null,
      current_position_z: null
    };
  }

  return {
    is_online: isRecentIso(partnerMember.last_presence_at || partnerMember.last_hud_sync_at),
    last_presence_at: partnerMember.last_presence_at || null,
    last_hud_sync_at: partnerMember.last_hud_sync_at || null,
    meditation_active: safeLower(partnerMember?.v2_cultivation_status) === "cultivating",
    cultivation_mode: getMemberCultivationMode(partnerMember),
    current_region_name: safeText(partnerMember.current_region_name),
    current_position_x: toNumberOrNull(partnerMember.current_position_x),
    current_position_y: toNumberOrNull(partnerMember.current_position_y),
    current_position_z: toNumberOrNull(partnerMember.current_position_z)
  };
}

function buildCounterpartSummary(partnership, memberAvatarKey, partnerMember) {
  if (!partnership) {
    return {
      partnership_uuid: null,
      partnership_id: null,
      partnership_status: null,
      partner_avatar_key: null,
      partner_username: null,
      partner_member_id: null,
      partner_is_online: false,
      partner_last_presence_at: null,
      partner_last_hud_sync_at: null,
      partner_meditation_active: false,
      partner_cultivation_mode: "idle",
      partner_current_region_name: "",
      partner_current_position_x: null,
      partner_current_position_y: null,
      partner_current_position_z: null,
      relation_role: null
    };
  }

  const isRequester = safeText(partnership.requester_avatar_key) === safeText(memberAvatarKey);
  const partnerAvatarKey = isRequester
    ? safeText(partnership.recipient_avatar_key)
    : safeText(partnership.requester_avatar_key);
  const partnerUsernameFromPartnership = isRequester
    ? safeText(partnership.recipient_username)
    : safeText(partnership.requester_username);

  const presence = buildPartnerPresence(partnerMember);

  return {
    partnership_uuid: safeText(partnership.id) || null,
    partnership_id: safeText(partnership.partnership_id) || null,
    partnership_status: safeText(partnership.status) || null,
    partner_avatar_key: partnerAvatarKey || null,
    partner_username: safeText(partnerMember?.sl_username, partnerUsernameFromPartnership) || null,
    partner_member_id: getMemberPrimaryId(partnerMember) || null,
    partner_is_online: Boolean(presence.is_online),
    partner_last_presence_at: presence.last_presence_at,
    partner_last_hud_sync_at: presence.last_hud_sync_at,
    partner_meditation_active: Boolean(presence.meditation_active),
    partner_cultivation_mode: safeText(presence.cultivation_mode, "idle"),
    partner_current_region_name: safeText(presence.current_region_name),
    partner_current_position_x: presence.current_position_x,
    partner_current_position_y: presence.current_position_y,
    partner_current_position_z: presence.current_position_z,
    relation_role: isRequester ? "requester" : "recipient"
  };
}

function matchBondMemberRowToMember(row, member) {
  if (!row || !member) return false;

  const rowIdentifiers = uniqueTextList([
    row?.member_id,
    row?.cultivation_member_id,
    row?.member_uuid,
    row?.sl_avatar_key,
    row?.sl_username
  ]);

  const memberIdentifiers = getMemberIdentifiers(member);

  if (!rowIdentifiers.length || !memberIdentifiers.length) return false;

  return rowIdentifiers.some((rv) =>
    memberIdentifiers.some((mv) => normalize(rv) === normalize(mv))
  );
}

function sortBondBookRows(rows = []) {
  return [...rows].sort((a, b) => {
    const aVol = safeNumber(pickFirst(a?.bond_volume_number, a?.volume_number, a?.volume_index), 0);
    const bVol = safeNumber(pickFirst(b?.bond_volume_number, b?.volume_number, b?.volume_index), 0);
    if (aVol !== bVol) return aVol - bVol;

    const aBook = safeNumber(pickFirst(a?.bond_book_number, a?.book_number, a?.book_index), 0);
    const bBook = safeNumber(pickFirst(b?.bond_book_number, b?.book_number, b?.book_index), 0);
    if (aBook !== bBook) return aBook - bBook;

    return new Date(b?.updated_at || 0).getTime() - new Date(a?.updated_at || 0).getTime();
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
    bond_volume_number: safeNumber(pickFirst(row.bond_volume_number, row.volume_number, row.volume_index), 0),
    bond_book_number: safeNumber(pickFirst(row.bond_book_number, row.book_number, row.book_index), 0),
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

function deriveBondRuntimeFromMemberBooks({ currentMember, partnerMember, memberBookRows }) {
  if (!Array.isArray(memberBookRows) || !memberBookRows.length) return null;

  const yourRows = memberBookRows.filter((row) => matchBondMemberRowToMember(row, currentMember));
  const partnerRows = memberBookRows.filter((row) => matchBondMemberRowToMember(row, partnerMember));

  const yourFocusRow = pickFocusBondBookRow(yourRows);
  const partnerFocusRow = pickFocusBondBookRow(partnerRows);

  const yourState = safeLower(pickFirst(yourFocusRow?.display_state, yourFocusRow?.status));
  const partnerState = safeLower(pickFirst(partnerFocusRow?.display_state, partnerFocusRow?.status));

  let status = "idle";

  if (
    ["active", "in_progress", "under_comprehension", "ready_to_complete"].includes(yourState) ||
    ["active", "in_progress", "under_comprehension", "ready_to_complete"].includes(partnerState)
  ) {
    status = "active";
  } else if (yourState === "paused" || partnerState === "paused") {
    status = "paused";
  }

  return {
    status,
    session_status: status,
    pause_reason: safeText(pickFirst(yourFocusRow?.pause_reason, partnerFocusRow?.pause_reason)) || null,
    current_member_book_state: buildBondBookStateSummary(yourFocusRow),
    partner_member_book_state: buildBondBookStateSummary(partnerFocusRow),
    last_progress_at:
      pickFirst(
        yourFocusRow?.last_progress_at,
        partnerFocusRow?.last_progress_at,
        yourFocusRow?.updated_at,
        partnerFocusRow?.updated_at
      ) || null
  };
}

// =========================================================
// ALIGNMENT HELPERS
// =========================================================

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
    if (!error) return data;

    lastError = error;
    if (!isRetryableAlignmentRpcError(error)) break;
  }

  throw new Error(`Failed alignment RPC ${functionName}: ${lastError?.message || "Unknown error"}`);
}

async function loadMemberAlignmentDashboardState(memberId, slAvatarKey) {
  const data = await callAlignmentRpc(
    "load_member_alignment_dashboard_state",
    buildAlignmentRpcPayloadVariants(memberId, slAvatarKey)
  );
  return unwrapRpcRow(data);
}

async function getLiveMemberMeditationPreview(memberId, slAvatarKey) {
  const data = await callAlignmentRpc(
    "get_live_member_meditation_preview",
    buildAlignmentRpcPayloadVariants(memberId, slAvatarKey)
  );
  return unwrapRpcRow(data);
}

function defaultPathNameFromKey(pathKey) {
  const key = safeLower(pathKey);
  if (key === "yin") return "Yin Path";
  if (key === "yang") return "Yang Path";
  if (key === "balance") return "Balance Path";
  return "Unaligned";
}

function buildAlignmentStateSummary({ dashboardState, previewState, applyState }) {
  const pathKey = safeText(
    pickFirst(
      dashboardState?.current_path_key,
      dashboardState?.revealed_path_key,
      dashboardState?.path_key,
      previewState?.current_path_key,
      previewState?.path_key,
      applyState?.current_path_key,
      applyState?.path_key
    ),
    "unaligned"
  );

  const pathName = safeText(
    pickFirst(
      dashboardState?.current_path_name,
      dashboardState?.path_name,
      previewState?.current_path_name,
      previewState?.path_name,
      applyState?.current_path_name,
      applyState?.path_name
    ),
    defaultPathNameFromKey(pathKey)
  );

  const pathState = safeText(
    pickFirst(
      dashboardState?.path_state,
      dashboardState?.current_path_state,
      previewState?.path_state,
      applyState?.path_state
    ),
    pathKey !== "unaligned" ? "revealed" : "unaligned"
  );

  const pathRevealed = safeBoolean(
    pickFirst(
      dashboardState?.path_revealed,
      dashboardState?.is_path_revealed,
      previewState?.path_revealed,
      applyState?.path_revealed
    ),
    pathKey !== "unaligned"
  );

  return {
    path_key: pathKey,
    path_name: pathName,
    path_state: pathState,
    path_revealed: pathRevealed,
    yin_total: safeNumber(pickFirst(dashboardState?.yin_total, previewState?.yin_total, applyState?.yin_total), 0),
    yang_total: safeNumber(pickFirst(dashboardState?.yang_total, previewState?.yang_total, applyState?.yang_total), 0),
    balance_total: safeNumber(pickFirst(dashboardState?.balance_total, previewState?.balance_total, applyState?.balance_total), 0),
    hour_group: safeText(pickFirst(dashboardState?.hour_group, dashboardState?.current_hour_group, previewState?.hour_group, applyState?.hour_group)) || null,
    phase_name: safeText(pickFirst(dashboardState?.phase_name, previewState?.phase_name, applyState?.phase_name)) || null,
    force_name: safeText(pickFirst(dashboardState?.force_name, previewState?.force_name, applyState?.force_name)) || null,
    phenomenon_name: safeText(pickFirst(dashboardState?.phenomenon_name, previewState?.phenomenon_name, applyState?.phenomenon_name)) || null,
    effective_bias: safeText(pickFirst(dashboardState?.effective_bias, previewState?.effective_bias, applyState?.effective_bias)) || null,
    qi_multiplier: roundNumber(pickFirst(dashboardState?.qi_multiplier, previewState?.qi_multiplier, applyState?.qi_multiplier, 1), 2),
    cp_multiplier: roundNumber(pickFirst(dashboardState?.cp_multiplier, previewState?.cp_multiplier, applyState?.cp_multiplier, 1), 2),
    aligned_bonus_available: safeBoolean(
      pickFirst(dashboardState?.aligned_bonus_available, previewState?.aligned_bonus_available, applyState?.aligned_bonus_available),
      false
    ),
    aligned_bonus_window_active: safeBoolean(
      pickFirst(dashboardState?.aligned_bonus_window_active, previewState?.aligned_bonus_window_active, applyState?.aligned_bonus_window_active),
      false
    ),
    conversion_target_path_key: safeText(
      pickFirst(
        dashboardState?.conversion_target_path_key,
        previewState?.conversion_target_path_key,
        applyState?.conversion_target_path_key
      )
    ) || null,
    conversion_target_path_name: safeText(
      pickFirst(
        dashboardState?.conversion_target_path_name,
        previewState?.conversion_target_path_name,
        applyState?.conversion_target_path_name
      )
    ) || null,
    conversion_cost_per_minute: safeNumber(
      pickFirst(
        dashboardState?.conversion_cost_per_minute,
        previewState?.conversion_cost_per_minute,
        applyState?.conversion_cost_per_minute
      ),
      0
    ),
    last_applied_at:
      pickFirst(
        applyState?.applied_at,
        applyState?.updated_at,
        dashboardState?.updated_at,
        previewState?.updated_at
      ) || null
  };
}

// =========================================================
// DATABASE FETCH HELPERS
// =========================================================

async function loadFullMemberByAvatarKey(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .maybeSingle();

  if (error) throw new Error(`Failed to reload cultivation member: ${error.message}`);
  return data || null;
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

  if (error) throw new Error(`Failed to load selected partnership: ${error.message}`);
  if (!Array.isArray(data) || !data.length) return null;

  return safeText(data[0]?.selected_partnership_id) || null;
}

async function loadPartnershipByUuidForMember(partnershipUuid, slAvatarKey) {
  if (!partnershipUuid || !slAvatarKey) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from("cultivation_partnerships")
    .select(
      "id,partnership_id,status,requester_avatar_key,recipient_avatar_key,requester_username,recipient_username,updated_at,created_at"
    )
    .eq("id", partnershipUuid)
    .or(`requester_avatar_key.eq.${slAvatarKey},recipient_avatar_key.eq.${slAvatarKey}`)
    .maybeSingle();

  if (error) throw new Error(`Failed to load selected partnership record: ${error.message}`);
  return data || null;
}

async function loadLatestAccessiblePartnership(slAvatarKey, status = null) {
  if (!slAvatarKey) return null;

  let query = supabase
    .schema("partner")
    .from("cultivation_partnerships")
    .select(
      "id,partnership_id,status,requester_avatar_key,recipient_avatar_key,requester_username,recipient_username,updated_at,created_at"
    )
    .or(`requester_avatar_key.eq.${slAvatarKey},recipient_avatar_key.eq.${slAvatarKey}`)
    .order("updated_at", { ascending: false })
    .limit(1);

  if (status) query = query.eq("status", status);

  const { data, error } = await query;

  if (error) throw new Error(`Failed to load partnership fallback record: ${error.message}`);
  if (!Array.isArray(data) || !data.length) return null;

  return data[0];
}

async function loadPartnerMemberByAvatarKey(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select(
      "id,member_id,sl_avatar_key,sl_username,v2_cultivation_status,v2_cultivation_started_at,last_presence_at,last_hud_sync_at,current_region_name,current_position_x,current_position_y,current_position_z,updated_at"
    )
    .eq("sl_avatar_key", slAvatarKey)
    .maybeSingle();

  if (error) throw new Error(`Failed to load partner member state: ${error.message}`);
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

  if (error) throw new Error(`Failed to load partner bond: ${error.message}`);
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

  if (error) throw new Error(`Failed to load partner bond member book states: ${error.message}`);
  return Array.isArray(data) ? data : [];
}

async function loadRealmStageProgression(realmKey, realmStageKey) {
  const normalizedRealmKey = normalizeRealmKeyForDb(realmKey);
  const normalizedRealmStageKey = normalizeRealmStageKeyForDb(realmStageKey);

  const { data, error } = await supabase
    .from("cultivation_realm_stage_progression")
    .select(
      "realm_key,realm_stage_key,auric_maximum,normal_gain_per_minute,vestiges_maximum,normal_vestiges_gain_per_minute"
    )
    .eq("realm_key", normalizedRealmKey)
    .eq("realm_stage_key", normalizedRealmStageKey)
    .maybeSingle();

  if (error) throw new Error(`Failed to load realm/stage progression: ${error.message}`);
  if (data) return data;

  const { data: fallback, error: fallbackError } = await supabase
    .from("cultivation_realm_stage_progression")
    .select(
      "realm_key,realm_stage_key,auric_maximum,normal_gain_per_minute,vestiges_maximum,normal_vestiges_gain_per_minute"
    )
    .eq("realm_key", "mortal")
    .eq("realm_stage_key", "base")
    .maybeSingle();

  if (fallbackError) {
    throw new Error(`Failed to load fallback realm/stage progression: ${fallbackError.message}`);
  }

  return fallback || null;
}

// =========================================================
// V2 CULTIVATION STATE LOADER
// =========================================================

async function loadV2CultivationState(slAvatarKey, member) {
  if (!slAvatarKey) {
    return buildEmptyV2CultivationState();
  }

  const activeStageKey = safeText(member?.v2_active_stage_key);
  const v2Status = safeText(member?.v2_cultivation_status, "idle");
  const gateOpen = Boolean(member?.v2_breakthrough_gate_open);
  const needsRepair = Boolean(member?.v2_stage_needs_repair);
  const accumulatedSeconds = safeNumber(member?.v2_accumulated_seconds, 0);

  let stageState = null;
  let breakthroughState = null;

  if (activeStageKey) {
    const [volumeStr, sectionStr] = activeStageKey.split(":");
    const volumeNumber = safeNumber(volumeStr, 0);
    const sectionKey = safeLower(sectionStr);

    if (volumeNumber > 0 && VALID_SECTIONS.includes(sectionKey)) {
      try {
        const { data, error } = await librarySupabase
          .from("v2_member_stage_state")
          .select("*")
          .eq("sl_avatar_key", slAvatarKey)
          .eq("volume_number", volumeNumber)
          .eq("section_key", sectionKey)
          .maybeSingle();

        if (!error && data) stageState = data;
      } catch (e) {
        console.error("load-member-state v2 stage state load error:", e);
      }
    }
  }

  try {
    const { data, error } = await supabase
      .schema("breakthrough")
      .from("v2_member_breakthrough_state")
      .select(
        "id,lifecycle_status,from_volume_number,from_section_key,to_volume_number,to_section_key,target_type,tribulation_family,breakthrough_started_at,breakthrough_ends_at,breakthrough_elapsed_at,breakthrough_duration_seconds,seconds_remaining,progress_pct,battle_status,outcome,verdict_key,verdict_text,verdict_revealed_at,stage_damaged,cooldown_active,cooldown_ends_at,total_attempts,total_failures,created_at,updated_at"
      )
      .eq("sl_avatar_key", slAvatarKey)
      .not("lifecycle_status", "in", '("success","failed_stable","failed_damaged","abandoned")')
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!error && data) breakthroughState = data;
  } catch (e) {
    console.error("load-member-state v2 breakthrough state load error:", e);
  }

  return buildV2CultivationStateSummary({
    member,
    stageState,
    breakthroughState,
    v2Status,
    gateOpen,
    needsRepair,
    accumulatedSeconds
  });
}

function buildEmptyV2CultivationState() {
  return {
    v2_active: false,
    v2_cultivation_status: "idle",
    v2_active_stage_key: null,
    v2_breakthrough_gate_open: false,
    v2_stage_needs_repair: false,
    v2_stage_state: null,
    v2_breakthrough_state: null,
    v2_next_action: "none"
  };
}

function buildV2CultivationStateSummary({
  member,
  stageState,
  breakthroughState,
  v2Status,
  gateOpen,
  needsRepair,
  accumulatedSeconds
}) {
  const activeStageKey = safeText(member?.v2_active_stage_key);

  let stageProgress = null;
  if (stageState) {
    const required = safeNumber(stageState.required_seconds, 0);
    const accumulated = safeNumber(stageState.accumulated_seconds, 0);
    const progressPct = required > 0 ? Math.min(100, Math.round((accumulated / required) * 100)) : 0;

    stageProgress = {
      stage_state_id: stageState.id,
      volume_number: stageState.volume_number,
      section_key: stageState.section_key,
      stage_status: stageState.stage_status,
      required_seconds: required,
      accumulated_seconds: accumulated,
      progress_pct: progressPct,
      human_accumulated: formatDuration(accumulated),
      human_required: formatDuration(required),
      human_remaining: formatDuration(Math.max(0, required - accumulated)),
      session_started_at: stageState.session_started_at || null,
      paused_at: stageState.paused_at || null,
      opened_at: stageState.opened_at || null,
      needs_repair: Boolean(stageState.needs_repair),
      repair_cp_cost: safeNumber(stageState.repair_cp_cost, 0),
      repair_resume_from_seconds: toNumberOrNull(stageState.repair_resume_from_seconds),
      open_cp_cost_paid: safeNumber(stageState.open_cp_cost_paid, 0),
      cultivation_completed_at: stageState.cultivation_completed_at || null,
      comprehended_at: stageState.comprehended_at || null
    };
  }

  let breakthroughSummary = null;
  if (breakthroughState) {
    const lifecycle = safeText(breakthroughState.lifecycle_status, "pending");
    const timerStarted = Boolean(breakthroughState.breakthrough_started_at);
    const timerElapsed = timerStarted && breakthroughState.breakthrough_ends_at
      ? new Date(breakthroughState.breakthrough_ends_at).getTime() <= Date.now()
      : false;

    breakthroughSummary = {
      breakthrough_state_id: breakthroughState.id,
      lifecycle_status: lifecycle,
      from_volume_number: breakthroughState.from_volume_number,
      from_section_key: breakthroughState.from_section_key,
      to_volume_number: breakthroughState.to_volume_number,
      to_section_key: breakthroughState.to_section_key,
      target_type: breakthroughState.target_type,
      tribulation_family: breakthroughState.tribulation_family,
      timer_started: timerStarted,
      timer_elapsed: timerElapsed,
      breakthrough_started_at: breakthroughState.breakthrough_started_at || null,
      breakthrough_ends_at: breakthroughState.breakthrough_ends_at || null,
      breakthrough_duration_seconds: safeNumber(breakthroughState.breakthrough_duration_seconds, 0),
      seconds_remaining: safeNumber(breakthroughState.seconds_remaining, 0),
      progress_pct: safeNumber(breakthroughState.progress_pct, 0),
      battle_status: safeText(breakthroughState.battle_status, "not_started"),
      outcome: safeText(breakthroughState.outcome) || null,
      verdict_key: safeText(breakthroughState.verdict_key) || null,
      verdict_text: safeText(breakthroughState.verdict_text) || null,
      verdict_revealed_at: breakthroughState.verdict_revealed_at || null,
      stage_damaged: Boolean(breakthroughState.stage_damaged),
      cooldown_active: Boolean(breakthroughState.cooldown_active),
      cooldown_ends_at: breakthroughState.cooldown_ends_at || null,
      total_attempts: safeNumber(breakthroughState.total_attempts, 0),
      total_failures: safeNumber(breakthroughState.total_failures, 0)
    };
  }

  let nextAction = "none";
  if (needsRepair) {
    nextAction = "repair_stage";
  } else if (v2Status === "idle" && activeStageKey && !gateOpen) {
    nextAction = "begin_cultivation";
  } else if (v2Status === "paused") {
    nextAction = "resume_cultivation";
  } else if (v2Status === "cultivating") {
    nextAction = "pause_cultivation";
  } else if (v2Status === "breakthrough_ready" && gateOpen && !breakthroughState) {
    nextAction = "enter_breakthrough";
  } else if (v2Status === "in_breakthrough" && breakthroughState) {
    const lifecycle = safeText(breakthroughState?.lifecycle_status);
    if (lifecycle === "pending") nextAction = "begin_breakthrough";
    else if (lifecycle === "active") nextAction = "wait_for_timer";
    else if (lifecycle === "timer_elapsed") nextAction = "resolve_battle";
    else if (lifecycle === "battle_resolved") nextAction = "reveal_verdict";
    else nextAction = "none";
  }

  return {
    v2_active: Boolean(activeStageKey),
    v2_cultivation_status: v2Status,
    v2_active_stage_key: activeStageKey || null,
    v2_breakthrough_gate_open: gateOpen,
    v2_stage_needs_repair: needsRepair,
    v2_stage_state: stageProgress,
    v2_breakthrough_state: breakthroughSummary,
    v2_next_action: nextAction
  };
}

// =========================================================
// MAIN HANDLER
// =========================================================

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return buildResponse(200, { ok: true });

  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use GET or POST."
    });
  }

  try {
    const body = parseBody(event);
    const query = event.queryStringParameters || {};
    const requestSource = { ...query, ...body };

    const sl_avatar_key = safeText(requestSource.sl_avatar_key);
    const sl_username = safeText(requestSource.sl_username);

    if (!sl_avatar_key) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: sl_avatar_key"
      });
    }

    const { data: member, error: memberError } = await supabase
      .from("cultivation_members")
      .select("*")
      .eq("sl_avatar_key", sl_avatar_key)
      .maybeSingle();

    if (memberError) {
      console.error("load-member-state member error:", memberError);
      return buildResponse(500, {
        success: false,
        message: "Failed to load member state.",
        error: memberError.message
      });
    }

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "No cultivation profile found for this SL avatar."
      });
    }

    if (
      sl_username &&
      safeText(member.sl_username) &&
      normalize(sl_username) !== normalize(member.sl_username)
    ) {
      return buildResponse(403, {
        success: false,
        message: "Username does not match cultivation profile."
      });
    }

    const nowIso = new Date().toISOString();
    let memberForResponse = member;

    // -------------------------------------------------------
    // V2 CULTIVATION STATE
    // -------------------------------------------------------
    let v2CultivationState = buildEmptyV2CultivationState();
    try {
      v2CultivationState = await loadV2CultivationState(sl_avatar_key, memberForResponse);
    } catch (v2LoadError) {
      console.error("load-member-state v2 cultivation load error:", v2LoadError);
    }

    // -------------------------------------------------------
    // REALM META
    // -------------------------------------------------------
    const realmMeta = getCanonicalRealmMeta(memberForResponse);
    const v2StageKey = safeText(memberForResponse.v2_active_stage_key);
    const realmStageKey = v2StageKey ? (v2StageKey.split(":")[1] || "base") : "base";
    const realmStageLabel = toTitle(realmStageKey, "Base");

    let progressionRow = null;
    try {
      progressionRow = await loadRealmStageProgression(realmMeta.realm_key, realmStageKey);
    } catch (e) {
      console.error("load-member-state progression load error:", e);
    }

    // -------------------------------------------------------
    // PARTNERSHIP / BOND
    // -------------------------------------------------------
    let selectedPartnershipUuid = null;
    let selectedPartnership = null;
    let resolvedPartnership = null;
    let partnerMember = null;
    let partnerBond = null;
    let bondRuntime = null;
    let bondMemberBookRows = [];

    try {
      selectedPartnershipUuid = await loadSelectedPartnershipUuid(getMemberPrimaryId(memberForResponse));

      if (selectedPartnershipUuid) {
        selectedPartnership = await loadPartnershipByUuidForMember(
          selectedPartnershipUuid,
          memberForResponse.sl_avatar_key
        );
      }

      if (!selectedPartnership) {
        const fallbackPartnership =
          (await loadLatestAccessiblePartnership(memberForResponse.sl_avatar_key, "active")) ||
          (await loadLatestAccessiblePartnership(memberForResponse.sl_avatar_key, null));
        resolvedPartnership = fallbackPartnership || null;
      } else {
        resolvedPartnership = selectedPartnership;
      }

      if (resolvedPartnership) {
        const counterpart = buildCounterpartSummary(
          resolvedPartnership,
          memberForResponse.sl_avatar_key,
          null
        );

        if (counterpart.partner_avatar_key) {
          partnerMember = await loadPartnerMemberByAvatarKey(counterpart.partner_avatar_key);
        }

        if (safeLower(resolvedPartnership.status) === "active") {
          partnerBond = await loadPartnerBondByContext(resolvedPartnership);
          bondMemberBookRows = await loadPartnerBondMemberBookRowsByContext(resolvedPartnership);
          bondRuntime = deriveBondRuntimeFromMemberBooks({
            currentMember: memberForResponse,
            partnerMember,
            memberBookRows: bondMemberBookRows
          });
        }
      }
    } catch (partnershipLoadError) {
      console.error("load-member-state partnership/bond load error:", partnershipLoadError);
    }

    // -------------------------------------------------------
    // ALIGNMENT
    // -------------------------------------------------------
    let alignmentDashboardState = null;
    let alignmentMeditationPreview = null;

    const resolvedMeditationMode = getMemberCultivationMode(memberForResponse, bondRuntime);

    try {
      const currentMemberId = getMemberPrimaryId(memberForResponse);

      if (currentMemberId) {
        alignmentDashboardState = await loadMemberAlignmentDashboardState(
          getMemberPrimaryId(memberForResponse),
          memberForResponse.sl_avatar_key
        );

        alignmentMeditationPreview = await getLiveMemberMeditationPreview(
          getMemberPrimaryId(memberForResponse),
          memberForResponse.sl_avatar_key
        );
      }
    } catch (alignmentLoadError) {
      console.error("load-member-state alignment load/apply error:", alignmentLoadError);
    }

    const alignmentState = buildAlignmentStateSummary({
      dashboardState: alignmentDashboardState,
      previewState: alignmentMeditationPreview,
      applyState: null
    });

    // -------------------------------------------------------
    // FINAL ASSEMBLY
    // -------------------------------------------------------
    const counterpartSummary = buildCounterpartSummary(
      resolvedPartnership,
      memberForResponse.sl_avatar_key,
      partnerMember
    );

    const bondTitle = safeText(
      pickFirst(
        partnerBond?.current_stage_name,
        partnerBond?.bond_stage_name,
        partnerBond?.stage_name
      )
    );

    const rawBondPercent = Number(
      pickFirst(
        partnerBond?.bond_percent,
        partnerBond?.current_percent,
        partnerBond?.progress_percent,
        0
      )
    );
    const bondPercent = Number.isFinite(rawBondPercent) ? rawBondPercent : 0;
    const bondStatus = safeText(
      pickFirst(bondRuntime?.status, bondRuntime?.session_status, partnerBond?.status),
      "idle"
    );

    const auricCurrent = safeNumber(memberForResponse.auric_current, 0);
    const auricMaximum = safeNumber(memberForResponse.auric_maximum, 0);
    const auricDrainPerMinute = 0;
    const minimumQiRequired = 0;

    const cultivationPointsCurrent = safeNumber(memberForResponse.vestiges, 0);
    const cultivationPointsMaximum = Math.max(
      cultivationPointsCurrent,
      safeNumber(progressionRow?.vestiges_maximum, 0)
    );
    const cultivationPointsRemaining = Math.max(
      0,
      cultivationPointsMaximum - cultivationPointsCurrent
    );
    const cultivationPointsCapped =
      cultivationPointsMaximum > 0 &&
      cultivationPointsCurrent >= cultivationPointsMaximum;

    const normalCpGainPerMinute = safeNumber(
      progressionRow?.normal_vestiges_gain_per_minute,
      0
    );
    const normalQiGainPerMinute = safeNumber(
      progressionRow?.normal_gain_per_minute,
      0
    );

    const formattedRealmName = safeText(
      finalizeRealmNameValue(safeText(memberForResponse.realm_name, realmMeta.realm_name))
    );

    const fullCultivationStage =
      realmStageLabel && formattedRealmName
        ? `${realmStageLabel} ${formattedRealmName}`
        : safeText(memberForResponse.realm_display_name, realmMeta.realm_display_name);

    const finalMember = buildMemberWithLiveSession(memberForResponse, nowIso, bondRuntime);

    const focusedPartnershipUuid =
      safeText(selectedPartnership?.id || selectedPartnershipUuid || resolvedPartnership?.id) || null;

    const baseResponse = {
      success: true,
      message: "Member state loaded successfully.",

      sl_avatar_key: safeText(finalMember.sl_avatar_key),
      sl_username: safeText(finalMember.sl_username),

      realm_index: safeNumber(realmMeta.realm_index, 1),
      realm_key: safeText(realmMeta.realm_key),
      realm_name: safeText(realmMeta.realm_name),
      realm_display_name: safeText(realmMeta.realm_display_name),
      realm_stage_key: realmStageKey,
      realm_stage_label: realmStageLabel,
      cultivation_stage: fullCultivationStage,

      auric_current: auricCurrent,
      auric_maximum: auricMaximum,
      auric_drain_per_minute: auricDrainPerMinute,
      minimum_qi_required: minimumQiRequired,

      vestiges: cultivationPointsCurrent,
      vestiges_maximum: cultivationPointsMaximum,
      vestiges_remaining: cultivationPointsRemaining,
      vestiges_capped: Boolean(cultivationPointsCapped),

      normal_qi_gain_per_minute: normalQiGainPerMinute,
      resonance_qi_gain_per_minute: normalQiGainPerMinute > 0 ? normalQiGainPerMinute * 2 : 0,
      normal_vestiges_gain_per_minute: normalCpGainPerMinute,
      resonance_vestiges_gain_per_minute: normalCpGainPerMinute > 0 ? normalCpGainPerMinute * 2 : 0,

      v2_cultivation_status: safeText(finalMember.v2_cultivation_status, "idle"),
      v2_active_stage_key: safeText(finalMember.v2_active_stage_key) || null,
      v2_breakthrough_gate_open: Boolean(finalMember.v2_breakthrough_gate_open),
      v2_accumulated_seconds: safeNumber(finalMember.v2_accumulated_seconds, 0),
      v2_cultivation_started_at: finalMember.v2_cultivation_started_at || null,
      v2_sessions_today: safeNumber(finalMember.v2_sessions_today, 0),
      cultivation_mode: safeText(getMemberCultivationMode(finalMember, bondRuntime), "idle"),

      v2_cultivation: v2CultivationState,

      focused_partnership_uuid: focusedPartnershipUuid,
      selected_partnership_uuid: safeText(selectedPartnership?.id || selectedPartnershipUuid) || null,
      selected_partnership_id: safeText(selectedPartnership?.partnership_id) || null,
      partnership_uuid: safeText(resolvedPartnership?.id) || null,
      partnership_id: safeText(resolvedPartnership?.partnership_id) || null,
      partnership_status: safeText(resolvedPartnership?.status) || null,

      partner_avatar_key: counterpartSummary.partner_avatar_key,
      partner_username: counterpartSummary.partner_username,
      partner_member_id: counterpartSummary.partner_member_id,
      partner_is_online: Boolean(counterpartSummary.partner_is_online),
      partner_last_presence_at: counterpartSummary.partner_last_presence_at,
      partner_last_hud_sync_at: counterpartSummary.partner_last_hud_sync_at,
      partner_meditation_active: Boolean(counterpartSummary.partner_meditation_active),
      partner_cultivation_mode: safeText(counterpartSummary.partner_cultivation_mode, "idle"),
      partner_current_region_name: safeText(counterpartSummary.partner_current_region_name),
      partner_current_position_x: toNumberOrNull(counterpartSummary.partner_current_position_x),
      partner_current_position_y: toNumberOrNull(counterpartSummary.partner_current_position_y),
      partner_current_position_z: toNumberOrNull(counterpartSummary.partner_current_position_z),
      partnership_relation_role: safeText(counterpartSummary.relation_role) || null,

      bond_title: safeText(bondTitle) || null,
      bond_stage_name: safeText(bondTitle) || null,
      bond_title_unlocked: safeText(bondTitle) !== "",
      bond_percent: bondPercent,
      bond_status: bondStatus,
      bond_partnership_uuid: safeText(resolvedPartnership?.id) || null,
      bond_partnership_id: safeText(resolvedPartnership?.partnership_id) || null,
      bond_runtime_active: ["active", "paused", "waiting_for_partner_start"].includes(bondStatus),
      bond_volume_number: safeNumber(
        pickFirst(
          bondRuntime?.current_member_book_state?.bond_volume_number,
          bondRuntime?.partner_member_book_state?.bond_volume_number,
          0
        ),
        0
      ),
      bond_book_number: safeNumber(
        pickFirst(
          bondRuntime?.current_member_book_state?.bond_book_number,
          bondRuntime?.partner_member_book_state?.bond_book_number,
          0
        ),
        0
      ),

      alignment_path_key: safeText(alignmentState.path_key, "unaligned"),
      alignment_path_name: safeText(alignmentState.path_name, "Unaligned"),
      alignment_path_state: safeText(alignmentState.path_state, "unaligned"),
      alignment_path_revealed: Boolean(alignmentState.path_revealed),
      alignment_yin_total: safeNumber(alignmentState.yin_total, 0),
      alignment_yang_total: safeNumber(alignmentState.yang_total, 0),
      alignment_balance_total: safeNumber(alignmentState.balance_total, 0),
      alignment_hour_group: safeText(alignmentState.hour_group) || null,
      alignment_phase_name: safeText(alignmentState.phase_name) || null,
      alignment_force_name: safeText(alignmentState.force_name) || null,
      alignment_phenomenon_name: safeText(alignmentState.phenomenon_name) || null,
      alignment_effective_bias: safeText(alignmentState.effective_bias) || null,
      alignment_qi_multiplier: safeNumber(alignmentState.qi_multiplier, 1),
      alignment_cp_multiplier: safeNumber(alignmentState.cp_multiplier, 1),
      alignment_aligned_bonus_available: Boolean(alignmentState.aligned_bonus_available),
      alignment_aligned_bonus_window_active: Boolean(alignmentState.aligned_bonus_window_active),
      alignment_conversion_target_path_key: safeText(alignmentState.conversion_target_path_key) || null,
      alignment_conversion_target_path_name: safeText(alignmentState.conversion_target_path_name) || null,
      alignment_conversion_cost_per_minute: safeNumber(alignmentState.conversion_cost_per_minute, 0),
      alignment_last_applied_at: alignmentState.last_applied_at || null,
      alignment_state: alignmentState,

      current_region_name: safeText(finalMember.current_region_name),
      current_position_x: toNumberOrNull(finalMember.current_position_x),
      current_position_y: toNumberOrNull(finalMember.current_position_y),
      current_position_z: toNumberOrNull(finalMember.current_position_z),
      last_presence_at: finalMember.last_presence_at || null,
      last_hud_sync_at: finalMember.last_hud_sync_at || null,
      updated_at: finalMember.updated_at || null
    };

    return buildResponse(200, baseResponse);
  } catch (error) {
    console.error("load-member-state server error:", error);
    return buildResponse(500, {
      success: false,
      message: "Server error",
      error: error.message
    });
  }
};