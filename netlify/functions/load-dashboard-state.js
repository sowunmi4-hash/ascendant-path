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

const COMPLETED_SECTION_STATUSES = new Set([
  "completed",
  "comprehended"
]);

const SECTION_ORDER_MAP = {
  base: 1,
  early: 2,
  middle: 3,
  late: 4
};

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
  2: { realm_key: "qi_gathering", realm_name: "auric gathering", realm_display_name: "Auric Gathering Realm" },
  3: { realm_key: "foundation", realm_name: "foundation", realm_display_name: "Foundation Realm" },
  4: { realm_key: "core_formation", realm_name: "core formation", realm_display_name: "Core Formation Realm" },
  5: { realm_key: "nascent_soul", realm_name: "nascent soul", realm_display_name: "Nascent Soul Realm" },
  6: { realm_key: "soul_transformation", realm_name: "soul transformation", realm_display_name: "Soul Transformation Realm" },
  7: { realm_key: "void_refinement", realm_name: "void refinement", realm_display_name: "Void Refinement Realm" },
  8: { realm_key: "body_integration", realm_name: "body integration", realm_display_name: "Body Integration Realm" },
  9: { realm_key: "mahayana", realm_name: "mahayana", realm_display_name: "Mahayana Realm" },
  10: { realm_key: "tribulation", realm_name: "tribulation", realm_display_name: "Tribulation Realm" }
};

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

    const payloadKey = JSON.stringify(sortedPayload);
    if (!seen.has(payloadKey)) {
      seen.add(payloadKey);
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

function isCompletedSectionStatus(status) {
  return COMPLETED_SECTION_STATUSES.has(safeLower(status));
}

function getRealmIndexFromValue(value) {
  const normalized = safeLower(value);
  if (!normalized) return 0;
  if (REALM_INDEX_MAP[normalized]) return REALM_INDEX_MAP[normalized];

  const trimmedRealm = normalized.replace(/\s+realm$/, "");
  return REALM_INDEX_MAP[trimmedRealm] || 0;
}

function formatRealmNameFromInternal(realmName) {
  const clean = safeText(realmName);
  if (!clean) return "";

  const titled = clean
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");

  return titled ? `${titled} Realm` : "";
}

function buildRealmMetaFromIndexAndName(realmIndex, realmName) {
  const catalog = REALM_META_BY_INDEX[realmIndex] || REALM_META_BY_INDEX[1];
  const cleanRealmName = safeText(realmName, catalog.realm_name);

  return {
    realm_index: realmIndex,
    realm_key: catalog.realm_key,
    realm_name: cleanRealmName,
    realm_display_name:
      formatRealmNameFromInternal(cleanRealmName) || catalog.realm_display_name
  };
}

function resolveAttainedRealmStateFromLibraryRows(rows, fallbackMember) {
  let best = null;

  for (const row of rows || []) {
    const realmIndex = getRealmIndexFromValue(row?.realm_name);
    if (!realmIndex) continue;

    for (const sectionKey of VALID_SECTIONS) {
      if (!isCompletedSectionStatus(row?.[`${sectionKey}_status`])) continue;

      if (
        !best ||
        realmIndex > best.realm_index ||
        (
          realmIndex === best.realm_index &&
          (SECTION_ORDER_MAP[sectionKey] || 0) > (SECTION_ORDER_MAP[best.realm_stage_key] || 0)
        )
      ) {
        best = {
          realm_index: realmIndex,
          realm_name: safeText(row?.realm_name),
          realm_stage_key: sectionKey
        };
      }
    }
  }

  if (best) {
    const meta = buildRealmMetaFromIndexAndName(best.realm_index, best.realm_name);

    return {
      ...meta,
      realm_stage_key: best.realm_stage_key,
      realm_stage_label: toTitle(best.realm_stage_key, "Base"),
      source: "library_completed"
    };
  }

  const fallbackRealmIndex = safeNumber(fallbackMember?.realm_index, 1);
  const fallbackCatalog = REALM_META_BY_INDEX[fallbackRealmIndex] || REALM_META_BY_INDEX[1];

  const v2FallbackStageKey = safeText(fallbackMember?.v2_active_stage_key);
  let fallbackStageKey = "base";
  if (v2FallbackStageKey && v2FallbackStageKey.includes(":")) {
    const parsed = safeLower(v2FallbackStageKey.split(":")[1]);
    if (VALID_SECTIONS.includes(parsed)) {
      fallbackStageKey = parsed;
    }
  } else {
    const rawKey = safeLower(fallbackMember?.realm_stage_key, "base");
    if (VALID_SECTIONS.includes(rawKey)) {
      fallbackStageKey = rawKey;
    }
  }

  return {
    realm_index: fallbackRealmIndex,
    realm_key: safeText(fallbackMember?.realm_key, fallbackCatalog.realm_key),
    realm_name: safeText(fallbackMember?.realm_name, fallbackCatalog.realm_name),
    realm_display_name: safeText(
      fallbackMember?.realm_display_name,
      formatRealmNameFromInternal(fallbackMember?.realm_name) || fallbackCatalog.realm_display_name
    ),
    realm_stage_key: fallbackStageKey,
    realm_stage_label: toTitle(fallbackStageKey, "Base"),
    source: "member_fallback"
  };
}

function isPartnerActive(lastPresenceAt) {
  if (!lastPresenceAt) return false;

  const presenceMs = new Date(lastPresenceAt).getTime();
  if (!Number.isFinite(presenceMs)) return false;

  const ageSeconds = (Date.now() - presenceMs) / 1000;
  return ageSeconds >= 0 && ageSeconds <= PARTNER_ACTIVE_WINDOW_SECONDS;
}

function computeVesselMode(memberRow, focusActivePartnership, focusPartnerOnline, hybridSharedReady) {
  if (!focusActivePartnership) return "solo";

  const pathType = safeLower(memberRow?.path_type);

  if (pathType === "dual") return "dual";
  if (pathType === "hybrid") return hybridSharedReady ? "hybrid" : "solo";

  return "solo";
}

function computeLiveVesselState(memberRow, focusActivePartnership, focusPartnerOnline, hybridSharedReady) {
  if (!focusActivePartnership) return "solo";

  const pathType = safeLower(memberRow?.path_type);

  if (pathType === "dual") {
    return focusPartnerOnline ? "dual_active" : "dual_inactive";
  }

  if (pathType === "hybrid") {
    return hybridSharedReady ? "hybrid_linked" : "solo";
  }

  return "solo";
}

function buildRealmLine(vesselMode, partner1Realm, partner2Realm) {
  if ((vesselMode === "dual" || vesselMode === "hybrid") && partner2Realm) {
    return `Realms: ${partner1Realm} • ${partner2Realm}`;
  }

  return `Realm: ${partner1Realm}`;
}

function getPartnershipRole(partnership, currentAvatarKey) {
  if (!partnership) return "none";
  if (sameValue(partnership.requester_avatar_key, currentAvatarKey)) return "requester";
  if (sameValue(partnership.recipient_avatar_key, currentAvatarKey)) return "recipient";
  return "unknown";
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

function buildOverallPartnershipStatus({
  activePartnershipCount,
  pendingIncomingCount,
  pendingOutgoingCount
}) {
  if (activePartnershipCount > 0) return "active";
  if (pendingIncomingCount > 0) return "pending_incoming";
  if (pendingOutgoingCount > 0) return "pending_outgoing";
  return "none";
}

function sanitizePartnershipRow(partnership, currentAvatarKey) {
  if (!partnership) return null;

  return {
    partnership_uuid: safeText(partnership.id) || null,
    partnership_id: safeText(partnership.partnership_id) || null,
    status: safeText(partnership.status),
    requester_avatar_key: safeText(partnership.requester_avatar_key),
    requester_username: safeText(partnership.requester_username),
    recipient_avatar_key: safeText(partnership.recipient_avatar_key),
    recipient_username: safeText(partnership.recipient_username),
    created_at: partnership.created_at || null,
    accepted_at: partnership.accepted_at || null,
    rejected_at: partnership.rejected_at || null,
    removed_at: partnership.removed_at || null,
    updated_at: partnership.updated_at || null,
    role: getPartnershipRole(partnership, currentAvatarKey),
    requested_by_you: sameValue(partnership.requester_avatar_key, currentAvatarKey)
  };
}

function getStableRealmDisplayName(memberRow) {
  return safeText(
    pickFirst(
      memberRow?.realm_display_name,
      memberRow?.current_realm_display_name,
      formatRealmNameFromInternal(
        pickFirst(memberRow?.realm_name, memberRow?.current_realm_name)
      ),
      memberRow?.realm_name,
      memberRow?.current_realm_name
    ),
    "Mortal Realm"
  );
}

function getStableRealmStageKey(memberRow) {
  const v2Key = safeText(memberRow?.v2_active_stage_key);
  if (v2Key && v2Key.includes(":")) {
    return safeText(v2Key.split(":")[1], "base");
  }
  return safeText(memberRow?.realm_stage_key, "base");
}

function getStableRealmStageLabel(memberRow) {
  return toTitle(getStableRealmStageKey(memberRow), "Base");
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

function doPartnershipRowsMatch(rowA, rowB) {
  if (!rowA || !rowB) return false;

  const rowAUuid = safeText(rowA?.id);
  const rowBUuid = safeText(rowB?.id);

  if (rowAUuid && rowBUuid && sameValue(rowAUuid, rowBUuid)) {
    return true;
  }

  const rowALegacyId = safeText(rowA?.partnership_id);
  const rowBLegacyId = safeText(rowB?.partnership_id);

  if (rowALegacyId && rowBLegacyId && sameValue(rowALegacyId, rowBLegacyId)) {
    return true;
  }

  return false;
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

function deriveEffectiveCultivationMode(memberRow, effectiveBondStatus) {
  if (effectiveBondStatus === "active") return "bond_cultivation";
  if (effectiveBondStatus === "paused") return "bond_paused";

  const v2Status = safeLower(memberRow?.v2_cultivation_status);
  if (v2Status === "cultivating") return "meditating";
  if (v2Status === "in_breakthrough") return "in_breakthrough";
  if (v2Status) return v2Status;

  const memberMode = safeLower(memberRow?.cultivation_mode);
  if (memberMode) return memberMode;

  return "idle";
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
    auric_accumulated: safeNumber(
      pickFirst(row.auric_accumulated, row.shared_auric_accumulated, row.accumulated_qi),
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

function canonicalizeAlignmentKey(value, fallback = "") {
  const text = safeLower(value, fallback);

  if (!text) return fallback;

  if (text === "balance") return "taiji";
  if (text === "yin_to_balance") return "yin_to_taiji";
  if (text === "yang_to_balance") return "yang_to_taiji";
  if (text === "balance_to_yin") return "taiji_to_yin";
  if (text === "balance_to_yang") return "taiji_to_yang";

  return text;
}

function canonicalizeAlignmentName(value, fallbackKey = "") {
  const text = safeText(value);

  if (!text) return defaultPathNameFromKey(fallbackKey);

  const normalized = safeLower(text);

  if (normalized === "balance") return "Taiji";
  if (normalized === "balance path") return "Taiji Path";

  return text.replace(/\bBalance\b/g, "Taiji");
}

function defaultPathNameFromKey(pathKey) {
  const key = canonicalizeAlignmentKey(pathKey);

  if (key === "yin") return "Yin Path";
  if (key === "yang") return "Yang Path";
  if (key === "taiji") return "Taiji Path";

  return "Unaligned";
}

function buildAlignmentStateSummary({ dashboardState, previewState }) {
  const rawPathKey = pickFirst(
    dashboardState?.current_path_key,
    dashboardState?.revealed_path_key,
    dashboardState?.path_key,
    dashboardState?.dominant_path_key,
    previewState?.current_path_key,
    previewState?.revealed_path_key,
    previewState?.path_key,
    previewState?.dominant_path_key
  );

  const pathKey = canonicalizeAlignmentKey(rawPathKey, "unaligned");

  const pathName = canonicalizeAlignmentName(
    pickFirst(
      dashboardState?.current_path_name,
      dashboardState?.revealed_path_name,
      dashboardState?.path_name,
      previewState?.current_path_name,
      previewState?.revealed_path_name,
      previewState?.path_name
    ),
    pathKey
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

  const conversionTargetPathKey = canonicalizeAlignmentKey(
    pickFirst(
      dashboardState?.conversion_target_path_key,
      dashboardState?.drift_target_path_key,
      previewState?.conversion_target_path_key,
      previewState?.drift_target_path_key
    )
  ) || null;

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
    taiji_total: safeNumber(
      pickFirst(
        dashboardState?.taiji_total,
        dashboardState?.balance_total,
        previewState?.taiji_total,
        previewState?.balance_total
      ),
      0
    ),

    hour_group: canonicalizeAlignmentKey(
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

    effective_bias: canonicalizeAlignmentKey(
      pickFirst(dashboardState?.effective_bias, previewState?.effective_bias)
    ) || null,

    auric_multiplier: roundNumber(
      pickFirst(
        dashboardState?.auric_multiplier,
        dashboardState?.current_auric_multiplier,
        previewState?.auric_multiplier,
        previewState?.auric_reward_multiplier,
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

    conversion_target_path_key: conversionTargetPathKey,

    conversion_target_path_name: canonicalizeAlignmentName(
      pickFirst(
        dashboardState?.conversion_target_path_name,
        dashboardState?.drift_target_path_name,
        previewState?.conversion_target_path_name,
        previewState?.drift_target_path_name
      ),
      conversionTargetPathKey
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

async function loadMemberLibraryRows(slAvatarKey) {
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
    .order("updated_at", { ascending: false })
    .order("volume_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load member library rows: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

async function resolveActiveRealmBook(memberRow, syncToMember = false, preloadedRows = null) {
  const v2StageKey = safeText(memberRow?.v2_active_stage_key);
  const v2VolumeNumber = v2StageKey && v2StageKey.includes(":")
    ? safeNumber(v2StageKey.split(":")[0], 0)
    : 0;

  const fallback = {
    active_realm_volume_number: v2VolumeNumber || safeNumber(memberRow?.active_realm_volume_number, 0),
    active_realm_book_name: safeText(memberRow?.active_realm_book_name),
    active_section_key: safeText(memberRow?.current_section, "base")
  };

  try {
    const realmRows = Array.isArray(preloadedRows)
      ? preloadedRows
      : await loadMemberLibraryRows(safeText(memberRow?.sl_avatar_key));

    const relevantRow = pickRelevantRealmRow(realmRows);
    if (!relevantRow) return fallback;

    const resolved = {
      active_realm_volume_number: safeNumber(relevantRow.volume_number, 0),
      active_realm_book_name: safeText(relevantRow.item_name),
      active_section_key: safeText(relevantRow.current_section, "base")
    };

    return resolved;
  } catch (error) {
    console.error("Active realm book lookup error:", error);
    return fallback;
  }
}

async function loadWalletRow(slAvatarKey) {
  const { data, error } = await supabase
    .from("member_wallets")
    .select(`
      sl_avatar_key,
      sl_username,
      ascension_tokens_balance,
      total_tokens_credited,
      total_tokens_spent,
      created_at,
      updated_at
    `)
    .eq("sl_avatar_key", slAvatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Wallet lookup error: ${error.message}`);
  }

  return data || null;
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

function buildPartnerSummary(partnership, currentAvatarKey, partnerMemberRow) {
  const identity = getPartnerIdentityFromPartnership(partnership, currentAvatarKey);

  return {
    partnership_uuid: safeText(partnership?.id) || null,
    partnership_id: safeText(partnership?.partnership_id) || null,
    partnership_status: safeText(partnership?.status) || null,

    partner_avatar_key: safeText(identity.avatarKey) || null,
    partner_username: safeText(
      partnerMemberRow?.sl_username,
      identity.username
    ) || null,
    partner_character_name: safeText(partnerMemberRow?.character_name, "Unnamed Cultivator"),
    partner_member_id: safeText(
      pickFirst(partnerMemberRow?.member_id, partnerMemberRow?.id)
    ) || null,

    partner_is_online: Boolean(partnerMemberRow && isPartnerActive(partnerMemberRow.last_presence_at)),
    partner_last_presence_at: partnerMemberRow?.last_presence_at || null,
    partner_last_hud_sync_at: partnerMemberRow?.last_hud_sync_at || null,
    partner_meditation_active: safeLower(partnerMemberRow?.v2_cultivation_status) === "cultivating",
    partner_cultivation_mode: safeText(partnerMemberRow?.cultivation_mode, "idle"),
    partner_current_region_name: safeText(partnerMemberRow?.current_region_name),
    partner_current_position_x:
      partnerMemberRow?.current_position_x !== undefined && partnerMemberRow?.current_position_x !== null
        ? Number(partnerMemberRow.current_position_x)
        : null,
    partner_current_position_y:
      partnerMemberRow?.current_position_y !== undefined && partnerMemberRow?.current_position_y !== null
        ? Number(partnerMemberRow.current_position_y)
        : null,
    partner_current_position_z:
      partnerMemberRow?.current_position_z !== undefined && partnerMemberRow?.current_position_z !== null
        ? Number(partnerMemberRow.current_position_z)
        : null
  };
}

function buildMiniSummaryRows(rows, currentAvatarKey, partnerMap) {
  return (rows || []).map((row) => {
    const identity = getPartnerIdentityFromPartnership(row, currentAvatarKey);
    const partnerMemberRow = partnerMap[safeText(identity.avatarKey)] || null;

    return {
      partnership_uuid: safeText(row?.id) || null,
      partnership_id: safeText(row?.partnership_id) || null,
      status: safeText(row?.status),
      partner_avatar_key: safeText(identity.avatarKey) || null,
      partner_username: safeText(partnerMemberRow?.sl_username, identity.username) || null,
      partner_is_online: Boolean(partnerMemberRow && isPartnerActive(partnerMemberRow.last_presence_at)),
      updated_at: row?.updated_at || null
    };
  });
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
    const cookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = cookies[cookieName];

    if (!sessionToken) {
      return json(401, {
        success: false,
        error: "not_logged_in"
      });
    }

    const now = new Date().toISOString();

    const { data: sessionRow, error: sessionError } = await supabase
      .from("website_sessions")
      .select("*")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();

    if (sessionError) {
      console.error("Session lookup error:", sessionError);
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
      console.error("Member lookup error:", memberError);
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

    let memberLibraryRows = [];
    let memberAttainedRealmState = resolveAttainedRealmStateFromLibraryRows([], memberRow);

    try {
      memberLibraryRows = await loadMemberLibraryRows(memberRow.sl_avatar_key);
      memberAttainedRealmState = resolveAttainedRealmStateFromLibraryRows(
        memberLibraryRows,
        memberRow
      );
    } catch (memberLibraryError) {
      console.error("Dashboard member library load error:", memberLibraryError);
    }

    const memberActiveRealm = await resolveActiveRealmBook(memberRow, true, memberLibraryRows);
    const walletRow = await loadWalletRow(memberRow.sl_avatar_key);

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
      console.error("Selected partnership resolution error:", selectedError);
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

    const allCounterpartAvatarKeys = [
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
    ].filter(Boolean);

    const partnerMap = await loadMembersByAvatarKeys(allCounterpartAvatarKeys);

    const focusPartnerMemberRow =
      partnerMap[safeText(focusPartnerIdentity.avatarKey)] ||
      (focusPartnerIdentity.avatarKey
        ? await loadPartnerByAvatarKey(focusPartnerIdentity.avatarKey)
        : null);

    let focusPartnerLibraryRows = [];
    let focusPartnerAttainedRealmState = null;

    if (focusPartnerMemberRow) {
      try {
        focusPartnerLibraryRows = await loadMemberLibraryRows(focusPartnerMemberRow.sl_avatar_key);
        focusPartnerAttainedRealmState = resolveAttainedRealmStateFromLibraryRows(
          focusPartnerLibraryRows,
          focusPartnerMemberRow
        );
      } catch (focusPartnerLibraryError) {
        console.error("Dashboard focus partner library load error:", focusPartnerLibraryError);
        focusPartnerAttainedRealmState = resolveAttainedRealmStateFromLibraryRows(
          [],
          focusPartnerMemberRow
        );
      }
    }

    const focusPartnerOnline =
      Boolean(focusPartnerMemberRow) &&
      isPartnerActive(focusPartnerMemberRow.last_presence_at);

    let focusPartnerSelectedPartnershipUuid = null;
    let focusPartnerSelectedPartnershipRow = null;
    let focusPartnerFocusedPartnership = null;

    if (focusIsActive && focusPartnerMemberRow) {
      try {
        const focusPartnerPrimaryId = getMemberPrimaryId(focusPartnerMemberRow);
        const focusPartnerOpenPartnershipRows = await loadOpenPartnershipRows(
          focusPartnerMemberRow.sl_avatar_key
        );

        const {
          activeRows: focusPartnerActiveRows,
          pendingIncomingRows: focusPartnerPendingIncomingRows,
          pendingOutgoingRows: focusPartnerPendingOutgoingRows
        } = groupOpenPartnershipRows(
          focusPartnerOpenPartnershipRows,
          focusPartnerMemberRow.sl_avatar_key
        );

        focusPartnerSelectedPartnershipUuid = await loadSelectedPartnershipUuid(
          focusPartnerPrimaryId
        );

        if (focusPartnerSelectedPartnershipUuid) {
          focusPartnerSelectedPartnershipRow = await loadPartnershipByUuidForMember(
            focusPartnerSelectedPartnershipUuid,
            focusPartnerMemberRow.sl_avatar_key
          );
        }

        focusPartnerFocusedPartnership = resolveFocusPartnership({
          selectedPartnershipUuid: focusPartnerSelectedPartnershipUuid,
          selectedPartnership: focusPartnerSelectedPartnershipRow,
          openRows: focusPartnerOpenPartnershipRows,
          activeRows: focusPartnerActiveRows,
          pendingIncomingRows: focusPartnerPendingIncomingRows,
          pendingOutgoingRows: focusPartnerPendingOutgoingRows
        });
      } catch (focusPartnerSelectionError) {
        console.error("Focus partner focus resolution error:", focusPartnerSelectionError);
      }
    }

    const memberPathType = safeLower(memberRow?.path_type);

    const mutualFocusActive =
      Boolean(focusIsActive) &&
      Boolean(focusPartnerFocusedPartnership) &&
      doPartnershipRowsMatch(focusPartnership, focusPartnerFocusedPartnership);

    const hybridSharedReady =
      memberPathType === "hybrid" &&
      Boolean(focusIsActive) &&
      Boolean(focusPartnerOnline) &&
      Boolean(mutualFocusActive);

    const dualSharedReady =
      memberPathType === "dual" &&
      Boolean(focusIsActive) &&
      Boolean(safeText(focusPartnerIdentity.avatarKey));

    const resourceShareEnabled = dualSharedReady || hybridSharedReady;

    const resourceSharePartnerAvatarKey = resourceShareEnabled
      ? safeText(focusPartnerIdentity.avatarKey) || null
      : null;

    const dashboardPartnerActive =
      memberPathType === "hybrid"
        ? hybridSharedReady
        : Boolean(focusIsActive && focusPartnerOnline);

    const focusPartnerWalletRow = focusPartnerMemberRow
      ? await loadWalletRow(focusPartnerMemberRow.sl_avatar_key)
      : null;

    const focusPartnerActiveRealm = focusPartnerMemberRow
      ? await resolveActiveRealmBook(focusPartnerMemberRow, false, focusPartnerLibraryRows)
      : {
          active_realm_volume_number: 0,
          active_realm_book_name: "",
          active_section_key: "base"
        };

    let focusBondRow = null;
    let focusBondSessionRow = null;
    let focusBondMemberBookRows = [];

    if (focusIsActive) {
      try {
        focusBondRow = await loadPartnerBondByContext(focusPartnership);
      } catch (bondError) {
        console.error("Dashboard bond lookup error:", bondError);
      }

      try {
        focusBondMemberBookRows = await loadPartnerBondMemberBookRowsByContext(focusPartnership);
      } catch (bondBookError) {
        console.error("Dashboard bond member book lookup error:", bondBookError);
      }

      focusBondSessionRow = deriveDashboardBondSession({
        focusIsActive,
        currentMemberRow: memberRow,
        partnerMemberRow: focusPartnerMemberRow,
        memberBookRows: focusBondMemberBookRows
      });
    }

    await Promise.all([
      supabase
        .from("website_sessions")
        .update({ updated_at: now })
        .eq("session_token", sessionToken),
      supabase
        .from("cultivation_members")
        .update({ last_presence_at: now })
        .eq("sl_avatar_key", memberRow.sl_avatar_key)
    ]);

    const currentMemberTokens = safeNumber(walletRow?.ascension_tokens_balance, 0);
    const partnerTokens = safeNumber(focusPartnerWalletRow?.ascension_tokens_balance, 0);

    const activePartnerSummaries = buildMiniSummaryRows(
      activeRows,
      memberRow.sl_avatar_key,
      partnerMap
    );

    const pendingIncomingSummaries = buildMiniSummaryRows(
      pendingIncomingRows,
      memberRow.sl_avatar_key,
      partnerMap
    );

    const pendingOutgoingSummaries = buildMiniSummaryRows(
      pendingOutgoingRows,
      memberRow.sl_avatar_key,
      partnerMap
    );

    const activePartnershipCount = activeRows.length;
    const activePartnersOnlineCount = activePartnerSummaries.filter((row) => row.partner_is_online).length;
    const pendingIncomingCount = pendingIncomingRows.length;
    const pendingOutgoingCount = pendingOutgoingRows.length;

    const effectiveBondStatus = deriveEffectiveBondStatus({
      focusIsActive,
      bondRow: focusBondRow,
      bondSessionRow: focusBondSessionRow
    });

    const effectiveBondPauseReason = deriveEffectiveBondPauseReason({
      bondRow: focusBondRow,
      bondSessionRow: focusBondSessionRow
    });

    const effectiveCultivationMode = deriveEffectiveCultivationMode(
      memberRow,
      effectiveBondStatus
    );

    const bondRuntimeActive = effectiveBondStatus === "active";
    const bondRuntimePaused = effectiveBondStatus === "paused";
    const bondStateLabel = mapBondStateDisplayLabel(effectiveBondStatus);

    let vesselMode = 'single';

    if (memberRow.path_type === 'dual') {
      vesselMode = 'dual';
    } else if (memberRow.path_type === 'hybrid') {
      const { data: selectedPartnership } = await supabase
        .schema('partner')
        .from('member_selected_partnerships')
        .select('selected_partnership_id')
        .eq('member_id', memberRow.member_id)
        .maybeSingle();

      if (selectedPartnership?.selected_partnership_id) {
        const { data: partnership } = await supabase
          .schema('partner')
          .from('cultivation_partnerships')
          .select('requester_avatar_key, recipient_avatar_key, status')
          .eq('id', selectedPartnership.selected_partnership_id)
          .eq('status', 'active')
          .maybeSingle();

        if (partnership) {
          const partnerAvatarKey = partnership.requester_avatar_key === memberRow.sl_avatar_key
            ? partnership.recipient_avatar_key
            : partnership.requester_avatar_key;

          const { data: partnerMember } = await supabase
            .from('cultivation_members')
            .select('last_presence_at')
            .eq('sl_avatar_key', partnerAvatarKey)
            .maybeSingle();

          const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
          const partnerOnline = partnerMember?.last_presence_at &&
            new Date(partnerMember.last_presence_at) > fiveMinutesAgo;

          vesselMode = partnerOnline ? 'hybrid' : 'single';
        }
      }
    }

    const liveVesselState = computeLiveVesselState(
      memberRow,
      focusIsActive ? focusPartnership : null,
      focusPartnerOnline,
      hybridSharedReady
    );

    const partnershipStatus = buildOverallPartnershipStatus({
      activePartnershipCount,
      pendingIncomingCount,
      pendingOutgoingCount
    });

    const focusPartnerSummary = {
      ...buildPartnerSummary(
        focusPartnership,
        memberRow.sl_avatar_key,
        focusPartnerMemberRow
      ),
      mutual_focus_active: mutualFocusActive,
      hybrid_shared_ready: hybridSharedReady,
      partner_selected_partnership_uuid: safeText(focusPartnerSelectedPartnershipUuid) || null,
      partner_focused_partnership_uuid: safeText(focusPartnerFocusedPartnership?.id) || null,
      partner_focused_partnership_id: safeText(focusPartnerFocusedPartnership?.partnership_id) || null,
      attained_realm_index: safeNumber(focusPartnerAttainedRealmState?.realm_index, 0) || null,
      attained_realm_key: safeText(focusPartnerAttainedRealmState?.realm_key) || null,
      attained_realm_name: safeText(focusPartnerAttainedRealmState?.realm_name) || null,
      attained_realm_display_name: safeText(focusPartnerAttainedRealmState?.realm_display_name) || null,
      attained_realm_stage_key: safeText(focusPartnerAttainedRealmState?.realm_stage_key) || null,
      attained_realm_stage_label: safeText(focusPartnerAttainedRealmState?.realm_stage_label) || null
    };

    const partner1Realm = safeText(
      memberAttainedRealmState?.realm_display_name,
      "Mortal Realm"
    );

    const partner2Realm = safeText(
      focusPartnerAttainedRealmState?.realm_display_name,
      focusPartnerMemberRow ? getStableRealmDisplayName(focusPartnerMemberRow) : ""
    );

    const memberRealmStageKey = safeText(
      memberAttainedRealmState?.realm_stage_key,
      getStableRealmStageKey(memberRow)
    );

    const memberRealmStageLabel = safeText(
      memberAttainedRealmState?.realm_stage_label,
      getStableRealmStageLabel(memberRow)
    );

    const focusPartnerRealmStageKey = safeText(
      focusPartnerAttainedRealmState?.realm_stage_key,
      focusPartnerMemberRow ? getStableRealmStageKey(focusPartnerMemberRow) : "base"
    );

    const focusPartnerRealmStageLabel = safeText(
      focusPartnerAttainedRealmState?.realm_stage_label,
      focusPartnerMemberRow ? getStableRealmStageLabel(focusPartnerMemberRow) : "Base"
    );

    const memberStageKey = memberRow.v2_active_stage_key?.split(':')[1] || 'base';
    const { data: stageProgression } = await supabase
      .from("cultivation_realm_stage_progression")
      .select("auric_maximum, vestiges_maximum, normal_gain_per_minute")
      .eq("realm_key", memberRow.realm_key || "mortal")
      .eq("realm_stage_key", memberStageKey)
      .maybeSingle();

    let partnerStageProgression = null;
    if (focusPartnerMemberRow) {
      const partnerStageKey = focusPartnerMemberRow.v2_active_stage_key?.split(':')[1] || 'base';
      const { data: _psp } = await supabase
        .from("cultivation_realm_stage_progression")
        .select("auric_maximum, vestiges_maximum")
        .eq("realm_key", focusPartnerMemberRow.realm_key || "mortal")
        .eq("realm_stage_key", partnerStageKey)
        .maybeSingle();
      partnerStageProgression = _psp;
    }

    const personalAuricCurrent = safeNumber(memberRow.auric_current, 0);
    const personalAuricMaximum = safeNumber(stageProgression?.auric_maximum, safeNumber(memberRow.auric_maximum, 0));

    const partnerAuricCurrent = safeNumber(focusPartnerMemberRow?.auric_current, 0);
    const partnerAuricMaximum = safeNumber(partnerStageProgression?.auric_maximum, safeNumber(focusPartnerMemberRow?.auric_maximum, 0));

    const sharedAuricCurrent = resourceShareEnabled ? personalAuricCurrent + partnerAuricCurrent : personalAuricCurrent;
    const sharedAuricMaximum = resourceShareEnabled ? personalAuricMaximum + partnerAuricMaximum : personalAuricMaximum;
    const sharedQiPercent = sharedAuricMaximum > 0
      ? Math.round(Math.max(0, Math.min(100, (sharedAuricCurrent / sharedAuricMaximum) * 100)))
      : 0;

    const personalCultivationPoints = safeNumber(memberRow.vestiges, 0);
    const personalCultivationPointsMaximum = safeNumber(stageProgression?.vestiges_maximum, 0);
    const personalCultivationPointsRemaining = Math.max(0, personalCultivationPointsMaximum - personalCultivationPoints);
    const personalCultivationPointsCapped = personalCultivationPointsMaximum > 0 && personalCultivationPoints >= personalCultivationPointsMaximum;

    const partnerCultivationPoints = safeNumber(focusPartnerMemberRow?.vestiges, 0);
    const partnerCultivationPointsMaximum = safeNumber(partnerStageProgression?.vestiges_maximum, 0);
    const partnerCultivationPointsRemaining = Math.max(0, partnerCultivationPointsMaximum - partnerCultivationPoints);
    const partnerCultivationPointsCapped = partnerCultivationPointsMaximum > 0 && partnerCultivationPoints >= partnerCultivationPointsMaximum;

    const sharedCultivationPoints = resourceShareEnabled
      ? personalCultivationPoints + partnerCultivationPoints
      : personalCultivationPoints;
    const sharedCultivationPointsMaximum = resourceShareEnabled
      ? personalCultivationPointsMaximum + partnerCultivationPointsMaximum
      : personalCultivationPointsMaximum;
    const sharedCultivationPointsRemaining = Math.max(0, sharedCultivationPointsMaximum - sharedCultivationPoints);
    const sharedCultivationPointsCapped = sharedCultivationPointsMaximum > 0 && sharedCultivationPoints >= sharedCultivationPointsMaximum;

    const personalMortalEnergy = safeNumber(memberRow.mortal_energy, 0);
    const partnerMortalEnergy = safeNumber(focusPartnerMemberRow?.mortal_energy, 0);
    const sharedMortalEnergy = resourceShareEnabled ? personalMortalEnergy + partnerMortalEnergy : personalMortalEnergy;

    const sharedAscensionTokens = resourceShareEnabled
      ? currentMemberTokens + partnerTokens
      : currentMemberTokens;

    const realmLine = buildRealmLine(vesselMode, partner1Realm, partner2Realm);

    const currentCultivationFocus =
      memberActiveRealm.active_realm_volume_number > 0 && memberActiveRealm.active_realm_book_name
        ? `Volume ${memberActiveRealm.active_realm_volume_number} • ${memberActiveRealm.active_realm_book_name}`
        : safeText(memberActiveRealm.active_realm_book_name);

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

    const bondStatus = normalizeBondSessionStatus(effectiveBondStatus, "idle");
    const bondPauseReason = effectiveBondPauseReason;

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
      console.error("Dashboard alignment load error:", alignmentError);
    }

    const alignmentState = buildAlignmentStateSummary({
      dashboardState: alignmentDashboardState,
      previewState: alignmentMeditationPreview
    });

    return json(200, {
      success: true,

      member: {
        ...memberRow,
        sl_avatar_key: safeText(memberRow.sl_avatar_key),
        sl_username: safeText(memberRow.sl_username),
        member_id: safeText(memberRow.member_id),
        cultivation_mode: effectiveCultivationMode,
        live_practice_mode: effectiveCultivationMode,
        live_practice_state: effectiveCultivationMode,
        ascension_tokens_balance: currentMemberTokens,
        last_presence_at: now,

        realm_index: safeNumber(memberAttainedRealmState?.realm_index, safeNumber(memberRow?.realm_index, 1)),
        realm_key: safeText(memberAttainedRealmState?.realm_key, memberRow?.realm_key),
        realm_name: safeText(memberAttainedRealmState?.realm_name, memberRow?.realm_name),
        realm_display_name: partner1Realm,
        realm_stage_key: memberRealmStageKey,
        realm_stage_label: memberRealmStageLabel,

        attained_realm_index: safeNumber(memberAttainedRealmState?.realm_index, safeNumber(memberRow?.realm_index, 1)),
        attained_realm_key: safeText(memberAttainedRealmState?.realm_key, memberRow?.realm_key),
        attained_realm_name: safeText(memberAttainedRealmState?.realm_name, memberRow?.realm_name),
        attained_realm_display_name: partner1Realm,
        attained_realm_stage_key: memberRealmStageKey,
        attained_realm_stage_label: memberRealmStageLabel,
        attained_realm_source: safeText(memberAttainedRealmState?.source, "member_fallback"),

        active_realm_volume_number: memberActiveRealm.active_realm_volume_number,
        active_realm_book_name: memberActiveRealm.active_realm_book_name,
        current_cultivation_focus: currentCultivationFocus,

        gender: memberRow?.gender || 'male',
        path_type: memberRow?.path_type || 'single',
        vessel_mode: vesselMode,

        auric_current: personalAuricCurrent,
        auric_maximum: personalAuricMaximum,
        vestiges: personalCultivationPoints,
        vestiges_maximum: personalCultivationPointsMaximum,
        vestiges_remaining: personalCultivationPointsRemaining,
        vestiges_capped: personalCultivationPointsCapped,

        alignment_path_key: alignmentState.path_key,
        alignment_path_name: alignmentState.path_name,
        alignment_path_state: alignmentState.path_state,
        alignment_path_revealed: alignmentState.path_revealed,
        alignment_yin_total: alignmentState.yin_total,
        alignment_yang_total: alignmentState.yang_total,
        alignment_taiji_total: alignmentState.taiji_total,
        alignment_hour_group: alignmentState.hour_group,
        alignment_phase_name: alignmentState.phase_name,
        alignment_force_name: alignmentState.force_name,
        alignment_phenomenon_name: alignmentState.phenomenon_name,
        alignment_effective_bias: alignmentState.effective_bias,
        alignment_auric_multiplier: alignmentState.auric_multiplier,
        alignment_cp_multiplier: alignmentState.cp_multiplier,
        alignment_aligned_bonus_available: alignmentState.aligned_bonus_available,
        alignment_aligned_bonus_window_active: alignmentState.aligned_bonus_window_active,
        alignment_conversion_target_path_key: alignmentState.conversion_target_path_key,
        alignment_conversion_target_path_name: alignmentState.conversion_target_path_name,
        alignment_conversion_cost_per_minute: alignmentState.conversion_cost_per_minute
      },

      alignment: {
        preview_applies_to_live_meditation: alignmentPreviewAppliesToLiveMeditation,
        path_key: alignmentState.path_key,
        path_name: alignmentState.path_name,
        path_state: alignmentState.path_state,
        path_revealed: alignmentState.path_revealed,
        yin_total: alignmentState.yin_total,
        yang_total: alignmentState.yang_total,
        taiji_total: alignmentState.taiji_total,
        hour_group: alignmentState.hour_group,
        phase_name: alignmentState.phase_name,
        force_name: alignmentState.force_name,
        phenomenon_name: alignmentState.phenomenon_name,
        effective_bias: alignmentState.effective_bias,
        auric_multiplier: alignmentState.auric_multiplier,
        cp_multiplier: alignmentState.cp_multiplier,
        aligned_bonus_available: alignmentState.aligned_bonus_available,
        aligned_bonus_window_active: alignmentState.aligned_bonus_window_active,
        conversion_target_path_key: alignmentState.conversion_target_path_key,
        conversion_target_path_name: alignmentState.conversion_target_path_name,
        conversion_cost_per_minute: alignmentState.conversion_cost_per_minute
      },

      wallet: {
        currency_name: "Ascension Tokens",
        wallet_found: !!walletRow,
        ascension_tokens_balance: currentMemberTokens,
        total_tokens_credited: safeNumber(walletRow?.total_tokens_credited, 0),
        total_tokens_spent: safeNumber(walletRow?.total_tokens_spent, 0),
        sl_avatar_key: walletRow ? safeText(walletRow.sl_avatar_key) : safeText(memberRow.sl_avatar_key),
        sl_username: walletRow ? safeText(walletRow.sl_username) : safeText(memberRow.sl_username),
        created_at: walletRow?.created_at || null,
        updated_at: walletRow?.updated_at || null
      },

      selected_partnership_uuid: safeText(selectedPartnershipUuid) || null,
      selected_partnership: sanitizePartnershipRow(selectedPartnershipRow, memberRow.sl_avatar_key),

      partnership_status: partnershipStatus,
      has_partnership: activePartnershipCount > 0 || pendingIncomingCount > 0 || pendingOutgoingCount > 0,
      partner_active: dashboardPartnerActive,
      partner_online: Boolean(focusIsActive && focusPartnerOnline),
      mutual_focus_active: mutualFocusActive,
      hybrid_shared_ready: hybridSharedReady,
      resource_share_enabled: resourceShareEnabled,
      active_window_seconds: PARTNER_ACTIVE_WINDOW_SECONDS,
      vessel_mode: vesselMode,
      live_vessel_state: liveVesselState,
      bond_active: bondRuntimeActive,

      partnership: sanitizePartnershipRow(focusPartnership, memberRow.sl_avatar_key),
      incoming_request: sanitizePartnershipRow(
        pendingIncomingRows[0] || null,
        memberRow.sl_avatar_key
      ),
      outgoing_request: sanitizePartnershipRow(
        pendingOutgoingRows[0] || null,
        memberRow.sl_avatar_key
      ),

      partner: focusPartnerMemberRow
        ? {
            ...focusPartnerMemberRow,
            sl_avatar_key: safeText(focusPartnerMemberRow.sl_avatar_key),
            sl_username: safeText(focusPartnerMemberRow.sl_username),
            member_id: safeText(focusPartnerMemberRow.member_id),
            ascension_tokens_balance: partnerTokens,

            realm_index: safeNumber(focusPartnerAttainedRealmState?.realm_index, safeNumber(focusPartnerMemberRow?.realm_index, 1)),
            realm_key: safeText(focusPartnerAttainedRealmState?.realm_key, focusPartnerMemberRow?.realm_key),
            realm_name: safeText(focusPartnerAttainedRealmState?.realm_name, focusPartnerMemberRow?.realm_name),
            realm_display_name: partner2Realm,
            realm_stage_key: focusPartnerRealmStageKey,
            realm_stage_label: focusPartnerRealmStageLabel,

            attained_realm_index: safeNumber(focusPartnerAttainedRealmState?.realm_index, safeNumber(focusPartnerMemberRow?.realm_index, 1)),
            attained_realm_key: safeText(focusPartnerAttainedRealmState?.realm_key, focusPartnerMemberRow?.realm_key),
            attained_realm_name: safeText(focusPartnerAttainedRealmState?.realm_name, focusPartnerMemberRow?.realm_name),
            attained_realm_display_name: partner2Realm,
            attained_realm_stage_key: focusPartnerRealmStageKey,
            attained_realm_stage_label: focusPartnerRealmStageLabel,
            attained_realm_source: safeText(focusPartnerAttainedRealmState?.source, "member_fallback"),

            active_realm_volume_number: focusPartnerActiveRealm.active_realm_volume_number,
            active_realm_book_name: focusPartnerActiveRealm.active_realm_book_name,

            auric_current: partnerAuricCurrent,
            auric_maximum: partnerAuricMaximum,
            vestiges: partnerCultivationPoints,
            vestiges_maximum: partnerCultivationPointsMaximum,
            vestiges_remaining: partnerCultivationPointsRemaining,
            vestiges_capped: partnerCultivationPointsCapped,
            partner_selected_partnership_uuid: safeText(focusPartnerSelectedPartnershipUuid) || null,
            partner_focused_partnership_uuid: safeText(focusPartnerFocusedPartnership?.id) || null,
            partner_focused_partnership_id: safeText(focusPartnerFocusedPartnership?.partnership_id) || null,
            mutual_focus_active: mutualFocusActive
          }
        : {
            sl_avatar_key: safeText(focusPartnerIdentity.avatarKey),
            sl_username: safeText(focusPartnerIdentity.username),
            member_id: "",
            realm_display_name: "",
            ascension_tokens_balance: 0,
            auric_current: 0,
            auric_maximum: 0,
            vestiges: 0,
            vestiges_maximum: 0,
            vestiges_remaining: 0,
            vestiges_capped: false,
            partner_selected_partnership_uuid: safeText(focusPartnerSelectedPartnershipUuid) || null,
            partner_focused_partnership_uuid: safeText(focusPartnerFocusedPartnership?.id) || null,
            partner_focused_partnership_id: safeText(focusPartnerFocusedPartnership?.partnership_id) || null,
            mutual_focus_active: mutualFocusActive
          },

      partner_1: {
        sl_avatar_key: safeText(memberRow.sl_avatar_key),
        sl_username: safeText(memberRow.sl_username),
        member_id: safeText(memberRow.member_id),
        character_name: safeText(memberRow.character_name, "Unnamed Cultivator"),
        realm_display_name: partner1Realm,
        realm_stage_key: memberRealmStageKey,
        realm_stage_label: memberRealmStageLabel,
        vestiges: personalCultivationPoints,
        vestiges_maximum: personalCultivationPointsMaximum,
        ascension_tokens_balance: currentMemberTokens,
        mortal_energy: personalMortalEnergy,
        auric_current: personalAuricCurrent,
        auric_maximum: personalAuricMaximum
      },

      partner_2: {
        sl_avatar_key: safeText(focusPartnerMemberRow?.sl_avatar_key),
        sl_username: safeText(focusPartnerMemberRow?.sl_username, focusPartnerIdentity.username),
        member_id: safeText(focusPartnerMemberRow?.member_id),
        character_name: safeText(focusPartnerMemberRow?.character_name, "Unnamed Cultivator"),
        realm_display_name: partner2Realm,
        realm_stage_key: focusPartnerRealmStageKey,
        realm_stage_label: focusPartnerRealmStageLabel,
        vestiges: partnerCultivationPoints,
        vestiges_maximum: partnerCultivationPointsMaximum,
        ascension_tokens_balance: partnerTokens,
        mortal_energy: partnerMortalEnergy,
        auric_current: partnerAuricCurrent,
        auric_maximum: partnerAuricMaximum
      },

      computed: {
        vessel_mode: vesselMode,
        live_vessel_state: liveVesselState,
        bond_active: bondRuntimeActive,
        bond_paused: bondRuntimePaused,
        partner_active: dashboardPartnerActive,
        partner_online: Boolean(focusIsActive && focusPartnerOnline),
        mutual_focus_active: mutualFocusActive,
        hybrid_shared_ready: hybridSharedReady,
        resource_share_enabled: resourceShareEnabled,
        realm_line: realmLine,

        partner_1_realm: partner1Realm,
        partner_2_realm: partner2Realm,
        partner_1_realm_stage_key: memberRealmStageKey,
        partner_1_realm_stage_label: memberRealmStageLabel,
        partner_2_realm_stage_key: focusPartnerRealmStageKey,
        partner_2_realm_stage_label: focusPartnerRealmStageLabel,

        partner_1_mortal_energy: personalMortalEnergy,
        partner_2_mortal_energy: partnerMortalEnergy,
        shared_mortal_energy: sharedMortalEnergy,

        personal_auric_current: personalAuricCurrent,
        personal_auric_maximum: personalAuricMaximum,
        partner_auric_current: partnerAuricCurrent,
        partner_auric_maximum: partnerAuricMaximum,
        shared_auric_current: sharedAuricCurrent,
        shared_auric_maximum: sharedAuricMaximum,
        shared_auric_percent: sharedQiPercent,

        personal_vestiges: personalCultivationPoints,
        personal_vestiges_maximum: personalCultivationPointsMaximum,
        partner_vestiges: partnerCultivationPoints,
        partner_vestiges_maximum: partnerCultivationPointsMaximum,
        shared_vestiges: sharedCultivationPoints,
        shared_vestiges_maximum: sharedCultivationPointsMaximum,
        shared_vestiges_remaining: sharedCultivationPointsRemaining,
        shared_vestiges_capped: sharedCultivationPointsCapped,

        personal_ascension_tokens: currentMemberTokens,
        partner_ascension_tokens: partnerTokens,
        shared_ascension_tokens: sharedAscensionTokens,

        dual_resource_mode: vesselMode === "dual" ? "shared" : "personal",
        hybrid_resource_mode: vesselMode === "hybrid" ? "personal_plus_shared" : "personal"
      },

      bond_session: focusBondSessionRow
        ? {
            partnership_id: safeText(focusBondSessionRow?.partnership_id) || null,
            status: normalizeBondSessionStatus(focusBondSessionRow?.status, "idle"),
            pause_reason: safeText(focusBondSessionRow?.pause_reason) || null,
            partner_a_ready: Boolean(focusBondSessionRow?.partner_a_ready),
            partner_b_ready: Boolean(focusBondSessionRow?.partner_b_ready),
            partner_a_meditating: Boolean(focusBondSessionRow?.partner_a_meditating),
            partner_b_meditating: Boolean(focusBondSessionRow?.partner_b_meditating),
            last_progress_tick_at: focusBondSessionRow?.last_progress_tick_at || null,
            started_at: focusBondSessionRow?.started_at || null,
            paused_at: focusBondSessionRow?.paused_at || null,
            stopped_at: focusBondSessionRow?.stopped_at || null,
            current_member_book_state: focusBondSessionRow?.current_member_book_state || null,
            partner_member_book_state: focusBondSessionRow?.partner_member_book_state || null
          }
        : null,

      bond: {
        partnership_uuid: safeText(focusPartnership?.id) || null,
        partnership_id: safeText(focusPartnership?.partnership_id) || null,
        title: bondTitle,
        stage_name: bondTitle,
        percent: bondPercent,
        status: bondStatus,
        state_label: bondStateLabel,
        pause_reason: bondPauseReason,
        runtime_active: bondRuntimeActive,
        runtime_paused: bondRuntimePaused,
        total_shared_minutes: safeNumber(
          pickFirst(
            focusBondRow?.total_shared_minutes,
            focusBondRow?.shared_minutes_accumulated,
            0
          ),
          0
        ),
        completed_books_count: safeNumber(
          pickFirst(
            focusBondRow?.completed_books_count,
            focusBondRow?.pair_completed_books_count,
            0
          ),
          0
        ),
        updated_at: pickFirst(
          focusBondSessionRow?.last_progress_tick_at,
          focusBondSessionRow?.paused_at,
          focusBondSessionRow?.started_at,
          focusBondRow?.updated_at
        ) || null
      },

      partnerships: {
        selected_partnership_uuid: safeText(selectedPartnershipUuid) || null,
        focus_partnership_uuid: safeText(focusPartnership?.id) || null,
        focused_partnership_uuid: safeText(focusPartnership?.id) || null,
        focus_partnership_id: safeText(focusPartnership?.partnership_id) || null,
        focus_partnership_status: safeText(focusPartnership?.status) || null,

        partner_selected_partnership_uuid: safeText(focusPartnerSelectedPartnershipUuid) || null,
        partner_focused_partnership_uuid: safeText(focusPartnerFocusedPartnership?.id) || null,
        partner_focused_partnership_id: safeText(focusPartnerFocusedPartnership?.partnership_id) || null,

        active_partnership_count: activePartnershipCount,
        active_partners_online_count: activePartnersOnlineCount,
        pending_incoming_count: pendingIncomingCount,
        pending_outgoing_count: pendingOutgoingCount,
        has_multiple_active_partnerships: activePartnershipCount > 1,
        current_focus_partner_username: safeText(
          focusPartnerMemberRow?.sl_username,
          focusPartnerIdentity.username
        ) || null,
        current_focus_partner_online: Boolean(focusPartnerOnline),
        mutual_focus_active: mutualFocusActive,
        hybrid_shared_ready: hybridSharedReady,
        resource_share_enabled: resourceShareEnabled,

        active: activePartnerSummaries,
        pending_incoming: pendingIncomingSummaries,
        pending_outgoing: pendingOutgoingSummaries
      },

      dashboard: {
        realm_line: realmLine,
        vessel_mode: vesselMode,
        live_vessel_state: liveVesselState,
        partner_active: dashboardPartnerActive,
        partner_online: Boolean(focusIsActive && focusPartnerOnline),
        mutual_focus_active: mutualFocusActive,
        hybrid_shared_ready: hybridSharedReady,
        resource_share_enabled: resourceShareEnabled,
        partnership_status: partnershipStatus,

        current_realm_display_name: partner1Realm,
        current_realm_stage_key: memberRealmStageKey,
        current_realm_stage_label: memberRealmStageLabel,
        current_cultivation_focus: currentCultivationFocus,
        live_practice_mode: effectiveCultivationMode,
        live_practice_state: effectiveCultivationMode,

        shared_auric_current: sharedAuricCurrent,
        shared_auric_maximum: sharedAuricMaximum,
        shared_auric_percent: sharedQiPercent,

        shared_vestiges: sharedCultivationPoints,
        shared_vestiges_maximum: sharedCultivationPointsMaximum,
        shared_vestiges_remaining: sharedCultivationPointsRemaining,
        shared_vestiges_capped: sharedCultivationPointsCapped,

        shared_ascension_tokens: sharedAscensionTokens,
        shared_mortal_energy: sharedMortalEnergy,

        partner_1_realm: partner1Realm,
        partner_2_realm: partner2Realm,
        partner_1_realm_stage_key: memberRealmStageKey,
        partner_1_realm_stage_label: memberRealmStageLabel,
        partner_2_realm_stage_key: focusPartnerRealmStageKey,
        partner_2_realm_stage_label: focusPartnerRealmStageLabel,

        partner_1_mortal_energy: personalMortalEnergy,
        partner_2_mortal_energy: partnerMortalEnergy,

        selected_partnership_uuid: safeText(selectedPartnershipUuid) || null,
        focus_partnership_uuid: safeText(focusPartnership?.id) || null,
        focused_partnership_uuid: safeText(focusPartnership?.id) || null,
        focus_partnership_id: safeText(focusPartnership?.partnership_id) || null,
        focus_partnership_status: safeText(focusPartnership?.status) || null,

        partner_selected_partnership_uuid: safeText(focusPartnerSelectedPartnershipUuid) || null,
        partner_focused_partnership_uuid: safeText(focusPartnerFocusedPartnership?.id) || null,
        partner_focused_partnership_id: safeText(focusPartnerFocusedPartnership?.partnership_id) || null,

        current_focus_partner_username: safeText(
          focusPartnerMemberRow?.sl_username,
          focusPartnerIdentity.username
        ) || null,
        current_focus_partner_online: Boolean(focusPartnerOnline),
        active_partnership_count: activePartnershipCount,
        active_partners_online_count: activePartnersOnlineCount,
        pending_incoming_count: pendingIncomingCount,
        pending_outgoing_count: pendingOutgoingCount,
        has_multiple_active_partnerships: activePartnershipCount > 1,

        bond_title: bondTitle,
        bond_percent: bondPercent,
        bond_status: bondStatus,
        bond_state_label: bondStateLabel,
        bond_runtime_active: bondRuntimeActive,
        bond_runtime_paused: bondRuntimePaused,

        alignment_preview_applies_to_live_meditation: alignmentPreviewAppliesToLiveMeditation,
        alignment_path_key: alignmentState.path_key,
        alignment_path_name: alignmentState.path_name,
        alignment_path_state: alignmentState.path_state,
        alignment_path_revealed: alignmentState.path_revealed,
        alignment_yin_total: alignmentState.yin_total,
        alignment_yang_total: alignmentState.yang_total,
        alignment_taiji_total: alignmentState.taiji_total,
        alignment_hour_group: alignmentState.hour_group,
        alignment_phase_name: alignmentState.phase_name,
        alignment_force_name: alignmentState.force_name,
        alignment_phenomenon_name: alignmentState.phenomenon_name,
        alignment_effective_bias: alignmentState.effective_bias,
        alignment_auric_multiplier: alignmentState.auric_multiplier,
        alignment_cp_multiplier: alignmentState.cp_multiplier,
        alignment_aligned_bonus_available: alignmentState.aligned_bonus_available,
        alignment_aligned_bonus_window_active: alignmentState.aligned_bonus_window_active,
        alignment_conversion_target_path_key: alignmentState.conversion_target_path_key,
        alignment_conversion_target_path_name: alignmentState.conversion_target_path_name,
        alignment_conversion_cost_per_minute: alignmentState.conversion_cost_per_minute
      }
    });
  } catch (err) {
    console.error("load-dashboard-state server error:", err);
    return json(500, {
      success: false,
      error: "server_error",
      details: err?.message || String(err)
    });
  }
};