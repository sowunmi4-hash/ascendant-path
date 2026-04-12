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

const PARTNERSHIP_TABLE = "cultivation_partnerships";
const MEMBER_SELECTED_PARTNERSHIPS_TABLE = "member_selected_partnerships";

const STARTABLE_DISPLAY_STATES = new Set([
  "ready_to_start",
  "paused",
  "active"
]);

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
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

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  const text = safeLower(value);
  return ["true", "1", "yes", "y", "on", "active"].includes(text);
}

function parsePositiveInteger(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
}

function firstFilled(...values) {
  for (const value of values) {
    const text = safeText(value);
    if (text) return text;
  }
  return "";
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizeIdentifier(value) {
  return safeLower(value).replace(/\s+/g, "");
}

function sameValue(a, b) {
  return safeLower(a) === safeLower(b);
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function requireId(value, label) {
  const clean = safeText(value);
  const lowered = safeLower(clean);

  if (!clean || lowered === "undefined" || lowered === "null") {
    throw new Error(`Missing valid ${label}.`);
  }

  return clean;
}

function getBondStageName(percent) {
  const p = clamp(safeNumber(percent, 0), 0, 100);

  if (p >= 100) return "Eternal Bond";
  if (p >= 80) return "Bond Spirit";
  if (p >= 60) return "Bond Soul";
  if (p >= 40) return "Bond Core";
  if (p >= 20) return "Bond Root";
  return "Bond Seed";
}

function normalizePartnershipRow(row) {
  if (!row) return null;

  return {
    ...row,
    id: requireId(row.id, "partnership_uuid")
  };
}

function getMemberPrimaryId(member) {
  return firstFilled(member?.id, member?.member_id) || null;
}

function normalizeRpcRow(data) {
  if (Array.isArray(data)) return data[0] || null;
  if (data && typeof data === "object") return data;
  return null;
}

function normalizeDisplayStateResult(data, fallback = "locked") {
  if (typeof data === "string") return safeLower(data) || fallback;

  if (Array.isArray(data)) {
    const row = data[0];
    if (!row) return fallback;
    if (typeof row === "string") return safeLower(row) || fallback;
    return safeLower(row.display_state || row.state || row.status) || fallback;
  }

  if (data && typeof data === "object") {
    return safeLower(data.display_state || data.state || data.status) || fallback;
  }

  return fallback;
}

function readCoreField(snapshot, ...keys) {
  for (const key of keys) {
    const value = snapshot?.[key];
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return null;
}

function detectCultivationState(member) {
  const v2Status = safeLower(member?.v2_cultivation_status);
  const isActive = v2Status === "cultivating";

  return {
    is_active: isActive,
    cultivation_status: v2Status || "idle",
    started_at: member?.v2_cultivation_started_at || null,
    accumulated_seconds: safeNumber(member?.v2_accumulated_seconds, 0),
    sessions_today: safeNumber(member?.v2_sessions_today, 0)
  };
}

function getPartnerRole(activePartnership, selfAvatarKey, selfUsername) {
  const requesterAvatar = normalizeIdentifier(activePartnership?.requester_avatar_key);
  const requesterUsername = normalizeIdentifier(activePartnership?.requester_username);
  const recipientAvatar = normalizeIdentifier(activePartnership?.recipient_avatar_key);
  const recipientUsername = normalizeIdentifier(activePartnership?.recipient_username);

  const selfAvatar = normalizeIdentifier(selfAvatarKey);
  const selfUser = normalizeIdentifier(selfUsername);

  if (
    (selfAvatar && selfAvatar === requesterAvatar) ||
    (selfUser && selfUser === requesterUsername)
  ) {
    return "partner_a";
  }

  if (
    (selfAvatar && selfAvatar === recipientAvatar) ||
    (selfUser && selfUser === recipientUsername)
  ) {
    return "partner_b";
  }

  return "";
}

function isMemberOfPartnership(partnershipRow, member) {
  if (!partnershipRow || !member) return false;

  const selfAvatar = normalizeIdentifier(member?.sl_avatar_key);
  const selfUsername = normalizeIdentifier(member?.sl_username);

  const requesterAvatar = normalizeIdentifier(partnershipRow?.requester_avatar_key);
  const requesterUsername = normalizeIdentifier(partnershipRow?.requester_username);
  const recipientAvatar = normalizeIdentifier(partnershipRow?.recipient_avatar_key);
  const recipientUsername = normalizeIdentifier(partnershipRow?.recipient_username);

  return (
    (selfAvatar && (selfAvatar === requesterAvatar || selfAvatar === recipientAvatar)) ||
    (selfUsername && (selfUsername === requesterUsername || selfUsername === recipientUsername))
  );
}

function getPartnerIdentityFromPartnership(activePartnership, member) {
  const requesterAvatar = safeText(activePartnership?.requester_avatar_key);
  const requesterUsername = safeLower(activePartnership?.requester_username);
  const recipientAvatar = safeText(activePartnership?.recipient_avatar_key);
  const recipientUsername = safeLower(activePartnership?.recipient_username);

  const selfAvatar = safeText(member?.sl_avatar_key);
  const selfUsername = safeLower(member?.sl_username);

  const selfIsRequester =
    (selfAvatar && sameValue(selfAvatar, requesterAvatar)) ||
    (selfUsername && sameValue(selfUsername, requesterUsername));

  return selfIsRequester
    ? {
        partner_avatar_key: recipientAvatar || null,
        partner_sl_username: recipientUsername || null
      }
    : {
        partner_avatar_key: requesterAvatar || null,
        partner_sl_username: requesterUsername || null
      };
}

function computeIndividualProgressPercent(row, fallbackRequiredMinutes = 0, fallbackRequiredQi = 0) {
  const requiredMinutes = Math.max(
    0,
    safeNumber(row?.required_minutes, fallbackRequiredMinutes)
  );
  const requiredQi = Math.max(
    0,
    safeNumber(row?.required_qi, fallbackRequiredQi)
  );

  const minutesAccumulated = Math.max(0, safeNumber(row?.minutes_accumulated, 0));
  const qiAccumulated = Math.max(0, safeNumber(row?.qi_accumulated, 0));

  const minuteRatio = requiredMinutes > 0 ? minutesAccumulated / requiredMinutes : 0;
  const qiRatio = requiredQi > 0 ? qiAccumulated / requiredQi : null;

  const ratio = qiRatio === null ? minuteRatio : Math.min(minuteRatio, qiRatio);
  return clamp(Number((ratio * 100).toFixed(2)), 0, 100);
}

function getStartConflictMessage(displayState, volumeNumber, bookNumber) {
  const state = safeLower(displayState);

  if (state === "locked") {
    return `Chronicle ${bookNumber} of Relic ${volumeNumber} is locked.`;
  }

  if (state === "ready_for_offering") {
    return `Chronicle ${bookNumber} of Relic ${volumeNumber} still needs your offering first.`;
  }

  if (state === "waiting_for_partner_offering") {
    return `Chronicle ${bookNumber} of Relic ${volumeNumber} is waiting for your partner's offering.`;
  }

  if (state === "awaiting_partner_completion") {
    return `Your side of Chronicle ${bookNumber} of Relic ${volumeNumber} is already complete and is waiting for your partner.`;
  }

  if (state === "pair_completed") {
    return `Chronicle ${bookNumber} of Relic ${volumeNumber} has already been pair-completed.`;
  }

  return `Chronicle ${bookNumber} of Relic ${volumeNumber} cannot be started from its current state.`;
}

function getStartMessage(preDisplayState, postDisplayState, volumeNumber, bookNumber, isCultivating) {
  const pre = safeLower(preDisplayState);
  const post = safeLower(postDisplayState);

  if (post === "active" && pre === "active") {
    return `Bond cultivation is already active for Chronicle ${bookNumber} of Relic ${volumeNumber}.`;
  }

  if (post === "active" && pre === "paused") {
    return `Bond cultivation resumed for Chronicle ${bookNumber} of Relic ${volumeNumber}.`;
  }

  if (post === "active") {
    return isCultivating
      ? `Bond cultivation started for Chronicle ${bookNumber} of Relic ${volumeNumber}.`
      : `Bond cultivation opened for Chronicle ${bookNumber} of Relic ${volumeNumber}. Progress will only advance while you are cultivating.`;
  }

  if (post === "paused") {
    return `Bond cultivation for Chronicle ${bookNumber} of Relic ${volumeNumber} is paused.`;
  }

  if (post === "awaiting_partner_completion") {
    return `Your side of Chronicle ${bookNumber} of Relic ${volumeNumber} is complete and is waiting for your partner.`;
  }

  if (post === "pair_completed") {
    return `Chronicle ${bookNumber} of Relic ${volumeNumber} has already been pair-completed.`;
  }

  return `Bond cultivation state loaded for Chronicle ${bookNumber} of Relic ${volumeNumber}.`;
}

function getStartAction(preDisplayState, postDisplayState) {
  const pre = safeLower(preDisplayState);
  const post = safeLower(postDisplayState);

  if (post === "active" && pre === "active") return "already_active";
  if (post === "active" && pre === "paused") return "resumed";
  if (post === "active") return "started";
  if (post === "paused") return "paused";
  if (post === "awaiting_partner_completion") return "awaiting_partner_completion";
  if (post === "pair_completed") return "pair_completed";
  return "loaded";
}

async function loadMember(slAvatarKey, slUsername) {
  let query = supabase
    .from("cultivation_members")
    .select("*")
    .limit(1);

  if (slAvatarKey) {
    query = query.eq("sl_avatar_key", slAvatarKey);
  } else if (slUsername) {
    query = query.eq("sl_username", slUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function loadSessionRow(sessionToken) {
  const { data, error } = await supabase
    .from("website_sessions")
    .select("*")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load website session: ${error.message}`);
  }

  return data || null;
}

async function loadMemberFromSessionToken(sessionToken) {
  const sessionRow = await loadSessionRow(sessionToken);
  if (!sessionRow) return { sessionRow: null, member: null };

  const member = await loadMember(
    safeText(sessionRow.sl_avatar_key),
    safeText(sessionRow.sl_username)
  );

  return { sessionRow, member };
}

async function loadSelectedPartnershipReference(memberId) {
  if (!memberId) return "";

  const { data, error } = await supabase
    .schema("partner")
    .from(MEMBER_SELECTED_PARTNERSHIPS_TABLE)
    .select("*")
    .eq("member_id", memberId)
    .order("updated_at", { ascending: false })
    .limit(2);

  if (error) {
    throw new Error(`Failed to load selected partnership: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(`Multiple selected partnership rows found for member ${memberId}.`);
  }

  const row = rows[0] || null;
  if (!row) return "";

  return (
    safeText(row.selected_partnership_id) ||
    safeText(row.partnership_id) ||
    safeText(row.selected_partnership_uuid) ||
    safeText(row.partnership_uuid) ||
    ""
  );
}

async function saveSelectedPartnership(memberId, partnershipUuid) {
  if (!memberId || !partnershipUuid || !isUuid(partnershipUuid)) return false;

  const { error } = await supabase.schema("partner").rpc("set_member_selected_partnership", {
    p_member_id: memberId,
    p_selected_partnership_id: partnershipUuid
  });

  if (error) {
    console.error("saveSelectedPartnership error:", error);
    return false;
  }

  return true;
}

async function loadPartnershipByUuid(partnershipUuid) {
  const cleanUuid = safeText(partnershipUuid);
  if (!isUuid(cleanUuid)) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      updated_at
    `)
    .eq("id", cleanUuid)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by UUID: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadPartnershipByLegacyId(legacyPartnershipId) {
  const cleanId = parsePositiveInteger(legacyPartnershipId);
  if (!cleanId) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      updated_at
    `)
    .eq("partnership_id", cleanId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by legacy id: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadActivePartnershipRowsForMember(member) {
  const avatarKey = safeText(member?.sl_avatar_key);
  const username = safeText(member?.sl_username);

  if (!avatarKey && !username) return [];

  let query = supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      updated_at
    `)
    .eq("status", "active")
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false });

  if (avatarKey) {
    query = query.or(
      `requester_avatar_key.eq.${avatarKey},recipient_avatar_key.eq.${avatarKey}`
    );
  } else {
    query = query.or(
      `requester_username.eq.${username},recipient_username.eq.${username}`
    );
  }

  const { data, error } = await query;

  if (error) {
    throw new Error(`Failed to load active partnerships: ${error.message}`);
  }

  return Array.isArray(data) ? data.map(normalizePartnershipRow) : [];
}

function buildResolvedPartnershipPayload({
  member,
  partnership,
  source,
  hasActivePartnerships,
  multipleActivePartnerships,
  selectedPartnershipRequired,
  selectedPartnershipFound,
  selectedPartnershipMissing,
  selectedPartnershipInactive,
  selectedPartnershipInvalid,
  explicitResolutionFailed,
  explicitResolutionReason,
  selectedReference
}) {
  const buyerRole = partnership
    ? getPartnerRole(
        partnership,
        safeText(member?.sl_avatar_key),
        safeLower(member?.sl_username)
      )
    : "";

  return {
    partnership: partnership || null,
    partnership_uuid: partnership?.id || null,
    partnership_id: parsePositiveInteger(partnership?.partnership_id),
    legacy_partnership_id: parsePositiveInteger(partnership?.partnership_id),
    buyer_role: buyerRole || null,
    partnership_source: source || null,

    has_active_partnership: Boolean(hasActivePartnerships),
    has_multiple_active_partnerships: Boolean(multipleActivePartnerships),

    selected_partnership_required: Boolean(selectedPartnershipRequired),
    selected_partnership_found: Boolean(selectedPartnershipFound),
    selected_partnership_missing: Boolean(selectedPartnershipMissing),
    selected_partnership_inactive: Boolean(selectedPartnershipInactive),
    selected_partnership_invalid: Boolean(selectedPartnershipInvalid),

    explicit_resolution_failed: Boolean(explicitResolutionFailed),
    explicit_resolution_reason: explicitResolutionReason || null,

    selected_reference: selectedReference || null
  };
}

async function resolveBondPartnershipForMember({
  member,
  requestedPartnershipUuid,
  requestedLegacyPartnershipId
}) {
  const activeRows = await loadActivePartnershipRowsForMember(member);

  const hasActivePartnerships = activeRows.length > 0;
  const multipleActivePartnerships = activeRows.length > 1;

  let selectedPartnershipFound = false;
  let selectedPartnershipMissing = false;
  let selectedPartnershipInactive = false;
  let selectedPartnershipInvalid = false;
  let explicitResolutionFailed = false;
  let explicitResolutionReason = null;
  let selectedReference = "";

  if (safeText(requestedPartnershipUuid)) {
    if (!isUuid(requestedPartnershipUuid)) {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "invalid_partnership_uuid",
        selectedReference
      });
    }

    const row = await loadPartnershipByUuid(requestedPartnershipUuid);

    if (!row) {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_uuid_not_found",
        selectedReference
      });
    }

    if (safeLower(row.status) !== "active") {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_not_active",
        selectedReference
      });
    }

    if (!isMemberOfPartnership(row, member)) {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "not_member_of_requested_partnership",
        selectedReference
      });
    }

    return buildResolvedPartnershipPayload({
      member,
      partnership: row,
      source: "explicit_partnership_uuid",
      hasActivePartnerships,
      multipleActivePartnerships,
      selectedPartnershipRequired: false,
      selectedPartnershipFound,
      selectedPartnershipMissing,
      selectedPartnershipInactive,
      selectedPartnershipInvalid,
      explicitResolutionFailed,
      explicitResolutionReason,
      selectedReference
    });
  }

  if (requestedLegacyPartnershipId) {
    const row = await loadPartnershipByLegacyId(requestedLegacyPartnershipId);

    if (!row) {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "legacy_partnership_id_not_found",
        selectedReference
      });
    }

    if (safeLower(row.status) !== "active") {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_not_active",
        selectedReference
      });
    }

    if (!isMemberOfPartnership(row, member)) {
      return buildResolvedPartnershipPayload({
        member,
        partnership: null,
        source: null,
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: multipleActivePartnerships,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed: true,
        explicitResolutionReason: "not_member_of_requested_partnership",
        selectedReference
      });
    }

    return buildResolvedPartnershipPayload({
      member,
      partnership: row,
      source: "explicit_legacy_partnership_id",
      hasActivePartnerships,
      multipleActivePartnerships,
      selectedPartnershipRequired: false,
      selectedPartnershipFound,
      selectedPartnershipMissing,
      selectedPartnershipInactive,
      selectedPartnershipInvalid,
      explicitResolutionFailed,
      explicitResolutionReason,
      selectedReference
    });
  }

  const memberId = getMemberPrimaryId(member);
  selectedReference = await loadSelectedPartnershipReference(memberId);

  if (selectedReference) {
    selectedPartnershipFound = true;

    let selectedRow = null;

    if (isUuid(selectedReference)) {
      selectedRow = await loadPartnershipByUuid(selectedReference);
    } else {
      selectedRow = await loadPartnershipByLegacyId(selectedReference);
    }

    if (!selectedRow) {
      selectedPartnershipMissing = true;
    } else if (safeLower(selectedRow.status) !== "active") {
      selectedPartnershipInactive = true;
    } else if (!isMemberOfPartnership(selectedRow, member)) {
      selectedPartnershipInvalid = true;
    } else {
      return buildResolvedPartnershipPayload({
        member,
        partnership: selectedRow,
        source: "selected_partnership",
        hasActivePartnerships,
        multipleActivePartnerships,
        selectedPartnershipRequired: false,
        selectedPartnershipFound,
        selectedPartnershipMissing,
        selectedPartnershipInactive,
        selectedPartnershipInvalid,
        explicitResolutionFailed,
        explicitResolutionReason,
        selectedReference
      });
    }
  }

  if (activeRows.length === 1) {
    return buildResolvedPartnershipPayload({
      member,
      partnership: activeRows[0],
      source: "single_active_fallback",
      hasActivePartnerships,
      multipleActivePartnerships,
      selectedPartnershipRequired: false,
      selectedPartnershipFound,
      selectedPartnershipMissing,
      selectedPartnershipInactive,
      selectedPartnershipInvalid,
      explicitResolutionFailed,
      explicitResolutionReason,
      selectedReference
    });
  }

  return buildResolvedPartnershipPayload({
    member,
    partnership: null,
    source: null,
    hasActivePartnerships,
    multipleActivePartnerships,
    selectedPartnershipRequired: multipleActivePartnerships,
    selectedPartnershipFound,
    selectedPartnershipMissing,
    selectedPartnershipInactive,
    selectedPartnershipInvalid,
    explicitResolutionFailed,
    explicitResolutionReason,
    selectedReference
  });
}

async function loadPartnerMemberRow({ partnerAvatarKey, partnerUsername }) {
  if (!partnerAvatarKey && !partnerUsername) return null;

  let query = supabase
    .from("cultivation_members")
    .select("*")
    .limit(1);

  if (partnerAvatarKey) {
    query = query.eq("sl_avatar_key", partnerAvatarKey);
  } else {
    query = query.eq("sl_username", partnerUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load partner member: ${error.message}`);
  }

  return data || null;
}

async function loadActivePersonalCultivationRows(slAvatarKey) {
  if (!slAvatarKey) return [];

  const { data, error } = await supabase
    .from("cultivation_section_progress")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey);

  if (error) {
    throw new Error(`Failed to load personal cultivation progress: ${error.message}`);
  }

  return (data || []).filter((row) => {
    const activeStartedAt = firstFilled(
      row?.active_session_started_at,
      row?.timing_started_at
    );
    const completedAt = firstFilled(row?.completed_at);
    return !!activeStartedAt && !completedAt;
  });
}

function isSchemaColumnError(error) {
  const message = safeLower(error?.message);
  return message.includes("column") && message.includes("does not exist");
}

function computePausedProgressSeconds(row) {
  const stored =
    safeNumber(row?.stored_accumulated_seconds, NaN) ||
    safeNumber(row?.accumulated_seconds, NaN) ||
    safeNumber(row?.progress_seconds, NaN) ||
    safeNumber(row?.section_time_progress, 0);

  const startedAt = firstFilled(
    row?.active_session_started_at,
    row?.timing_started_at
  );

  if (!startedAt) {
    return safeNumber(stored, 0);
  }

  const startedMs = new Date(startedAt).getTime();
  const nowMs = Date.now();

  if (!Number.isFinite(startedMs) || startedMs <= 0 || nowMs <= startedMs) {
    return safeNumber(stored, 0);
  }

  const liveSeconds = Math.floor((nowMs - startedMs) / 1000);
  return Math.max(0, safeNumber(stored, 0) + liveSeconds);
}

async function tryUpdateById(tableName, rowId, payloads) {
  let lastError = null;

  for (const payload of payloads) {
    const { data, error } = await supabase
      .from(tableName)
      .update(payload)
      .eq("id", rowId)
      .select("*")
      .maybeSingle();

    if (!error && data) {
      return data;
    }

    if (error && isSchemaColumnError(error)) {
      lastError = error;
      continue;
    }

    if (error) {
      lastError = error;
      break;
    }
  }

  throw new Error(lastError?.message || `Failed to update ${tableName}.`);
}

async function pausePersonalCultivationForMember(member) {
  const rows = await loadActivePersonalCultivationRows(member?.sl_avatar_key);
  if (!rows.length) return [];

  const nowIso = new Date().toISOString();
  const updatedRows = [];

  for (const row of rows) {
    if (!row?.id) continue;

    const newProgressSeconds = computePausedProgressSeconds(row);

    const payloads = [
      {
        stored_accumulated_seconds: newProgressSeconds,
        active_session_started_at: null,
        timing_started_at: null,
        updated_at: nowIso
      },
      {
        accumulated_seconds: newProgressSeconds,
        active_session_started_at: null,
        timing_started_at: null,
        updated_at: nowIso
      },
      {
        progress_seconds: newProgressSeconds,
        active_session_started_at: null,
        timing_started_at: null,
        updated_at: nowIso
      },
      {
        section_time_progress: newProgressSeconds,
        active_session_started_at: null,
        timing_started_at: null,
        updated_at: nowIso
      },
      {
        active_session_started_at: null,
        timing_started_at: null,
        updated_at: nowIso
      },
      {
        active_session_started_at: null,
        updated_at: nowIso
      }
    ];

    const updated = await tryUpdateById("cultivation_section_progress", row.id, payloads);
    updatedRows.push(updated);
  }

  return updatedRows;
}

async function initializePartnerBondStates(partnershipUuid) {
  const cleanId = safeText(partnershipUuid);
  if (!isUuid(cleanId)) return null;

  const { error } = await partnerSupabase.rpc(
    "initialize_partner_bond_member_book_states",
    {
      p_partnership_uuid: cleanId
    }
  );

  if (error) {
    throw new Error(`Failed to initialize partner bond member book states: ${error.message}`);
  }

  return true;
}

async function loadMemberBookRows(partnershipUuid, memberIds, volumeNumber, bookNumber) {
  const { data, error } = await partnerSupabase
    .from("partner_bond_member_book_states")
    .select("*")
    .eq("partnership_uuid", partnershipUuid)
    .in("member_id", memberIds)
    .eq("bond_volume_number", volumeNumber)
    .eq("bond_book_number", bookNumber);

  if (error) {
    throw new Error(`Failed to load partner bond member book rows: ${error.message}`);
  }

  return data || [];
}

async function loadDisplayState({
  partnershipUuid,
  memberId,
  volumeNumber,
  bookNumber
}) {
  const { data, error } = await partnerSupabase.rpc(
    "get_partner_bond_member_book_display_state",
    {
      p_partnership_uuid: partnershipUuid,
      p_member_id: memberId,
      p_bond_volume_number: volumeNumber,
      p_bond_book_number: bookNumber
    }
  );

  if (error) {
    throw new Error(`Failed to load bond display state: ${error.message}`);
  }

  return normalizeDisplayStateResult(data, "locked");
}

async function loadSnapshot({
  partnershipUuid,
  memberId,
  volumeNumber,
  bookNumber
}) {
  const { data, error } = await partnerSupabase.rpc(
    "get_partner_bond_member_book_snapshot",
    {
      p_partnership_uuid: partnershipUuid,
      p_member_id: memberId,
      p_bond_volume_number: volumeNumber,
      p_bond_book_number: bookNumber
    }
  );

  if (error) {
    throw new Error(`Failed to load bond snapshot: ${error.message}`);
  }

  return normalizeRpcRow(data);
}

async function startBondBook({
  partnershipUuid,
  memberId,
  volumeNumber,
  bookNumber
}) {
  const { data, error } = await partnerSupabase.rpc(
    "start_partner_bond_member_book",
    {
      p_partnership_uuid: partnershipUuid,
      p_member_id: memberId,
      p_bond_volume_number: volumeNumber,
      p_bond_book_number: bookNumber
    }
  );

  if (error) {
    throw new Error(`Failed to start bond book: ${error.message}`);
  }

  return normalizeRpcRow(data);
}

async function loadBondBookCatalog(volumeNumber, bookNumber) {
  const { data: volumes, error: volumeError } = await supabase
    .schema("partner")
    .from("bond_volumes")
    .select("id, volume_number")
    .eq("is_active", true);

  if (volumeError) {
    throw new Error(`Failed to load bond volumes: ${volumeError.message}`);
  }

  const volumeRow = (volumes || []).find(
    (row) => safeNumber(row?.volume_number, 0) === safeNumber(volumeNumber, 0)
  );

  if (!volumeRow?.id) return null;

  const { data: books, error: bookError } = await supabase
    .schema("partner")
    .from("bond_books")
    .select("*")
    .eq("volume_id", volumeRow.id)
    .eq("is_active", true);

  if (bookError) {
    throw new Error(`Failed to load bond books: ${bookError.message}`);
  }

  return (books || []).find(
    (row) => safeNumber(row?.book_number, 0) === safeNumber(bookNumber, 0)
  ) || null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use POST."
    });
  }

  try {
    const body = parseBody(event);

    const cookieHeader =
      event.headers?.cookie ||
      event.headers?.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionCookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = safeText(cookies[sessionCookieName]);

    if (!sessionToken) {
      return buildResponse(401, {
        success: false,
        message: "Session cookie is required. Please log in."
      });
    }

    const sessionResult = await loadMemberFromSessionToken(sessionToken);
    const sessionRow = sessionResult.sessionRow;
    let member = sessionResult.member;

    if (!sessionRow) {
      return buildResponse(401, {
        success: false,
        message: "Invalid or expired website session."
      });
    }

    const slAvatarKey = safeText(sessionRow.sl_avatar_key);
    const slUsername = safeLower(sessionRow.sl_username);

    if (!member && (slAvatarKey || slUsername)) {
      member = await loadMember(slAvatarKey, slUsername);
    }

    const requestedPartnershipUuid = safeText(
      body.partnership_uuid ||
      body.selected_partnership_uuid
    );

    const requestedLegacyPartnershipId =
      parsePositiveInteger(
        body.partnership_id ||
        body.legacy_partnership_id
      ) || null;

    const volumeNumber = parsePositiveInteger(body.volume_number);
    const bookNumber = parsePositiveInteger(body.book_number);

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    if (!volumeNumber) {
      return buildResponse(400, {
        success: false,
        message: "A valid volume_number is required."
      });
    }

    if (!bookNumber) {
      return buildResponse(400, {
        success: false,
        message: "A valid book_number is required."
      });
    }

    const memberId = getMemberPrimaryId(member);
    if (!memberId) {
      return buildResponse(500, {
        success: false,
        message: "Cultivation member is missing its primary key."
      });
    }

    const resolvedPartnership = await resolveBondPartnershipForMember({
      member,
      requestedPartnershipUuid,
      requestedLegacyPartnershipId
    });

    if (!resolvedPartnership.partnership_uuid || !resolvedPartnership.partnership) {
      return buildResponse(409, {
        success: false,
        message: "No active partnership could be resolved for this member.",
        partnership_resolution: {
          requested_partnership_uuid: requestedPartnershipUuid || null,
          requested_partnership_id: requestedLegacyPartnershipId || null,
          selected_partnership_reference: resolvedPartnership.selected_reference || null,
          has_active_partnership: Boolean(resolvedPartnership.has_active_partnership),
          has_multiple_active_partnerships: Boolean(
            resolvedPartnership.has_multiple_active_partnerships
          ),
          selected_partnership_required: Boolean(
            resolvedPartnership.selected_partnership_required
          ),
          explicit_resolution_failed: Boolean(
            resolvedPartnership.explicit_resolution_failed
          ),
          explicit_resolution_reason:
            resolvedPartnership.explicit_resolution_reason || null
        }
      });
    }

    const activePartnership = resolvedPartnership.partnership;
    const partnershipUuid = safeText(activePartnership.id);

    await saveSelectedPartnership(memberId, partnershipUuid);

    const partnerRole =
      resolvedPartnership.buyer_role ||
      getPartnerRole(
        activePartnership,
        safeText(member.sl_avatar_key),
        safeLower(member.sl_username)
      );

    if (!partnerRole) {
      return buildResponse(409, {
        success: false,
        message: "This member does not belong to the resolved active partnership."
      });
    }

    const partnerIdentity = getPartnerIdentityFromPartnership(activePartnership, member);

    const partnerMember = await loadPartnerMemberRow({
      partnerAvatarKey: partnerIdentity.partner_avatar_key,
      partnerUsername: partnerIdentity.partner_sl_username
    });

    if (!partnerMember) {
      return buildResponse(404, {
        success: false,
        message: "Partner member record could not be found."
      });
    }

    const partnerMemberId = getMemberPrimaryId(partnerMember);
    if (!partnerMemberId) {
      return buildResponse(500, {
        success: false,
        message: "Partner member is missing its primary key."
      });
    }

    const memberCultivation = detectCultivationState(member);
    const partnerCultivation = detectCultivationState(partnerMember);

    await initializePartnerBondStates(partnershipUuid);

    const preDisplayState = await loadDisplayState({
      partnershipUuid,
      memberId,
      volumeNumber,
      bookNumber
    });

    if (!STARTABLE_DISPLAY_STATES.has(preDisplayState)) {
      return buildResponse(409, {
        success: false,
        message: getStartConflictMessage(preDisplayState, volumeNumber, bookNumber),
        display_state: preDisplayState,
        partnership: {
          partnership_uuid: partnershipUuid,
          partnership_id: parsePositiveInteger(activePartnership.partnership_id) || null,
          legacy_partnership_id: parsePositiveInteger(activePartnership.partnership_id) || null,
          buyer_role: partnerRole,
          partnership_source: resolvedPartnership.partnership_source || null
        }
      });
    }

    const pausedPersonalRows = await pausePersonalCultivationForMember(member);

    if (preDisplayState !== "active") {
      await startBondBook({
        partnershipUuid,
        memberId,
        volumeNumber,
        bookNumber
      });
    }

    const [freshMember, freshPartner, postDisplayState, snapshot, bookRows, catalogBook] = await Promise.all([
      loadMember(safeText(member.sl_avatar_key), safeLower(member.sl_username)),
      loadMember(safeText(partnerMember.sl_avatar_key), safeLower(partnerMember.sl_username)),
      loadDisplayState({
        partnershipUuid,
        memberId,
        volumeNumber,
        bookNumber
      }),
      loadSnapshot({
        partnershipUuid,
        memberId,
        volumeNumber,
        bookNumber
      }),
      loadMemberBookRows(
        partnershipUuid,
        [memberId, partnerMemberId],
        volumeNumber,
        bookNumber
      ),
      loadBondBookCatalog(volumeNumber, bookNumber)
    ]);

    const selfRow =
      (bookRows || []).find((row) => safeText(row?.member_id) === safeText(memberId)) || null;

    const partnerRow =
      (bookRows || []).find((row) => safeText(row?.member_id) === safeText(partnerMemberId)) || null;

    const requiredMinutes = Math.max(
      0,
      safeNumber(
        selfRow?.required_minutes,
        safeNumber(catalogBook?.required_minutes, safeNumber(catalogBook?.required_shared_minutes, 0))
      )
    );

    const auricDrainPerMinuteEach = Math.max(
      0,
      safeNumber(catalogBook?.auric_drain_per_minute_each, 0)
    );

    const requiredQi = Math.max(
      0,
      safeNumber(selfRow?.required_qi, requiredMinutes * auricDrainPerMinuteEach)
    );

    const selfProgressPercent = computeIndividualProgressPercent(
      selfRow,
      requiredMinutes,
      requiredQi
    );

    const partnerProgressPercent = computeIndividualProgressPercent(
      partnerRow,
      requiredMinutes,
      requiredQi
    );

    const bondPercent = clamp(
      safeNumber(
        readCoreField(
          snapshot,
          "bond_percent",
          "shared_bond_percent",
          "core_bond_percent"
        ),
        0
      ),
      0,
      100
    );

    const bondStage =
      firstFilled(
        readCoreField(
          snapshot,
          "current_stage_name",
          "bond_stage_name",
          "stage_name"
        )
      ) || getBondStageName(bondPercent);

    const action = getStartAction(preDisplayState, postDisplayState);
    const message = getStartMessage(
      preDisplayState,
      postDisplayState,
      volumeNumber,
      bookNumber,
      memberCultivation.is_active
    );

    return buildResponse(200, {
      success: true,
      started: postDisplayState === "active",
      resumed: action === "resumed",
      already_active: action === "already_active",
      display_state: postDisplayState,
      previous_display_state: preDisplayState,
      action,
      message,

      partnership: {
        partnership_uuid: partnershipUuid,
        partnership_id: parsePositiveInteger(activePartnership.partnership_id) || null,
        legacy_partnership_id: parsePositiveInteger(activePartnership.partnership_id) || null,
        buyer_role: partnerRole,
        partnership_source: resolvedPartnership.partnership_source || null
      },

      member: {
        id: getMemberPrimaryId(freshMember || member),
        sl_avatar_key: safeText((freshMember || member)?.sl_avatar_key) || null,
        sl_username: safeText((freshMember || member)?.sl_username) || null,
        vestiges: safeNumber((freshMember || member)?.vestiges, 0),
        cultivation_active: memberCultivation.is_active,
        cultivation_status: memberCultivation.cultivation_status,
        cultivation_started_at: memberCultivation.started_at,
        accumulated_seconds: memberCultivation.accumulated_seconds,
        sessions_today: memberCultivation.sessions_today,
        personal_cultivation_paused: pausedPersonalRows.length > 0,
        paused_personal_rows: pausedPersonalRows.length
      },

      partner: {
        id: getMemberPrimaryId(freshPartner || partnerMember),
        sl_avatar_key: safeText((freshPartner || partnerMember)?.sl_avatar_key) || null,
        sl_username: safeText((freshPartner || partnerMember)?.sl_username) || null,
        vestiges: safeNumber((freshPartner || partnerMember)?.vestiges, 0),
        cultivation_active: partnerCultivation.is_active,
        cultivation_status: partnerCultivation.cultivation_status
      },

      book: {
        volume_number: volumeNumber,
        book_number: bookNumber,
        title: safeText(catalogBook?.title, `Chronicle ${bookNumber}`),
        description: safeText(catalogBook?.description) || null,
        display_state: postDisplayState,
        required_minutes: requiredMinutes,
        required_qi: requiredQi,
        cp_cost_each: safeNumber(catalogBook?.cp_cost_each, safeNumber(selfRow?.offering_cp_spent, 0)),
        token_cost_each: safeNumber(catalogBook?.token_cost_each, safeNumber(selfRow?.offering_token_spent, 0)),
        auric_drain_per_minute_each: auricDrainPerMinuteEach,

        self: {
          offering_complete: safeBoolean(selfRow?.offering_complete),
          offering_cp_spent: safeNumber(selfRow?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(selfRow?.offering_token_spent, 0),
          offering_completed_at: selfRow?.offering_completed_at || null,
          minutes_accumulated: safeNumber(selfRow?.minutes_accumulated, 0),
          qi_accumulated: safeNumber(selfRow?.qi_accumulated, 0),
          progress_percent: selfProgressPercent,
          status: safeText(selfRow?.status) || null,
          started_at: selfRow?.started_at || null,
          last_progress_at: selfRow?.last_progress_at || null,
          paused_at: selfRow?.paused_at || null,
          completed_at: selfRow?.completed_at || null
        },

        partner: {
          offering_complete: safeBoolean(partnerRow?.offering_complete),
          offering_cp_spent: safeNumber(partnerRow?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(partnerRow?.offering_token_spent, 0),
          offering_completed_at: partnerRow?.offering_completed_at || null,
          minutes_accumulated: safeNumber(partnerRow?.minutes_accumulated, 0),
          qi_accumulated: safeNumber(partnerRow?.qi_accumulated, 0),
          progress_percent: partnerProgressPercent,
          status: safeText(partnerRow?.status) || null,
          started_at: partnerRow?.started_at || null,
          last_progress_at: partnerRow?.last_progress_at || null,
          paused_at: partnerRow?.paused_at || null,
          completed_at: partnerRow?.completed_at || null
        },

        pair: {
          offering_complete:
            safeBoolean(selfRow?.offering_complete) &&
            safeBoolean(partnerRow?.offering_complete),
          pair_completed: postDisplayState === "pair_completed"
        }
      },

      bond_core: {
        bond_percent: Number(bondPercent.toFixed(2)),
        stage_name: bondStage,
        completed_books_count: safeNumber(
          readCoreField(
            snapshot,
            "completed_books_count",
            "pair_completed_books_count"
          ),
          0
        ),
        total_shared_minutes: safeNumber(
          readCoreField(
            snapshot,
            "total_shared_minutes",
            "shared_total_minutes"
          ),
          0
        ),
        shared_offering_cp_total: safeNumber(
          readCoreField(
            snapshot,
            "shared_offering_cp_total",
            "offering_cp_total",
            "total_offering_cp"
          ),
          0
        ),
        shared_offering_token_total: safeNumber(
          readCoreField(
            snapshot,
            "shared_offering_token_total",
            "offering_token_total",
            "total_offering_token"
          ),
          0
        )
      },

      snapshot: snapshot || null
    });
  } catch (error) {
    console.error("start-bond-cultivation error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to start bond cultivation.",
      error: error.message || "Unknown error."
    });
  }
};