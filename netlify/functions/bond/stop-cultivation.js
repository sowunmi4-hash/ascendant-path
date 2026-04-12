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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
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
  return firstFilled(member?.member_id, member?.id) || null;
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

function detectMeditationState(member) {
  const v2Status = safeLower(member?.v2_cultivation_status);
  const isActive = v2Status === "cultivating";

  return {
    is_active: isActive,
    raw_state: v2Status || null,
    started_at: member?.v2_cultivation_started_at || null
  };
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
  const qiAccumulated = Math.max(0, safeNumber(row?.auric_accumulated, 0));

  const minuteRatio = requiredMinutes > 0 ? minutesAccumulated / requiredMinutes : 0;
  const qiRatio = requiredQi > 0 ? qiAccumulated / requiredQi : null;

  const ratio = qiRatio === null ? minuteRatio : Math.min(minuteRatio, qiRatio);
  return clamp(Number((ratio * 100).toFixed(2)), 0, 100);
}

function getStopMessage(displayState, bookNumber, volumeNumber) {
  const state = safeLower(displayState);

  if (state === "paused") {
    return `Your progression for Bond Book ${bookNumber} in Volume ${volumeNumber} is now paused.`;
  }

  if (state === "awaiting_partner_completion") {
    return `Your side of Bond Book ${bookNumber} in Volume ${volumeNumber} is complete and is waiting for your partner.`;
  }

  if (state === "ready_to_start") {
    return `Bond Book ${bookNumber} in Volume ${volumeNumber} is no longer active and is ready to start again.`;
  }

  if (state === "available") {
    return `Bond Book ${bookNumber} in Volume ${volumeNumber} is no longer active.`;
  }

  if (state === "pair_completed") {
    return `Bond Book ${bookNumber} in Volume ${volumeNumber} has already been pair-completed.`;
  }

  return `Bond cultivation stopped for Bond Book ${bookNumber} in Volume ${volumeNumber}.`;
}

function getPartnershipResolutionFailureMessage(resolvedPartnership) {
  if (!resolvedPartnership.has_active_partnership) {
    return "No active partnership was found for this member.";
  }

  if (resolvedPartnership.selected_partnership_required) {
    return "Multiple active partnerships were found. Select a partnership first before stopping bond cultivation.";
  }

  if (resolvedPartnership.selected_partnership_missing) {
    return "The saved selected partnership no longer exists. Re-select a partnership before stopping bond cultivation.";
  }

  if (resolvedPartnership.selected_partnership_inactive) {
    return "The saved selected partnership is not active. Re-select an active partnership before stopping bond cultivation.";
  }

  if (resolvedPartnership.selected_partnership_invalid) {
    return "The saved selected partnership is invalid for this member. Re-select a valid partnership before stopping bond cultivation.";
  }

  if (resolvedPartnership.explicit_resolution_failed) {
    const reason = safeLower(resolvedPartnership.explicit_resolution_reason);

    if (reason === "invalid_partnership_uuid") {
      return "The provided partnership UUID is invalid.";
    }

    if (reason === "partnership_uuid_not_found") {
      return "The requested partnership UUID could not be found.";
    }

    if (reason === "legacy_partnership_id_not_found") {
      return "The requested partnership id could not be found.";
    }

    if (reason === "partnership_not_active") {
      return "The requested partnership is not active.";
    }

    if (reason === "not_member_of_requested_partnership") {
      return "This member does not belong to the requested partnership.";
    }
  }

  return "No active partnership could be resolved for this member.";
}

async function loadMember(slAvatarKey, slUsername) {
  let query = supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      character_name,
      auric_current,
      vestiges,
      v2_cultivation_status,
      v2_cultivation_started_at,
      v2_accumulated_seconds,
      v2_sessions_today,
      realm_name,
      realm_display_name,
      last_presence_at
    `)
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

  if (!data) return null;

  return {
    ...data,
    id: data.member_id || null
  };
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

async function touchSessionAndPresence(sessionToken, member) {
  const now = new Date().toISOString();
  const work = [];

  if (sessionToken) {
    work.push(
      supabase
        .from("website_sessions")
        .update({ updated_at: now })
        .eq("session_token", sessionToken)
    );
  }

  if (member?.sl_avatar_key) {
    work.push(
      supabase
        .from("cultivation_members")
        .update({ last_presence_at: now })
        .eq("sl_avatar_key", member.sl_avatar_key)
    );
  }

  if (work.length) {
    await Promise.all(work);
  }
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
      rejected_at,
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
      rejected_at,
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
      rejected_at,
      updated_at
    `)
    .eq("status", "active")
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false });

  if (avatarKey) {
    query = query.or(
      `requester_avatar_key.eq.${avatarKey},recipient_avatar_key.eq.${avatarKey}`
    );
  } else if (username) {
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

function getPartnerIdentityFromResolvedPartnership(partnershipRow, member) {
  if (!partnershipRow) {
    return {
      partner_avatar_key: null,
      partner_sl_username: null
    };
  }

  const selfAvatar = safeText(member?.sl_avatar_key);
  const selfUsername = safeLower(member?.sl_username);

  const requesterAvatar = safeText(partnershipRow.requester_avatar_key);
  const requesterUsername = safeText(partnershipRow.requester_username);
  const recipientAvatar = safeText(partnershipRow.recipient_avatar_key);
  const recipientUsername = safeText(partnershipRow.recipient_username);

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

async function loadPartnerMemberByIdentity({ partnerAvatarKey, partnerUsername }) {
  if (!partnerAvatarKey && !partnerUsername) return null;

  let query = supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      character_name,
      auric_current,
      vestiges,
      v2_cultivation_status,
      v2_cultivation_started_at,
      v2_accumulated_seconds,
      v2_sessions_today,
      last_presence_at
    `)
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

  if (!data) return null;

  return {
    ...data,
    id: data.member_id || null
  };
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

async function loadPartnerBondCore(partnershipUuid) {
  const cleanId = safeText(partnershipUuid);
  if (!isUuid(cleanId)) return null;

  const { data, error } = await partnerSupabase
    .from("partner_bonds")
    .select("*")
    .eq("partnership_id", cleanId)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partner bond core: ${error.message}`);
  }

  return data || null;
}

async function pauseBondBookMember({
  partnershipUuid,
  memberId,
  volumeNumber,
  bookNumber
}) {
  const nowIso = new Date().toISOString();

  const { data: currentRows, error: currentError } = await partnerSupabase
    .from("partner_bond_member_book_states")
    .select("*")
    .eq("partnership_uuid", partnershipUuid)
    .eq("member_id", memberId)
    .eq("bond_volume_number", volumeNumber)
    .eq("bond_book_number", bookNumber)
    .limit(1);

  if (currentError) {
    throw new Error(`Failed to load current bond member state: ${currentError.message}`);
  }

  const currentRow = Array.isArray(currentRows) && currentRows.length ? currentRows[0] : null;

  if (!currentRow) {
    throw new Error(`Bond member book state was not found for Volume ${volumeNumber} Book ${bookNumber}.`);
  }

  const currentStatus = safeLower(currentRow.status);
  const hasProgress =
    safeNumber(currentRow.minutes_accumulated, 0) > 0 ||
    safeNumber(currentRow.auric_accumulated, 0) > 0 ||
    !!currentRow.started_at;

  if (currentStatus === "completed") {
    return {
      updatedRow: currentRow,
      already_stopped: true,
      reason: "completed"
    };
  }

  if (!["active", "paused"].includes(currentStatus) && !hasProgress) {
    return {
      updatedRow: currentRow,
      already_stopped: true,
      reason: "not_active"
    };
  }

  const nextStatus = currentStatus === "completed" ? "completed" : "paused";

  const payload = {
    status: nextStatus,
    paused_at: nowIso,
    updated_at: nowIso
  };

  const { data: updatedRows, error: updateError } = await partnerSupabase
    .from("partner_bond_member_book_states")
    .update(payload)
    .eq("id", currentRow.id)
    .select("*")
    .limit(1);

  if (updateError) {
    throw new Error(`Failed to pause bond member book state: ${updateError.message}`);
  }

  const updatedRow = Array.isArray(updatedRows) && updatedRows.length ? updatedRows[0] : null;

  return {
    updatedRow: updatedRow || { ...currentRow, ...payload },
    already_stopped: false,
    reason: "paused"
  };
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

const handler = async (event) => {
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
    const query = event.queryStringParameters || {};
    const requestSource = { ...query, ...body };

    const cookieHeader =
      event.headers?.cookie ||
      event.headers?.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionCookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = safeText(cookies[sessionCookieName]);

    const slAvatarKey = safeText(requestSource.sl_avatar_key);
    const slUsername = safeLower(requestSource.sl_username);

    const requestedPartnershipUuid = safeText(
      requestSource.partnership_uuid ||
      requestSource.partnership_id_uuid ||
      requestSource.selected_partnership_uuid
    );

    const requestedLegacyPartnershipId =
      parsePositiveInteger(
        requestSource.partnership_id ||
        requestSource.legacy_partnership_id
      ) || null;

    const requestedVolumeNumber = parsePositiveInteger(requestSource.volume_number);
    const requestedBookNumber = parsePositiveInteger(requestSource.book_number);

    let sessionRow = null;
    let member = null;

    if (sessionToken) {
      const sessionResult = await loadMemberFromSessionToken(sessionToken);
      sessionRow = sessionResult.sessionRow;
      member = sessionResult.member;

      if (!sessionRow) {
        return buildResponse(401, {
          success: false,
          message: "Invalid or expired website session."
        });
      }
    } else {
      if (!slAvatarKey && !slUsername) {
        return buildResponse(400, {
          success: false,
          message: "sl_avatar_key or sl_username is required."
        });
      }

      member = await loadMember(slAvatarKey, slUsername);
    }

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    if (!requestedVolumeNumber) {
      return buildResponse(400, {
        success: false,
        message: "A valid volume_number is required."
      });
    }

    if (!requestedBookNumber) {
      return buildResponse(400, {
        success: false,
        message: "A valid book_number is required."
      });
    }

    await touchSessionAndPresence(sessionToken || null, member);

    const resolvedPartnership = await resolveBondPartnershipForMember({
      member,
      requestedPartnershipUuid,
      requestedLegacyPartnershipId
    });

    if (!resolvedPartnership.partnership_uuid || !resolvedPartnership.partnership) {
      return buildResponse(409, {
        success: false,
        message: getPartnershipResolutionFailureMessage(resolvedPartnership),
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

    const memberPrimaryId = getMemberPrimaryId(member);
    if (!memberPrimaryId) {
      return buildResponse(500, {
        success: false,
        message: "Cultivation member is missing its primary key."
      });
    }

    let selectedPartnershipUuid = null;

    if (resolvedPartnership.partnership_uuid) {
      await saveSelectedPartnership(memberPrimaryId, resolvedPartnership.partnership_uuid);
      selectedPartnershipUuid = resolvedPartnership.partnership_uuid;
    } else if (isUuid(resolvedPartnership.selected_reference)) {
      selectedPartnershipUuid = resolvedPartnership.selected_reference;
    }

    const activePartnership = resolvedPartnership.partnership;
    const partnershipUuid = safeText(activePartnership.id);
    const legacyPartnershipId = parsePositiveInteger(activePartnership.partnership_id);

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

    const partnerIdentity = getPartnerIdentityFromResolvedPartnership(activePartnership, member);

    const partnerMember = await loadPartnerMemberByIdentity({
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

    await initializePartnerBondStates(partnershipUuid);

    const pauseResult = await pauseBondBookMember({
      partnershipUuid,
      memberId: memberPrimaryId,
      volumeNumber: requestedVolumeNumber,
      bookNumber: requestedBookNumber
    });

    const [freshMember, freshPartner, displayState, snapshot, bookRows, catalogBook, bondCoreRow] = await Promise.all([
      loadMember(safeText(member.sl_avatar_key), safeLower(member.sl_username)),
      loadMember(safeText(partnerMember.sl_avatar_key), safeLower(partnerMember.sl_username)),
      loadDisplayState({
        partnershipUuid,
        memberId: memberPrimaryId,
        volumeNumber: requestedVolumeNumber,
        bookNumber: requestedBookNumber
      }),
      loadSnapshot({
        partnershipUuid,
        memberId: memberPrimaryId,
        volumeNumber: requestedVolumeNumber,
        bookNumber: requestedBookNumber
      }),
      loadMemberBookRows(
        partnershipUuid,
        [memberPrimaryId, partnerMemberId],
        requestedVolumeNumber,
        requestedBookNumber
      ),
      loadBondBookCatalog(requestedVolumeNumber, requestedBookNumber),
      loadPartnerBondCore(partnershipUuid)
    ]);

    const selfRow =
      (bookRows || []).find((row) => safeText(row?.member_id) === safeText(memberPrimaryId)) || null;

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
          "core_bond_percent",
          bondCoreRow?.bond_percent
        ),
        safeNumber(bondCoreRow?.bond_percent, 0)
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
        ),
        bondCoreRow?.current_stage_name,
        getBondStageName(bondPercent)
      ) || getBondStageName(bondPercent);

    let message = getStopMessage(displayState, requestedBookNumber, requestedVolumeNumber);

    if (pauseResult.already_stopped && pauseResult.reason === "completed") {
      message = `Bond Book ${requestedBookNumber} in Volume ${requestedVolumeNumber} is already completed.`;
    } else if (pauseResult.already_stopped && pauseResult.reason === "not_active") {
      message = `Bond Book ${requestedBookNumber} in Volume ${requestedVolumeNumber} is not currently active.`;
    }

    return buildResponse(200, {
      success: true,
      stopped: !pauseResult.already_stopped,
      already_stopped: pauseResult.already_stopped,
      display_state: displayState,
      paused: displayState === "paused",
      awaiting_partner_completion: displayState === "awaiting_partner_completion",
      ready_to_start: displayState === "ready_to_start",
      available: displayState === "available",
      pair_completed: displayState === "pair_completed",
      message,

      partnership_resolution: {
        requested_partnership_uuid: requestedPartnershipUuid || null,
        requested_partnership_id: requestedLegacyPartnershipId || null,
        selected_partnership_uuid: selectedPartnershipUuid || null,
        selected_partnership_found: Boolean(resolvedPartnership.selected_partnership_found),
        selected_partnership_missing: Boolean(resolvedPartnership.selected_partnership_missing),
        selected_partnership_inactive: Boolean(resolvedPartnership.selected_partnership_inactive),
        selected_partnership_invalid: Boolean(resolvedPartnership.selected_partnership_invalid),
        selected_partnership_required: Boolean(resolvedPartnership.selected_partnership_required),
        explicit_resolution_failed: Boolean(resolvedPartnership.explicit_resolution_failed),
        explicit_resolution_reason: resolvedPartnership.explicit_resolution_reason || null,
        focus_partnership_uuid: partnershipUuid,
        focus_partnership_id: legacyPartnershipId,
        focus_partnership_source: resolvedPartnership.partnership_source || null,
        has_active_partnership: Boolean(resolvedPartnership.has_active_partnership),
        has_multiple_active_partnerships: Boolean(
          resolvedPartnership.has_multiple_active_partnerships
        )
      },

      partnership: {
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId,
        legacy_partnership_id: legacyPartnershipId,
        buyer_role: partnerRole,
        partnership_source: resolvedPartnership.partnership_source || null
      },

      member: {
        id: getMemberPrimaryId(freshMember || member),
        sl_avatar_key: safeText((freshMember || member)?.sl_avatar_key) || null,
        sl_username: safeText((freshMember || member)?.sl_username) || null,
        vestiges: safeNumber((freshMember || member)?.vestiges, 0),
        is_cultivating: detectMeditationState(freshMember || member).is_active
      },

      partner: {
        id: getMemberPrimaryId(freshPartner || partnerMember),
        sl_avatar_key: safeText((freshPartner || partnerMember)?.sl_avatar_key) || null,
        sl_username: safeText((freshPartner || partnerMember)?.sl_username) || null,
        vestiges: safeNumber((freshPartner || partnerMember)?.vestiges, 0),
        is_cultivating: detectMeditationState(freshPartner || partnerMember).is_active
      },

      book: {
        volume_number: requestedVolumeNumber,
        book_number: requestedBookNumber,
        title: safeText(catalogBook?.title, `Bond Book ${requestedBookNumber}`),
        description: safeText(catalogBook?.description) || null,
        display_state: displayState,
        required_minutes: requiredMinutes,
        required_qi: requiredQi,
        cp_cost_each: safeNumber(
          catalogBook?.cp_cost_each,
          safeNumber(selfRow?.offering_cp_spent, 0)
        ),
        token_cost_each: safeNumber(
          catalogBook?.token_cost_each,
          safeNumber(selfRow?.offering_token_spent, 0)
        ),
        auric_drain_per_minute_each: auricDrainPerMinuteEach,

        self: {
          offering_complete: safeBoolean(selfRow?.offering_complete),
          offering_cp_spent: safeNumber(selfRow?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(selfRow?.offering_token_spent, 0),
          offering_completed_at: selfRow?.offering_completed_at || null,
          minutes_accumulated: safeNumber(selfRow?.minutes_accumulated, 0),
          auric_accumulated: safeNumber(selfRow?.auric_accumulated, 0),
          progress_percent: selfProgressPercent,
          status: safeText(selfRow?.status) || null,
          started_at: selfRow?.started_at || null,
          paused_at: selfRow?.paused_at || null,
          completed_at: selfRow?.completed_at || null
        },

        partner: {
          offering_complete: safeBoolean(partnerRow?.offering_complete),
          offering_cp_spent: safeNumber(partnerRow?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(partnerRow?.offering_token_spent, 0),
          offering_completed_at: partnerRow?.offering_completed_at || null,
          minutes_accumulated: safeNumber(partnerRow?.minutes_accumulated, 0),
          auric_accumulated: safeNumber(partnerRow?.auric_accumulated, 0),
          progress_percent: partnerProgressPercent,
          status: safeText(partnerRow?.status) || null,
          started_at: partnerRow?.started_at || null,
          paused_at: partnerRow?.paused_at || null,
          completed_at: partnerRow?.completed_at || null
        },

        pair: {
          offering_complete:
            safeBoolean(selfRow?.offering_complete) &&
            safeBoolean(partnerRow?.offering_complete),
          pair_completed: displayState === "pair_completed"
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
          safeNumber(bondCoreRow?.completed_books_count, 0)
        ),
        total_shared_minutes: safeNumber(
          readCoreField(
            snapshot,
            "total_shared_minutes",
            "shared_total_minutes"
          ),
          safeNumber(bondCoreRow?.total_shared_minutes, 0)
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
    console.error("stop-bond-cultivation error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to stop bond cultivation.",
      error: error.message || "Unknown error."
    });
  }
};

module.exports = { handler };