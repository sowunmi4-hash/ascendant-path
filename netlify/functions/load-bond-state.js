const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
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
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
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

function getMemberId(row) {
  return safeText(row?.member_id || row?.id);
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

function parsePositiveInteger(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function normalizeIdentifier(value) {
  return safeLower(value).replace(/\s+/g, "");
}

function sameValue(a, b) {
  return safeLower(a) === safeLower(b);
}

function requireId(value, label) {
  const clean = safeText(value);
  const lowered = safeLower(clean);

  if (!clean || lowered === "undefined" || lowered === "null") {
    throw new Error(`Missing valid ${label}.`);
  }

  return clean;
}

function formatMinutes(value) {
  const minutes = Math.max(0, Math.floor(safeNumber(value, 0)));
  if (minutes >= 60) {
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return mins > 0 ? `${hours}h ${mins}m` : `${hours}h`;
  }
  return `${minutes}m`;
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

function detectCultivationState(member) {
  const status = safeLower(member?.v2_cultivation_status);
  const isActive = status === "cultivating";

  return {
    is_active: isActive,
    status: status || "idle",
    started_at: member?.v2_cultivation_started_at || null
  };
}

function getStableRealmStageKey(member) {
  return safeText(member?.v2_active_stage_key) || null;
}

function getStableRealmStageLabel(member) {
  const stageKey = safeText(member?.v2_active_stage_key);
  if (!stageKey) return null;
  // Derive a human-readable label from the stage key
  // e.g. "1:qi_condensation:early" -> "Qi Condensation (Early)"
  const parts = stageKey.split(":");
  if (parts.length < 2) return stageKey;
  const stageName = parts[1].replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const sub = parts[2] ? ` (${parts[2].replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())})` : "";
  return `${stageName}${sub}`;
}

function getActiveRealmVolumeNumber(member) {
  const stageKey = safeText(member?.v2_active_stage_key);
  if (!stageKey) return null;
  const volumePart = stageKey.split(":")[0];
  const num = parsePositiveInteger(volumePart);
  return num || null;
}

function normalizePartnershipRow(row) {
  if (!row) return null;

  return {
    ...row,
    id: requireId(row.id, "partnership_uuid")
  };
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

function getPartnerIdentityFromPartnership(partnershipRow, member) {
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

function isPartnerActive(lastPresenceAt, windowSeconds = 300) {
  if (!lastPresenceAt) return false;

  const presenceMs = new Date(lastPresenceAt).getTime();
  if (!Number.isFinite(presenceMs)) return false;

  const ageSeconds = (Date.now() - presenceMs) / 1000;
  return ageSeconds >= 0 && ageSeconds <= windowSeconds;
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

function getBookPriority(displayState) {
  const state = safeLower(displayState);

  if (state === "awaiting_partner_completion") return 1;
  if (state === "active") return 2;
  if (state === "paused") return 3;
  if (state === "ready_to_start") return 4;
  if (state === "waiting_for_partner_offering") return 5;
  if (state === "ready_for_offering") return 6;
  if (state === "pair_completed") return 7;
  if (state === "locked") return 99;

  return 50;
}

function pickPreferredBondBook(books = []) {
  const rows = Array.isArray(books) ? books : [];
  if (!rows.length) return null;

  return rows
    .slice()
    .sort((a, b) => {
      const aPriority = getBookPriority(a.display_state);
      const bPriority = getBookPriority(b.display_state);

      if (aPriority !== bPriority) return aPriority - bPriority;
      return safeNumber(a.book_number, 0) - safeNumber(b.book_number, 0);
    })[0];
}

function pickPreferredBondVolume(volumes = []) {
  const rows = Array.isArray(volumes) ? volumes : [];
  if (!rows.length) return null;

  const ranked = rows
    .map((volume) => {
      const preferredBook = pickPreferredBondBook(volume.books || []);
      return {
        volume,
        score: preferredBook ? getBookPriority(preferredBook.display_state) : 100,
        bookNumber: preferredBook ? safeNumber(preferredBook.book_number, 0) : 999
      };
    })
    .sort((a, b) => {
      if (a.score !== b.score) return a.score - b.score;
      if (a.bookNumber !== b.bookNumber) return a.bookNumber - b.bookNumber;
      return safeNumber(a.volume.volume_number, 0) - safeNumber(b.volume.volume_number, 0);
    });

  return ranked[0]?.volume || rows[0] || null;
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
    .select("*")
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
    .select("*")
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
    .select("*")
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

  const memberId = getMemberId(member);
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

function buildPartnershipContextFromResolved(member, resolvedPartnership) {
  const partnershipRow = resolvedPartnership?.partnership || null;

  if (!partnershipRow) {
    return {
      has_partner: false,
      partnership_uuid: null,
      partnership_id: null,
      partnership_key: null,
      legacy_partnership_id: null,
      partner_sl_avatar_key: null,
      partner_sl_username: null,
      buyer_role: null,
      partnership_source: resolvedPartnership?.partnership_source || null
    };
  }

  const partnerIdentity = getPartnerIdentityFromPartnership(partnershipRow, member);
  const partnershipUuid = safeText(partnershipRow.id) || null;
  const legacyPartnershipId = parsePositiveInteger(partnershipRow.partnership_id);

  return {
    has_partner: true,
    partnership_uuid: partnershipUuid,
    partnership_id: legacyPartnershipId,
    partnership_key: partnershipUuid,
    legacy_partnership_id: legacyPartnershipId,
    partner_sl_avatar_key: partnerIdentity.partner_avatar_key,
    partner_sl_username: partnerIdentity.partner_sl_username,
    buyer_role: resolvedPartnership?.buyer_role || null,
    partnership_source: resolvedPartnership?.partnership_source || null
  };
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
    console.error("load-bond-state partner lookup error:", error);
    return null;
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
    throw new Error(`Failed to load partner members batch: ${error.message}`);
  }

  const map = {};
  for (const row of data || []) {
    const key = safeText(row?.sl_avatar_key);
    if (key) map[key] = row;
  }

  return map;
}

function buildActivePartnershipSummaries(rows, member, partnerMap, selectedUuid, focusUuid) {
  return (rows || []).map((row) => {
    const partnerIdentity = getPartnerIdentityFromPartnership(row, member);
    const partnerRow = partnerMap[safeText(partnerIdentity.partner_avatar_key)] || null;
    const partnershipUuid = safeText(row?.id) || null;

    return {
      partnership_uuid: partnershipUuid,
      partnership_id: parsePositiveInteger(row?.partnership_id),
      partnership_status: safeText(row?.status) || null,
      buyer_role: getPartnerRole(
        row,
        safeText(member?.sl_avatar_key),
        safeLower(member?.sl_username)
      ) || null,
      partner_avatar_key: safeText(partnerIdentity.partner_avatar_key) || null,
      partner_username: safeText(partnerRow?.sl_username, partnerIdentity.partner_sl_username) || null,
      partner_is_online: false,
      is_selected: partnershipUuid && partnershipUuid === safeText(selectedUuid),
      is_focus: partnershipUuid && partnershipUuid === safeText(focusUuid),
      updated_at: row?.updated_at || null
    };
  });
}

async function loadBondVolumes() {
  const { data, error } = await supabase
    .schema("partner")
    .from("bond_volumes")
    .select("*")
    .eq("is_active", true)
    .order("volume_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond volumes: ${error.message}`);
  }

  return data || [];
}

async function loadBondBooks() {
  const { data, error } = await supabase
    .schema("partner")
    .from("bond_books")
    .select("*")
    .eq("is_active", true)
    .order("sort_order", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond books: ${error.message}`);
  }

  return data || [];
}

async function loadBondStoreItems() {
  const { data, error } = await supabase
    .schema("library")
    .from("library_store_items")
    .select(`
      id,
      item_key,
      category,
      item_type,
      volume_number,
      item_name,
      description,
      price_currency,
      price_amount,
      stock,
      is_active,
      updated_at
    `)
    .eq("category", "bond")
    .order("volume_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond store items: ${error.message}`);
  }

  return data || [];
}

async function initializePartnerBondStates(partnershipUuid) {
  const cleanId = safeText(partnershipUuid);
  if (!isUuid(cleanId)) return null;

  const { error } = await supabase.schema("partner").rpc(
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

async function loadPartnerBondMemberBookStates(partnershipUuid, memberIds = []) {
  const cleanId = safeText(partnershipUuid);
  if (!isUuid(cleanId)) return [];

  let query = supabase
    .schema("partner")
    .from("partner_bond_member_book_states")
    .select("*")
    .eq("partnership_uuid", cleanId)
    .order("bond_volume_number", { ascending: true })
    .order("bond_book_number", { ascending: true });

  const cleanMemberIds = (memberIds || []).map((id) => safeText(id)).filter(Boolean);
  if (cleanMemberIds.length) {
    query = query.in("member_id", cleanMemberIds);
  }

  const { data, error } = await query;

  if (error) {
    throw new Error(`Failed to load partner bond member book states: ${error.message}`);
  }

  return data || [];
}

async function loadPartnerDisplayState({
  partnershipUuid,
  memberId,
  bondVolumeNumber,
  bondBookNumber
}) {
  const { data, error } = await supabase.schema("partner").rpc(
    "get_partner_bond_member_book_display_state",
    {
      p_partnership_uuid: partnershipUuid,
      p_member_id: memberId,
      p_bond_volume_number: bondVolumeNumber,
      p_bond_book_number: bondBookNumber
    }
  );

  if (error) {
    throw new Error(
      `Failed to load bond display state for V${bondVolumeNumber} B${bondBookNumber}: ${error.message}`
    );
  }

  return normalizeDisplayStateResult(data, "locked");
}

async function loadPartnerSnapshot({
  partnershipUuid,
  memberId,
  bondVolumeNumber,
  bondBookNumber
}) {
  const { data, error } = await supabase.schema("partner").rpc(
    "get_partner_bond_member_book_snapshot",
    {
      p_partnership_uuid: partnershipUuid,
      p_member_id: memberId,
      p_bond_volume_number: bondVolumeNumber,
      p_bond_book_number: bondBookNumber
    }
  );

  if (error) {
    throw new Error(
      `Failed to load bond snapshot for V${bondVolumeNumber} B${bondBookNumber}: ${error.message}`
    );
  }

  return normalizeRpcRow(data);
}

function buildUnavailableBondRecord({
  resolvedPartnership,
  partnerContext,
  partnerMember,
  requestedVolumeNumber
}) {
  let message = "Bond Shrine remains dormant until a valid active partnership exists.";

  if (!resolvedPartnership.has_active_partnership) {
    message = "Bond Shrine remains dormant until a valid active partnership exists.";
  } else if (resolvedPartnership.selected_partnership_required) {
    message = "Multiple active partnerships were found. Select a partnership first before opening the Bond Book.";
  } else if (resolvedPartnership.selected_partnership_missing) {
    message = "The saved selected partnership no longer exists. Re-select a partnership before opening the Bond Book.";
  } else if (resolvedPartnership.selected_partnership_inactive) {
    message = "The saved selected partnership is not active. Re-select an active partnership before opening the Bond Book.";
  } else if (resolvedPartnership.selected_partnership_invalid) {
    message = "The saved selected partnership is invalid for this member. Re-select a valid partnership before opening the Bond Book.";
  } else if (resolvedPartnership.explicit_resolution_failed) {
    message = "The requested partnership could not be resolved for the Bond Book.";
  }

  const partnershipUuid = partnerContext.partnership_uuid || null;
  const partnershipId = partnerContext.partnership_id || null;

  return {
    available: false,
    has_partner: Boolean(resolvedPartnership.has_active_partnership),
    partnership_uuid: partnershipUuid,
    partnership_id: partnershipId,
    legacy_partnership_id: partnershipId,
    partner_role: null,
    message,
    partner: partnerContext.has_partner
      ? {
          sl_avatar_key: partnerContext.partner_sl_avatar_key || partnerMember?.sl_avatar_key || null,
          sl_username: partnerContext.partner_sl_username || partnerMember?.sl_username || null,
          display_name: partnerMember?.display_name || null,
          realm_name: partnerMember?.realm_name || null,
          realm_display_name: partnerMember?.realm_display_name || null,
          path_type: partnerMember?.path_type || null,
          cultivation_active: safeLower(partnerMember?.v2_cultivation_status) === "cultivating",
          is_online: false
        }
      : null,
    summary: {
      purchased_volumes: 0,
      total_books: 0,
      completed_books: 0,
      active_books: 0,
      bond_percent: 0,
      bond_stage: "Bond Seed",
      partnership_uuid: partnershipUuid,
      partnership_id: partnershipId,
      total_shared_minutes: 0,
      completed_books_count: 0,
      shared_offering_cp_total: 0,
      shared_offering_token_total: 0
    },
    selected_volume_number: requestedVolumeNumber || null,
    selected_book_number: null,
    selected_book: null,
    actionable_book: null,
    current_book: null,
    active_book: null,
    selected_volume: null,
    volume: null,
    volumes: [],
    active_session: null,
    selected_snapshot: null
  };
}

async function buildBondRecord({
  member,
  resolvedPartnership,
  partnershipRow,
  partnerContext,
  partnerMember,
  requestedVolumeNumber,
  warnings
}) {
  const selfMemberId = getMemberId(member);
  const partnerMemberId = getMemberId(partnerMember);

  if (!partnerContext?.has_partner || !partnershipRow || !selfMemberId || !partnerMemberId) {
    return buildUnavailableBondRecord({
      resolvedPartnership,
      partnerContext,
      partnerMember,
      requestedVolumeNumber
    });
  }

  const bondPartnershipUuid = safeText(
    partnerContext?.partnership_uuid || partnershipRow?.id
  );
  const legacyPartnershipId = parsePositiveInteger(
    partnerContext?.partnership_id || partnershipRow?.partnership_id
  );

  await initializePartnerBondStates(bondPartnershipUuid);

  const [bondVolumes, bondBooks, bondStoreItems, memberRows] = await Promise.all([
    loadBondVolumes(),
    loadBondBooks(),
    loadBondStoreItems(),
    loadPartnerBondMemberBookStates(bondPartnershipUuid, [selfMemberId, partnerMemberId])
  ]);

  const volumeIdToNumber = new Map();
  for (const volume of bondVolumes || []) {
    const volumeId = safeText(volume?.id);
    const volumeNumber = safeNumber(volume?.volume_number, 0);
    if (volumeId && volumeNumber > 0) {
      volumeIdToNumber.set(volumeId, volumeNumber);
    }
  }

  const storeByVolume = new Map();
  for (const item of bondStoreItems || []) {
    const volumeNumber = safeNumber(item?.volume_number, 0);
    if (volumeNumber > 0 && !storeByVolume.has(volumeNumber)) {
      storeByVolume.set(volumeNumber, item);
    }
  }

  const selfRowMap = new Map();
  const partnerRowMap = new Map();

  for (const row of memberRows || []) {
    const key = `${safeNumber(row?.bond_volume_number, 0)}:${safeNumber(row?.bond_book_number, 0)}`;
    if (safeText(row?.member_id) === selfMemberId) {
      selfRowMap.set(key, row);
    } else if (safeText(row?.member_id) === partnerMemberId) {
      partnerRowMap.set(key, row);
    }
  }

  const displayStateTasks = (bondBooks || []).map(async (book) => {
    const volumeNumber = safeNumber(
      book?.volume_number,
      volumeIdToNumber.get(safeText(book?.volume_id)) || 0
    );
    const bookNumber = safeNumber(book?.book_number, 0);
    const key = `${volumeNumber}:${bookNumber}`;

    try {
      const displayState = await loadPartnerDisplayState({
        partnershipUuid: bondPartnershipUuid,
        memberId: selfMemberId,
        bondVolumeNumber: volumeNumber,
        bondBookNumber: bookNumber
      });

      return [key, displayState];
    } catch (error) {
      warnings.push(`Display state fallback used for Volume ${volumeNumber} Book ${bookNumber}.`);
      return [key, safeLower(selfRowMap.get(key)?.status || "locked") || "locked"];
    }
  });

  const displayStateEntries = await Promise.all(displayStateTasks);
  const displayStateMap = new Map(displayStateEntries);

  const booksByVolumeNumber = new Map();
  for (const book of bondBooks || []) {
    const volumeNumber = safeNumber(
      book?.volume_number,
      volumeIdToNumber.get(safeText(book?.volume_id)) || 0
    );
    if (!booksByVolumeNumber.has(volumeNumber)) {
      booksByVolumeNumber.set(volumeNumber, []);
    }
    booksByVolumeNumber.get(volumeNumber).push(book);
  }

  const builtVolumes = (bondVolumes || []).map((volume, volumeIndex) => {
    const volumeNumber = safeNumber(volume?.volume_number, volumeIndex + 1);
    const rawBooks = (booksByVolumeNumber.get(volumeNumber) || [])
      .slice()
      .sort((a, b) => safeNumber(a.book_number, 0) - safeNumber(b.book_number, 0));

    const builtBooks = rawBooks.map((book, bookIndex) => {
      const bookNumber = safeNumber(book?.book_number, bookIndex + 1);
      const key = `${volumeNumber}:${bookNumber}`;
      const selfState = selfRowMap.get(key) || null;
      const partnerState = partnerRowMap.get(key) || null;
      const displayState = displayStateMap.get(key) || "locked";

      const requiredMinutes = Math.max(
        0,
        safeNumber(selfState?.required_minutes, book?.required_minutes || book?.required_shared_minutes || 0)
      );

      const qiDrainPerMinuteEach = Math.max(
        0,
        safeNumber(book?.qi_drain_per_minute_each, 0)
      );

      const requiredQi = Math.max(
        0,
        safeNumber(selfState?.required_qi, requiredMinutes * qiDrainPerMinuteEach)
      );

      const selfProgressPercent = computeIndividualProgressPercent(
        selfState,
        requiredMinutes,
        requiredQi
      );

      const partnerProgressPercent = computeIndividualProgressPercent(
        partnerState,
        requiredMinutes,
        requiredQi
      );

      const selfOfferingComplete = safeBoolean(selfState?.offering_complete);
      const partnerOfferingComplete = safeBoolean(partnerState?.offering_complete);
      const pairCompleted = displayState === "pair_completed";

      return {
        id: safeText(book?.id) || null,
        volume_id: safeText(book?.volume_id) || null,
        volume_number: volumeNumber,
        book_number: bookNumber,
        book_name: safeText(book?.title, `Bond Book ${bookNumber}`),
        book_label: `Book ${bookNumber}`,
        description: safeText(book?.description) || null,

        display_state: displayState,
        status: displayState,

        is_locked: displayState === "locked",
        is_ready_for_offering: displayState === "ready_for_offering",
        is_waiting_for_partner_offering: displayState === "waiting_for_partner_offering",
        is_ready_to_start: displayState === "ready_to_start",
        is_active: displayState === "active",
        is_paused: displayState === "paused",
        is_awaiting_partner_completion: displayState === "awaiting_partner_completion",
        is_pair_completed: pairCompleted,
        is_completed: pairCompleted,

        self: {
          member_id: safeText(selfState?.member_id) || selfMemberId || null,
          row_id: safeText(selfState?.id) || null,
          status: safeText(selfState?.status) || null,
          offering_complete: selfOfferingComplete,
          offering_cp_spent: safeNumber(selfState?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(selfState?.offering_token_spent, 0),
          offering_completed_at: selfState?.offering_completed_at || null,
          minutes_accumulated: safeNumber(selfState?.minutes_accumulated, 0),
          required_minutes: requiredMinutes,
          qi_accumulated: safeNumber(selfState?.qi_accumulated, 0),
          required_qi: requiredQi,
          progress_percent: selfProgressPercent,
          started_at: selfState?.started_at || null,
          last_progress_at: selfState?.last_progress_at || null,
          paused_at: selfState?.paused_at || null,
          completed_at: selfState?.completed_at || null,
          updated_at: selfState?.updated_at || null
        },

        partner: {
          member_id: safeText(partnerState?.member_id) || partnerMemberId || null,
          row_id: safeText(partnerState?.id) || null,
          status: safeText(partnerState?.status) || null,
          offering_complete: partnerOfferingComplete,
          offering_cp_spent: safeNumber(partnerState?.offering_cp_spent, 0),
          offering_token_spent: safeNumber(partnerState?.offering_token_spent, 0),
          offering_completed_at: partnerState?.offering_completed_at || null,
          minutes_accumulated: safeNumber(partnerState?.minutes_accumulated, 0),
          required_minutes: requiredMinutes,
          qi_accumulated: safeNumber(partnerState?.qi_accumulated, 0),
          required_qi: requiredQi,
          progress_percent: partnerProgressPercent,
          started_at: partnerState?.started_at || null,
          last_progress_at: partnerState?.last_progress_at || null,
          paused_at: partnerState?.paused_at || null,
          completed_at: partnerState?.completed_at || null,
          updated_at: partnerState?.updated_at || null
        },

        pair: {
          offering_complete: selfOfferingComplete && partnerOfferingComplete,
          pair_completed: pairCompleted
        },

        cp_cost_each: Math.max(0, safeNumber(book?.cp_cost_each, 0)),
        token_cost_each: Math.max(0, safeNumber(book?.token_cost_each, 0)),
        qi_drain_per_minute_each: qiDrainPerMinuteEach,
        required_minutes: requiredMinutes,
        required_qi: requiredQi,
        human_required_minutes: formatMinutes(requiredMinutes),
        human_self_minutes: formatMinutes(selfState?.minutes_accumulated),
        human_partner_minutes: formatMinutes(partnerState?.minutes_accumulated),

        percent_start: safeNumber(book?.percent_start, 0),
        percent_end: safeNumber(book?.percent_end, 0),
        created_at: book?.created_at || null,
        updated_at: firstFilled(selfState?.updated_at, book?.updated_at) || null
      };
    });

    const preferredBook = pickPreferredBondBook(builtBooks);
    const completedBooks = builtBooks.filter((book) => book.is_pair_completed).length;
    const unlocked = builtBooks.some((book) => book.display_state !== "locked");

    let volumeStatus = "locked";
    if (builtBooks.length && completedBooks === builtBooks.length) {
      volumeStatus = "completed";
    } else if (preferredBook) {
      volumeStatus = preferredBook.display_state;
    } else if (unlocked) {
      volumeStatus = "ready_for_offering";
    }

    const progressPercent = builtBooks.length
      ? Number(
          (
            builtBooks.reduce((sum, book) => {
              if (book.is_pair_completed) return sum + 100;
              return sum + safeNumber(book.self.progress_percent, 0);
            }, 0) / builtBooks.length
          ).toFixed(2)
        )
      : 0;

    const storeRow = storeByVolume.get(volumeNumber) || null;

    return {
      id: safeText(volume?.id) || null,
      volume_number: volumeNumber,
      item_name: safeText(volume?.title, `Bond Volume ${volumeNumber}`),
      stage_name: safeText(volume?.stage_name) || null,
      description: firstFilled(storeRow?.description, volume?.description) || null,
      volume_status: volumeStatus,
      progress_percent: clamp(progressPercent, 0, 100),
      is_unlocked: unlocked,
      is_completed: volumeStatus === "completed",
      is_active: volumeStatus === "active",
      is_paused: volumeStatus === "paused",
      has_ready_for_offering_book: builtBooks.some((book) => book.is_ready_for_offering),
      has_waiting_for_partner_offering_book: builtBooks.some((book) => book.is_waiting_for_partner_offering),
      has_ready_to_start_book: builtBooks.some((book) => book.is_ready_to_start),
      has_awaiting_partner_completion_book: builtBooks.some((book) => book.is_awaiting_partner_completion),
      books_total: builtBooks.length,
      books_completed: completedBooks,
      price_currency: firstFilled(storeRow?.price_currency) || null,
      price_amount: safeNumber(storeRow?.price_amount, 0),
      stock: storeRow?.stock ?? null,
      actionable_book_number: preferredBook?.book_number || null,
      actionable_book_name: preferredBook?.book_name || null,
      actionable_book_status: preferredBook?.display_state || null,
      books: builtBooks
    };
  });

  const requested = parsePositiveInteger(requestedVolumeNumber);
  const preferredVolume = pickPreferredBondVolume(builtVolumes);

  const selectedVolumeNumber =
    requested ||
    safeNumber(preferredVolume?.volume_number, 0) ||
    safeNumber(builtVolumes[0]?.volume_number, 0) ||
    1;

  const selectedVolume =
    builtVolumes.find((volume) => safeNumber(volume.volume_number, 0) === selectedVolumeNumber) ||
    builtVolumes[0] ||
    null;

  const selectedBook = pickPreferredBondBook(selectedVolume?.books || []);

  let selectedSnapshot = null;
  if (selectedBook) {
    try {
      selectedSnapshot = await loadPartnerSnapshot({
        partnershipUuid: bondPartnershipUuid,
        memberId: selfMemberId,
        bondVolumeNumber: selectedBook.volume_number,
        bondBookNumber: selectedBook.book_number
      });
    } catch (error) {
      warnings.push(
        `Snapshot fallback used for Volume ${selectedBook.volume_number} Book ${selectedBook.book_number}.`
      );
    }
  }

  const allBooks = builtVolumes.flatMap((volume) => volume.books || []);
  const pairCompletedBookRows = allBooks.filter((book) => book.is_pair_completed);
  const pairCompletedBooks = pairCompletedBookRows.length;
  const completedSharedMinutesFallback = pairCompletedBookRows.reduce(
    (sum, book) => sum + safeNumber(book.required_minutes, 0),
    0
  );

  const bondPercent = clamp(
    safeNumber(
      readCoreField(
        selectedSnapshot,
        "bond_percent",
        "shared_bond_percent",
        "core_bond_percent"
      ),
      pairCompletedBooks * 5
    ),
    0,
    100
  );

  const bondStage =
    firstFilled(
      readCoreField(
        selectedSnapshot,
        "current_stage_name",
        "bond_stage_name",
        "stage_name"
      )
    ) || getBondStageName(bondPercent);

  const activeSessionStatus = selectedBook?.display_state || "idle";
  let pauseReason = null;

  if (activeSessionStatus === "paused") {
    pauseReason = "self_paused";
  } else if (activeSessionStatus === "waiting_for_partner_offering") {
    pauseReason = "waiting_for_partner_offering";
  } else if (activeSessionStatus === "awaiting_partner_completion") {
    pauseReason = "awaiting_partner_completion";
  }

  const activeSession = selectedBook
    ? {
        status: activeSessionStatus,
        display_state: activeSessionStatus,
        partnership_uuid: bondPartnershipUuid || null,
        partnership_id: legacyPartnershipId || null,
        volume_number: selectedBook.volume_number,
        book_number: selectedBook.book_number,
        bond_volume_number: selectedBook.volume_number,
        bond_book_number: selectedBook.book_number,
        self_status: selectedBook.self.status,
        partner_status: selectedBook.partner.status,
        self_offering_complete: selectedBook.self.offering_complete,
        partner_offering_complete: selectedBook.partner.offering_complete,
        both_offering_complete: selectedBook.pair.offering_complete,
        pair_offering_complete: selectedBook.pair.offering_complete,
        self_progress_percent: selectedBook.self.progress_percent,
        partner_progress_percent: selectedBook.partner.progress_percent,
        self_minutes_accumulated: safeNumber(selectedBook.self.minutes_accumulated, 0),
        partner_minutes_accumulated: safeNumber(selectedBook.partner.minutes_accumulated, 0),
        self_qi_accumulated: safeNumber(selectedBook.self.qi_accumulated, 0),
        partner_qi_accumulated: safeNumber(selectedBook.partner.qi_accumulated, 0),
        pair_completed: selectedBook.pair.pair_completed,
        pause_reason: pauseReason,
        self_started_at: selectedBook.self.started_at,
        self_last_progress_at: selectedBook.self.last_progress_at,
        self_paused_at: selectedBook.self.paused_at,
        self_completed_at: selectedBook.self.completed_at,
        updated_at: firstFilled(
          selectedBook.self.updated_at,
          selectedBook.partner.updated_at,
          selectedSnapshot?.updated_at
        ) || null
      }
    : null;

  const bondCore = {
    partnership_uuid: bondPartnershipUuid || null,
    partnership_id: legacyPartnershipId || null,
    bond_percent: Number(bondPercent.toFixed(2)),
    stage_name: bondStage,
    completed_books_count: safeNumber(
      readCoreField(
        selectedSnapshot,
        "completed_books_count",
        "pair_completed_books_count"
      ),
      pairCompletedBooks
    ),
    total_shared_minutes: safeNumber(
      readCoreField(
        selectedSnapshot,
        "total_shared_minutes",
        "shared_total_minutes"
      ),
      completedSharedMinutesFallback
    )
  };

  return {
    available: true,
    has_partner: true,
    partnership_uuid: bondPartnershipUuid || null,
    partnership_id: legacyPartnershipId || null,
    legacy_partnership_id: legacyPartnershipId || null,
    partner_role: getPartnerRole(
      partnershipRow,
      safeText(member?.sl_avatar_key),
      safeLower(member?.sl_username)
    ) || null,
    message: "Bond book state loaded successfully.",
    partner: {
      sl_avatar_key: partnerContext.partner_sl_avatar_key || null,
      sl_username: partnerContext.partner_sl_username || null,
      display_name: partnerMember?.display_name || null,
      realm_name: partnerMember?.realm_name || null,
      realm_display_name: partnerMember?.realm_display_name || null,
      path_type: partnerMember?.path_type || null,
      cultivation_active: safeLower(partnerMember?.v2_cultivation_status) === "cultivating",
      is_online: false
    },
    summary: {
      purchased_volumes: builtVolumes.filter((volume) => volume.is_unlocked || volume.is_completed).length,
      total_books: allBooks.length,
      completed_books: pairCompletedBooks,
      active_books: allBooks.filter((book) => book.is_active || book.is_paused || book.is_awaiting_partner_completion).length,
      bond_percent: Number(bondPercent.toFixed(2)),
      bond_stage: bondStage,
      active_volume_number: selectedVolume?.volume_number || null,
      active_book_number: selectedBook?.book_number || null,
      active_book_display_state: selectedBook?.display_state || null,
      partnership_uuid: bondPartnershipUuid || null,
      partnership_id: legacyPartnershipId || null,
      total_shared_minutes: bondCore.total_shared_minutes,
      completed_books_count: bondCore.completed_books_count,
      shared_offering_cp_total: safeNumber(
        readCoreField(
          selectedSnapshot,
          "shared_offering_cp_total",
          "offering_cp_total",
          "total_offering_cp"
        ),
        0
      ),
      shared_offering_token_total: safeNumber(
        readCoreField(
          selectedSnapshot,
          "shared_offering_token_total",
          "offering_token_total",
          "total_offering_token"
        ),
        0
      )
    },
    selected_volume_number: selectedVolume ? safeNumber(selectedVolume.volume_number, 0) : null,
    selected_book_number: selectedBook ? safeNumber(selectedBook.book_number, 0) : null,
    selected_book: selectedBook || null,
    actionable_book: selectedBook || null,
    current_book: selectedBook || null,
    active_book: selectedBook || null,
    selected_volume: selectedVolume || null,
    volume: selectedVolume,
    volumes: builtVolumes,
    active_session: activeSession,
    selected_snapshot: selectedSnapshot || null,
    bond_core: bondCore
  };
}

async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use GET or POST."
    });
  }

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, {
        success: false,
        message: "Missing Supabase environment variables."
      });
    }

    const body = parseBody(event);
    const query = event.queryStringParameters || {};
    const requestSource = { ...query, ...body };

    const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
    const cookies = parseCookies(cookieHeader);
    const cookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = safeText(cookies[cookieName]);

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

    const requestedVolumeNumber =
      parsePositiveInteger(requestSource.volume_number) ||
      parsePositiveInteger(requestSource.volume) ||
      parsePositiveInteger(requestSource.bond_volume_number) ||
      null;

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

    const memberId = getMemberId(member);

    await touchSessionAndPresence(sessionToken || null, member);

    const warnings = [];
    const cultivationState = detectCultivationState(member);

    const activePartnershipRows = await loadActivePartnershipRowsForMember(member);

    const resolvedPartnership = await resolveBondPartnershipForMember({
      member,
      requestedPartnershipUuid,
      requestedLegacyPartnershipId
    });

    let persistedSelectedPartnershipUuid = null;

    if (resolvedPartnership.partnership_uuid && memberId) {
      await saveSelectedPartnership(memberId, resolvedPartnership.partnership_uuid);
      persistedSelectedPartnershipUuid = resolvedPartnership.partnership_uuid;
    } else if (isUuid(resolvedPartnership.selected_reference)) {
      persistedSelectedPartnershipUuid = resolvedPartnership.selected_reference;
    }

    const partnership = buildPartnershipContextFromResolved(member, resolvedPartnership);

    const allCounterpartAvatarKeys = activePartnershipRows
      .map((row) => getPartnerIdentityFromPartnership(row, member).partner_avatar_key)
      .filter(Boolean);

    const partnerMap = await loadMembersByAvatarKeys(allCounterpartAvatarKeys);

    const partnerMember = partnership.has_partner
      ? partnerMap[safeText(partnership.partner_sl_avatar_key)] ||
        (await loadPartnerMemberRow({
          partnerAvatarKey: partnership.partner_sl_avatar_key,
          partnerUsername: partnership.partner_sl_username
        }))
      : null;

    let bondRecord = null;

    try {
      bondRecord = await buildBondRecord({
        member,
        resolvedPartnership,
        partnershipRow: resolvedPartnership.partnership,
        partnerContext: partnership,
        partnerMember,
        requestedVolumeNumber,
        warnings
      });
    } catch (bondError) {
      console.error("load-bond-state build error:", bondError);
      warnings.push("Bond record could not be fully loaded.");
      bondRecord = buildUnavailableBondRecord({
        resolvedPartnership,
        partnerContext: partnership,
        partnerMember,
        requestedVolumeNumber
      });
      bondRecord.message = bondError.message || "Bond record could not be loaded.";
    }

    const activePartnershipSummaries = buildActivePartnershipSummaries(
      activePartnershipRows,
      member,
      partnerMap,
      persistedSelectedPartnershipUuid,
      partnership.partnership_uuid
    );

    const focusPartnerOnline = false;

    const topLevelBondCore = {
      partnership_uuid: bondRecord?.partnership_uuid || partnership.partnership_uuid || null,
      partnership_id: bondRecord?.partnership_id || partnership.partnership_id || null,
      bond_percent: safeNumber(bondRecord?.bond_core?.bond_percent ?? bondRecord?.summary?.bond_percent, 0),
      stage_name: safeText(
        bondRecord?.bond_core?.stage_name || bondRecord?.summary?.bond_stage,
        "Bond Seed"
      ),
      completed_books_count: safeNumber(
        bondRecord?.bond_core?.completed_books_count ?? bondRecord?.summary?.completed_books_count,
        0
      ),
      total_shared_minutes: safeNumber(
        bondRecord?.bond_core?.total_shared_minutes ?? bondRecord?.summary?.total_shared_minutes,
        0
      )
    };

    const topLevelBond = {
      partnership_uuid: bondRecord?.partnership_uuid || partnership.partnership_uuid || null,
      partnership_id: bondRecord?.partnership_id || partnership.partnership_id || null,
      bond_percent: topLevelBondCore.bond_percent,
      current_stage_name: topLevelBondCore.stage_name,
      completed_books_count: topLevelBondCore.completed_books_count,
      total_shared_minutes: topLevelBondCore.total_shared_minutes,
      status: safeText(
        bondRecord?.active_session?.status ||
        bondRecord?.selected_book?.display_state,
        "idle"
      ),
      pause_reason: safeText(bondRecord?.active_session?.pause_reason) || null,
      updated_at: bondRecord?.active_session?.updated_at || null
    };

    return buildResponse(200, {
      success: true,
      message: "Bond book state loaded successfully.",
      mode: "bond",

      user: {
        id: memberId || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username),
        display_name: safeText(member.display_name) || null,
        cultivation_points: safeNumber(member.cultivation_points, 0),
        realm_name: safeText(member.realm_name) || null,
        realm_stage_key: getStableRealmStageKey(member),
        realm_stage_label: getStableRealmStageLabel(member),
        realm_display_name: safeText(member.realm_display_name) || null,
        active_realm_volume_number: getActiveRealmVolumeNumber(member),
        v2_active_stage_key: safeText(member.v2_active_stage_key) || null,
        v2_breakthrough_gate_open: safeBoolean(member.v2_breakthrough_gate_open),
        v2_cultivation_status: safeText(member.v2_cultivation_status) || "idle",
        v2_accumulated_seconds: safeNumber(member.v2_accumulated_seconds, 0),
        v2_sessions_today: safeNumber(member.v2_sessions_today, 0)
      },

      cultivation: {
        is_active: cultivationState.is_active,
        status: cultivationState.status,
        started_at: cultivationState.started_at
      },

      selected_partnership_uuid: persistedSelectedPartnershipUuid || null,
      selected_partnership_id:
        partnership.partnership_uuid && partnership.partnership_uuid === persistedSelectedPartnershipUuid
          ? parsePositiveInteger(partnership.partnership_id)
          : null,

      partnership_resolution: {
        requested_partnership_uuid: requestedPartnershipUuid || null,
        requested_partnership_id: requestedLegacyPartnershipId || null,

        selected_partnership_uuid: persistedSelectedPartnershipUuid || null,
        selected_partnership_found: Boolean(resolvedPartnership.selected_partnership_found),
        selected_partnership_missing: Boolean(resolvedPartnership.selected_partnership_missing),
        selected_partnership_inactive: Boolean(resolvedPartnership.selected_partnership_inactive),
        selected_partnership_invalid: Boolean(resolvedPartnership.selected_partnership_invalid),
        selected_partnership_required: Boolean(resolvedPartnership.selected_partnership_required),

        explicit_resolution_failed: Boolean(resolvedPartnership.explicit_resolution_failed),
        explicit_resolution_reason: resolvedPartnership.explicit_resolution_reason || null,

        focus_partnership_uuid: partnership.partnership_uuid || null,
        focus_partnership_id: partnership.partnership_id || null,
        focus_partnership_source: partnership.partnership_source || null,

        active_partnership_count: activePartnershipRows.length,
        has_multiple_active_partnerships: activePartnershipRows.length > 1,
        current_focus_partner_username: safeText(
          partnerMember?.sl_username,
          partnership.partner_sl_username
        ) || null,
        current_focus_partner_online: focusPartnerOnline
      },

      active_partnerships: {
        count: activePartnershipRows.length,
        has_multiple_active_partnerships: activePartnershipRows.length > 1,
        rows: activePartnershipSummaries
      },

      partnership: {
        has_partner: partnership.has_partner,
        has_active_partnership: Boolean(resolvedPartnership.has_active_partnership),
        has_multiple_active_partnerships: Boolean(resolvedPartnership.has_multiple_active_partnerships),
        selected_partnership_required: Boolean(resolvedPartnership.selected_partnership_required),
        selected_partnership_found: Boolean(resolvedPartnership.selected_partnership_found),
        selected_partnership_missing: Boolean(resolvedPartnership.selected_partnership_missing),
        selected_partnership_inactive: Boolean(resolvedPartnership.selected_partnership_inactive),
        selected_partnership_invalid: Boolean(resolvedPartnership.selected_partnership_invalid),
        explicit_resolution_failed: Boolean(resolvedPartnership.explicit_resolution_failed),
        explicit_resolution_reason: resolvedPartnership.explicit_resolution_reason || null,

        partnership_uuid: partnership.partnership_uuid || null,
        partnership_id: partnership.partnership_id || null,
        partnership_key: partnership.partnership_key || null,
        legacy_partnership_id: partnership.legacy_partnership_id || null,
        buyer_role: partnership.buyer_role || null,
        partnership_source: partnership.partnership_source || null,

        partner: partnership.has_partner
          ? {
              id: getMemberId(partnerMember) || null,
              sl_avatar_key: partnership.partner_sl_avatar_key || partnerMember?.sl_avatar_key || null,
              sl_username: partnership.partner_sl_username || safeText(partnerMember?.sl_username) || null,
              display_name: safeText(partnerMember?.display_name) || null,
              realm_name: safeText(partnerMember?.realm_name) || null,
              realm_display_name: safeText(partnerMember?.realm_display_name) || null,
              path_type: safeText(partnerMember?.path_type) || null,
              cultivation_active: safeLower(partnerMember?.v2_cultivation_status) === "cultivating",
              is_online: focusPartnerOnline
            }
          : null
      },

      partner: bondRecord?.partner || null,
      bond_core: topLevelBondCore,
      bond: topLevelBond,
      bond_session: bondRecord?.active_session || null,

      active_record: {
        mode: "bond",
        available: !!bondRecord?.available,
        partnership_uuid: partnership.partnership_uuid || null,
        partnership_id: partnership.partnership_id || null,
        volume_number: bondRecord?.selected_volume_number || null,
        title: bondRecord?.volume?.item_name || null
      },

      bond_record: bondRecord,
      warnings
    });
  } catch (error) {
    console.error("load-bond-state error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to load bond book state.",
      error: error.message || "Unknown error."
    });
  }
}

module.exports = { handler };