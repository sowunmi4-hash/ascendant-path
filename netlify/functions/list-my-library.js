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

const VOLUME_SECTION_BOOK_TITLES = {
  1: { base: "Scripture of Dust and Breath", early: "Manual of the Waking Body", middle: "Canon of Tempered Flesh", late: "Sutra of the Earthbound Vessel" },
  2: { base: "Art of the First Meridian", early: "Breath-Gathering Scripture", middle: "Classic of Flowing Channels", late: "Record of the Spiraling Tide" },
  3: { base: "Foundation Pillar Manual", early: "Sutra of Rooted Essence", middle: "Jade Pillar Canon", late: "Scripture of the Unshaken Base" },
  4: { base: "Treatise on the Inner Crucible", early: "Golden Core Refinement Art", middle: "Canon of Condensed Radiance", late: "Sutra of the Sealed Sun" },
  5: { base: "Awakening of the Inner Spirit", early: "Scripture of the Infant Soul", middle: "Mirror of Divine Consciousness", late: "Canon of the Living Spirit" },
  6: { base: "Manual of the Broken Fetters", early: "Sutra of Cleaved Illusions", middle: "Canon of the Empty Bond", late: "Scripture of the Severed Self" },
  7: { base: "Art of the Hollow Expanse", early: "Scripture of Silent Space", middle: "Canon of the Formless Sky", late: "Voidheart Refinement Sutra" },
  8: { base: "Ladder of the Rising Soul", early: "Scripture of Heaven-Bound Spirit", middle: "Canon of Ascendant Will", late: "Sutra of the Celestial Threshold" },
  9: { base: "Edict of Sacred Presence", early: "Scripture of the Saintly Flame", middle: "Canon of Heaven's Mandate", late: "Sutra of Crowned Divinity" },
  10: { base: "Book of Undying Dawn", early: "Scripture of Eternal Breath", middle: "Canon of Boundless Heaven", late: "Sutra of the Deathless Throne" }
};

const BOND_VOLUME_DEFAULT_TITLES = {
  1: "The Seed of Union",
  2: "The Root of Accord",
  3: "The Core of Communion",
  4: "The Soul of Devotion",
  5: "The Spirit of Eternity"
};

const BOND_BOOK_LABELS = {
  1: "First Book",
  2: "Second Book",
  3: "Third Book",
  4: "Fourth Book"
};

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

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  cookieHeader.split(";").forEach((part) => {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    try { cookies[key] = decodeURIComponent(val); } catch { cookies[key] = val; }
  });
  return cookies;
}

function safeText(value) { return String(value || "").trim(); }
function safeLower(value) { return safeText(value).toLowerCase(); }
function safeNumber(value, fallback) { if (fallback === undefined) fallback = 0; const n = Number(value); return Number.isFinite(n) ? n : fallback; }
function safeBoolean(value) { if (typeof value === "boolean") return value; if (typeof value === "number") return value === 1; const text = safeLower(value); return ["true", "1", "yes", "y", "on", "active"].includes(text); }
function parseBody(event) { try { return event.body ? JSON.parse(event.body) : {}; } catch (e) { return {}; } }
function toTitle(value, fallback) { if (!fallback) fallback = "Base"; const text = safeLower(value); if (!text) return fallback; return text.charAt(0).toUpperCase() + text.slice(1); }
function firstFilled() { for (var i = 0; i < arguments.length; i++) { var text = safeText(arguments[i]); if (text) return text; } return ""; }
function normalizeIdentifier(value) { return safeLower(value).replace(/\s+/g, ""); }
function clamp(value, min, max) { return Math.min(max, Math.max(min, value)); }
function isMissingTableError(error) { const message = safeLower(error && error.message ? error.message : ""); return message.includes("does not exist") || message.includes("could not find the table") || message.includes("schema cache") || message.includes("relation") || (message.includes("column") && message.includes("does not exist")); }
function looksLikeUuid(value) { return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(safeText(value)); }
function requireId(value, label) { const clean = safeText(value); const lowered = safeLower(clean); if (!clean || lowered === "undefined" || lowered === "null") { throw new Error("Missing valid " + label + "."); } return clean; }
function getMemberId(row) { return safeText(row && row.member_id ? row.member_id : (row && row.id ? row.id : "")); }

function normalizePartnershipRow(row) {
  if (!row) return null;
  return Object.assign({}, row, { id: requireId(row.id, "partnership_uuid") });
}

function getPartnerRole(partnershipRow, slAvatarKey) {
  const requester = safeLower(partnershipRow && partnershipRow.requester_avatar_key ? partnershipRow.requester_avatar_key : "");
  const recipient = safeLower(partnershipRow && partnershipRow.recipient_avatar_key ? partnershipRow.recipient_avatar_key : "");
  const current = safeLower(slAvatarKey);
  if (!current) return "";
  if (current === requester) return "partner_a";
  if (current === recipient) return "partner_b";
  return "";
}

function getSectionBookTitles(volumeNumber, realmName) {
  const safeVolume = safeNumber(volumeNumber, 0);
  const cleanRealm = safeText(realmName);
  const locked = VOLUME_SECTION_BOOK_TITLES[safeVolume];
  if (locked) return locked;
  return {
    base: (cleanRealm || "Realm") + " Base Scripture",
    early: (cleanRealm || "Realm") + " Early Scripture",
    middle: (cleanRealm || "Realm") + " Middle Scripture",
    late: (cleanRealm || "Realm") + " Late Scripture"
  };
}

function getSectionBookTitle(volumeNumber, realmName, sectionKey) {
  const titles = getSectionBookTitles(volumeNumber, realmName);
  return safeText(titles[safeLower(sectionKey)]);
}

function formatSectionSummary(row) {
  const statuses = {
    base: row.base_status || "sealed",
    early: row.early_status || "sealed",
    middle: row.middle_status || "sealed",
    late: row.late_status || "sealed"
  };
  const completedCount = Object.values(statuses).filter(function(s) { return s === "comprehended"; }).length;
  return { statuses: statuses, completed_count: completedCount, total_sections: 4 };
}

function decorateEligibility(items) {
  const itemMap = new Map();
  items.forEach(function(item) { itemMap.set(safeNumber(item.volume_number, 0), item); });
  return items.map(function(item) {
    const volumeNumber = safeNumber(item.volume_number, 0);
    const volumeStatus = safeLower(item.volume_status);
    const previousVolumeNumber = volumeNumber - 1;
    let eligibleForComprehension = false;
    let eligibilityReason = "";
    let previousVolumeCompleted = false;
    if (volumeNumber <= 1) {
      eligibleForComprehension = true;
      eligibilityReason = "Volume 1 is the starting realm volume.";
    } else {
      const previousVolume = itemMap.get(previousVolumeNumber) || null;
      previousVolumeCompleted = !!previousVolume && safeLower(previousVolume.volume_status) === "completed_volume";
      if (previousVolumeCompleted) { eligibleForComprehension = true; eligibilityReason = "Volume " + previousVolumeNumber + " has been completed."; }
      else { eligibleForComprehension = false; eligibilityReason = "Complete Volume " + previousVolumeNumber + " before this volume can be studied."; }
    }
    if (volumeStatus === "completed_volume") { eligibleForComprehension = true; eligibilityReason = "This volume has already been completed."; }
    if (volumeStatus === "under_comprehension") { eligibleForComprehension = true; eligibilityReason = "This volume is already under comprehension."; }
    return Object.assign({}, item, {
      eligible_for_comprehension: eligibleForComprehension,
      display_access_status: eligibleForComprehension ? "Eligible for Comprehension" : "Sealed Until Eligible",
      eligibility_reason: eligibilityReason,
      previous_volume_number: previousVolumeNumber > 0 ? previousVolumeNumber : null,
      previous_volume_completed: previousVolumeCompleted
    });
  });
}

function getRequiredSeconds(volumeNumber, sectionKey) {
  const baseHoursBySection = { base: 1, early: 2, middle: 3, late: 4 };
  const safeVolume = Math.max(1, safeNumber(volumeNumber, 1));
  const baseHours = baseHoursBySection[sectionKey] || 0;
  const extraHours = (safeVolume - 1) * 0.5;
  return Math.round((baseHours + extraHours) * 3600);
}

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(safeNumber(totalSeconds, 0)));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours > 0 && minutes > 0) return hours + "h " + minutes + "m";
  if (hours > 0) return hours + "h";
  if (minutes > 0) return minutes + "m";
  return "0m";
}

function detectMeditationState(member) {
  const v2Status = safeLower((member && member.v2_cultivation_status) || "");
  const rawState = v2Status || safeLower((member && member.current_activity) || "");
  let isActive = v2Status === "cultivating" || safeBoolean(member && member.is_meditating) || ["active", "meditating", "in_progress", "ongoing", "cultivating"].includes(rawState);
  const startedAt = (member && member.active_meditation_started_at) || null;
  return { is_active: isActive, raw_state: rawState || null, started_at: startedAt || null };
}

function buildProgressMap(progressRows) {
  const map = new Map();
  (progressRows || []).forEach(function(row) {
    const volumeNumber = safeNumber(row.volume_number, 0);
    const sectionKey = safeLower(row.section_key);
    if (!volumeNumber || !sectionKey) return;
    map.set(volumeNumber + ":" + sectionKey, row);
  });
  return map;
}

function getProgressRow(progressMap, volumeNumber, sectionKey) {
  return progressMap.get(safeNumber(volumeNumber, 0) + ":" + safeLower(sectionKey)) || null;
}

function buildSectionTimer(opts) {
  const realmName = opts.realmName;
  const volumeNumber = opts.volumeNumber;
  const sectionKey = opts.sectionKey;
  const sectionStatus = opts.sectionStatus;
  const progressRow = opts.progressRow;
  const meditationState = opts.meditationState;
  const nowMs = opts.nowMs;
  const status = safeLower(sectionStatus || "sealed");
  const requiredSeconds = safeNumber(progressRow && progressRow.required_seconds, 0) || safeNumber(progressRow && progressRow.section_time_required, 0) || getRequiredSeconds(volumeNumber, sectionKey);
  const storedAccumulatedSeconds = safeNumber(progressRow && progressRow.accumulated_seconds, 0) || safeNumber(progressRow && progressRow.progress_seconds, 0) || safeNumber(progressRow && progressRow.section_time_progress, 0);
  const activeSessionStartedAt = (progressRow && progressRow.active_session_started_at) || (progressRow && progressRow.timing_started_at) || null;
  let liveSessionSeconds = 0;
  if (status === "under_comprehension" && meditationState.is_active && activeSessionStartedAt) {
    const startMs = new Date(activeSessionStartedAt).getTime();
    if (Number.isFinite(startMs) && startMs > 0 && nowMs > startMs) liveSessionSeconds = Math.floor((nowMs - startMs) / 1000);
  }
  const accumulatedSeconds = Math.min(requiredSeconds, storedAccumulatedSeconds + liveSessionSeconds);
  const remainingSeconds = Math.max(0, requiredSeconds - accumulatedSeconds);
  const progressPercent = requiredSeconds > 0 ? Math.min(100, Number(((accumulatedSeconds / requiredSeconds) * 100).toFixed(1))) : 0;
  let timerState = "locked";
  let displayState = "Locked";
  if (status === "sealed") { timerState = "locked"; displayState = "Locked"; }
  else if (status === "opened") { timerState = "ready_to_start"; displayState = "Ready to Start"; }
  else if (status === "comprehended") { timerState = "completed"; displayState = "Completed"; }
  else if (status === "under_comprehension") {
    if (remainingSeconds <= 0) { timerState = "ready_to_complete"; displayState = "Ready to Complete"; }
    else if (meditationState.is_active && activeSessionStartedAt) { timerState = "cultivating"; displayState = "Cultivating"; }
    else { timerState = "paused"; displayState = "Paused"; }
  }
  return {
    section_key: sectionKey, section_label: toTitle(sectionKey, "Base"), book_title: getSectionBookTitle(volumeNumber, realmName, sectionKey),
    status: status, required_seconds: requiredSeconds, accumulated_seconds: accumulatedSeconds, stored_accumulated_seconds: storedAccumulatedSeconds,
    live_session_seconds: liveSessionSeconds, remaining_seconds: remainingSeconds, progress_percent: progressPercent,
    timer_state: timerState, display_timer_state: displayState, human_required: formatDuration(requiredSeconds),
    human_accumulated: formatDuration(accumulatedSeconds), human_remaining: formatDuration(remainingSeconds),
    can_complete: status === "under_comprehension" && remainingSeconds <= 0,
    is_currently_cultivating: status === "under_comprehension" && meditationState.is_active && !!activeSessionStartedAt && remainingSeconds > 0,
    active_session_started_at: activeSessionStartedAt,
    comprehension_started_at: (progressRow && progressRow.comprehension_started_at) || null,
    completed_at: (progressRow && progressRow.completed_at) || null,
    updated_at: (progressRow && progressRow.updated_at) || null
  };
}

async function loadPartnershipByUuid(partnershipUuid) {
  const resolvedUuid = requireId(partnershipUuid, "partnership_uuid");
  const { data, error } = await supabase.schema("partner").from(PARTNERSHIP_TABLE).select("id,partnership_id,requester_avatar_key,requester_username,recipient_avatar_key,recipient_username,status,created_at,accepted_at,updated_at").eq("id", resolvedUuid).limit(1).maybeSingle();
  if (error) throw new Error("Failed to load partnership by UUID: " + error.message);
  return normalizePartnershipRow(data);
}

async function loadPartnershipByLegacyId(legacyPartnershipId) {
  const resolvedLegacyId = requireId(legacyPartnershipId, "legacy partnership_id");
  const { data, error } = await supabase.schema("partner").from(PARTNERSHIP_TABLE).select("id,partnership_id,requester_avatar_key,requester_username,recipient_avatar_key,recipient_username,status,created_at,accepted_at,updated_at").eq("partnership_id", resolvedLegacyId).limit(1).maybeSingle();
  if (error) throw new Error("Failed to load partnership by legacy ID: " + error.message);
  return normalizePartnershipRow(data);
}

async function loadSelectedPartnershipReference(memberId) {
  if (!safeText(memberId)) return "";
  const { data, error } = await supabase.schema("partner").from(MEMBER_SELECTED_PARTNERSHIPS_TABLE).select("*").eq("member_id", memberId).limit(2);
  if (error) throw new Error("Failed to load selected partnership: " + error.message);
  const rows = Array.isArray(data) ? data : [];
  if (rows.length > 1) throw new Error("Multiple selected partnership rows found for member " + memberId + ".");
  const row = rows[0] || null;
  if (!row) return "";
  return safeText(row.partnership_id) || safeText(row.partnership_uuid) || safeText(row.selected_partnership_id) || safeText(row.selected_partnership_uuid) || "";
}

async function loadActivePartnershipRows(opts) {
  const slAvatarKey = opts.slAvatarKey;
  const slUsername = opts.slUsername;
  const filters = [];
  if (safeText(slAvatarKey)) { filters.push("requester_avatar_key.eq." + slAvatarKey); filters.push("recipient_avatar_key.eq." + slAvatarKey); }
  if (safeText(slUsername)) { filters.push("requester_username.eq." + slUsername); filters.push("recipient_username.eq." + slUsername); }
  if (!filters.length) return [];
  const { data, error } = await supabase.schema("partner").from(PARTNERSHIP_TABLE).select("id,partnership_id,requester_avatar_key,requester_username,recipient_avatar_key,recipient_username,status,created_at,accepted_at,updated_at").or(filters.join(",")).eq("status", "active").order("created_at", { ascending: true });
  if (error) throw new Error("Failed to load active partnerships: " + error.message);
  return (data || []).map(normalizePartnershipRow);
}

function buildResolvedLibraryPartnership(opts) {
  const slAvatarKey = safeText(opts.member && opts.member.sl_avatar_key ? opts.member.sl_avatar_key : "");
  const buyerRole = opts.partnership ? getPartnerRole(opts.partnership, slAvatarKey) : "";
  return {
    has_active_partnership: Boolean(opts.hasAnyActivePartnerships),
    has_multiple_active_partnerships: Boolean(opts.multipleActiveFound),
    selected_partnership_required: Boolean(opts.selectedPartnershipRequired),
    selected_partnership_found: Boolean(opts.selectedPartnershipFound),
    selected_partnership_invalid: Boolean(opts.selectedPartnershipInvalid),
    selected_partnership_missing: Boolean(opts.selectedPartnershipMissing),
    selected_partnership_inactive: Boolean(opts.selectedPartnershipInactive),
    explicit_resolution_failed: Boolean(opts.explicitResolutionFailed),
    explicit_resolution_reason: opts.explicitResolutionReason || null,
    partnership: opts.partnership || null,
    partnership_uuid: (opts.partnership && opts.partnership.id) ? opts.partnership.id : null,
    legacy_partnership_id: (opts.partnership && opts.partnership.partnership_id) ? opts.partnership.partnership_id : null,
    buyer_role: buyerRole || null,
    partnership_source: opts.source || null
  };
}

async function resolveLibraryPartnership(opts) {
  const member = opts.member;
  const requestedPartnershipUuid = opts.requestedPartnershipUuid;
  const requestedLegacyPartnershipId = opts.requestedLegacyPartnershipId;
  const slAvatarKey = safeText(member && member.sl_avatar_key ? member.sl_avatar_key : "");
  const slUsername = safeLower(member && member.sl_username ? member.sl_username : "");
  const memberId = getMemberId(member) || null;
  const activeRows = await loadActivePartnershipRows({ slAvatarKey: slAvatarKey, slUsername: slUsername });
  const hasAnyActivePartnerships = activeRows.length > 0;
  const multipleActiveFound = activeRows.length > 1;
  let selectedPartnershipFound = false;
  let selectedPartnershipInvalid = false;
  let selectedPartnershipMissing = false;
  let selectedPartnershipInactive = false;
  let explicitResolutionFailed = false;
  let explicitResolutionReason = null;

  if (safeText(requestedPartnershipUuid)) {
    if (!looksLikeUuid(requestedPartnershipUuid)) return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "invalid_partnership_uuid" });
    const row = await loadPartnershipByUuid(requestedPartnershipUuid);
    if (!row) return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "partnership_uuid_not_found" });
    if (safeLower(row.status) !== "active") return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "partnership_not_active" });
    if (!getPartnerRole(row, slAvatarKey)) return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "not_member_of_requested_partnership" });
    return buildResolvedLibraryPartnership({ member: member, partnership: row, source: "explicit_partnership_uuid", hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: false, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: explicitResolutionFailed, explicitResolutionReason: explicitResolutionReason });
  }

  if (safeText(requestedLegacyPartnershipId)) {
    const row = await loadPartnershipByLegacyId(requestedLegacyPartnershipId);
    if (!row) return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "legacy_partnership_id_not_found" });
    if (safeLower(row.status) !== "active") return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "partnership_not_active" });
    if (!getPartnerRole(row, slAvatarKey)) return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: true, explicitResolutionReason: "not_member_of_requested_partnership" });
    return buildResolvedLibraryPartnership({ member: member, partnership: row, source: "explicit_legacy_partnership_id", hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: false, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: explicitResolutionFailed, explicitResolutionReason: explicitResolutionReason });
  }

  const selectedReference = await loadSelectedPartnershipReference(memberId);
  if (selectedReference) {
    selectedPartnershipFound = true;
    let selectedRow = null;
    if (looksLikeUuid(selectedReference)) { selectedRow = await loadPartnershipByUuid(selectedReference); }
    else { selectedRow = await loadPartnershipByLegacyId(selectedReference); }
    if (!selectedRow) { selectedPartnershipMissing = true; }
    else if (safeLower(selectedRow.status) !== "active") { selectedPartnershipInactive = true; }
    else if (!getPartnerRole(selectedRow, slAvatarKey)) { selectedPartnershipInvalid = true; }
    else { return buildResolvedLibraryPartnership({ member: member, partnership: selectedRow, source: "selected_partnership", hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: false, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: explicitResolutionFailed, explicitResolutionReason: explicitResolutionReason }); }
  }

  if (activeRows.length === 1) {
    return buildResolvedLibraryPartnership({ member: member, partnership: activeRows[0], source: "single_active_fallback", hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: false, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: explicitResolutionFailed, explicitResolutionReason: explicitResolutionReason });
  }

  return buildResolvedLibraryPartnership({ member: member, partnership: null, source: null, hasAnyActivePartnerships: hasAnyActivePartnerships, multipleActiveFound: multipleActiveFound, selectedPartnershipRequired: multipleActiveFound, selectedPartnershipFound: selectedPartnershipFound, selectedPartnershipInvalid: selectedPartnershipInvalid, selectedPartnershipMissing: selectedPartnershipMissing, selectedPartnershipInactive: selectedPartnershipInactive, explicitResolutionFailed: explicitResolutionFailed, explicitResolutionReason: explicitResolutionReason });
}

function extractPartnerContextFromResolvedPartnership(member, resolvedPartnership) {
  const partnership = resolvedPartnership && resolvedPartnership.partnership ? resolvedPartnership.partnership : null;
  if (!partnership) {
    return { has_partner: false, partner_sl_avatar_key: null, partner_sl_username: null, partnership_key: null, partnership_uuid: null, legacy_partnership_id: null, buyer_role: null, partnership_source: resolvedPartnership && resolvedPartnership.partnership_source ? resolvedPartnership.partnership_source : null };
  }
  const selfAvatar = safeText(member && member.sl_avatar_key ? member.sl_avatar_key : "");
  const selfUsername = safeLower(member && member.sl_username ? member.sl_username : "");
  const requesterAvatar = safeText(partnership.requester_avatar_key);
  const requesterUsername = safeLower(partnership.requester_username);
  const recipientAvatar = safeText(partnership.recipient_avatar_key);
  const recipientUsername = safeLower(partnership.recipient_username);
  const selfIsRequester = (selfAvatar && normalizeIdentifier(selfAvatar) === normalizeIdentifier(requesterAvatar)) || (selfUsername && normalizeIdentifier(selfUsername) === normalizeIdentifier(requesterUsername));
  const partnerAvatarKey = selfIsRequester ? recipientAvatar : requesterAvatar;
  const partnerUsername = selfIsRequester ? recipientUsername : requesterUsername;
  return {
    has_partner: true,
    partner_sl_avatar_key: partnerAvatarKey || null,
    partner_sl_username: partnerUsername || null,
    partnership_key: firstFilled(partnership.id, partnership.partnership_id) || null,
    partnership_uuid: safeText(partnership.id) || null,
    legacy_partnership_id: partnership.partnership_id || null,
    buyer_role: selfIsRequester ? "partner_a" : "partner_b",
    partnership_source: resolvedPartnership.partnership_source || null
  };
}

async function loadPartnerMemberRow(opts) {
  const partnerAvatarKey = opts.partnerAvatarKey;
  const partnerUsername = opts.partnerUsername;
  if (!partnerAvatarKey && !partnerUsername) return null;
  let query = supabase.from("cultivation_members").select("*").limit(1);
  if (partnerAvatarKey) { query = query.eq("sl_avatar_key", partnerAvatarKey); }
  else { query = query.eq("sl_username", partnerUsername); }
  const { data, error } = await query;
  if (error) { console.error("library loader partner lookup error:", error); return null; }
  return Array.isArray(data) && data.length > 0 ? data[0] : null;
}

function safeArray(value) { return Array.isArray(value) ? value : []; }

function pickFirstValue(obj, fields) {
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i];
    if (obj && obj[field] !== undefined && obj[field] !== null && obj[field] !== "") return obj[field];
  }
  return null;
}

function getBondStageName(percent) {
  const p = Math.max(0, Math.min(100, safeNumber(percent, 0)));
  if (p >= 100) return "Eternal Bond";
  if (p >= 80) return "Bond Spirit";
  if (p >= 60) return "Bond Soul";
  if (p >= 40) return "Bond Core";
  if (p >= 20) return "Bond Root";
  return "Bond Seed";
}

function getBondProgressPercentFromMemberRow(row) {
  const requiredMinutes = Math.max(0, safeNumber(pickFirstValue(row, ["required_minutes", "required_shared_minutes"]), 0));
  const requiredQi = Math.max(0, safeNumber(pickFirstValue(row, ["required_qi", "required_shared_qi"]), 0));
  const minutesAccumulated = Math.max(0, safeNumber(pickFirstValue(row, ["minutes_accumulated", "shared_minutes_accumulated"]), 0));
  const qiAccumulated = Math.max(0, safeNumber(pickFirstValue(row, ["qi_accumulated", "shared_qi_accumulated"]), 0));
  const rowStatus = safeLower(pickFirstValue(row, ["status", "state"]) || "");
  if (rowStatus === "completed") return 100;
  const minuteRatio = requiredMinutes > 0 ? minutesAccumulated / requiredMinutes : 0;
  const qiRatio = requiredQi > 0 ? qiAccumulated / requiredQi : null;
  const ratio = qiRatio === null ? minuteRatio : Math.min(minuteRatio, qiRatio);
  return clamp(Number((ratio * 100).toFixed(2)), 0, 100);
}

function getPairBookDisplayState(selfRow, partnerRow) {
  const selfStatus = safeLower(pickFirstValue(selfRow, ["status", "state"]) || "");
  const partnerStatus = safeLower(pickFirstValue(partnerRow, ["status", "state"]) || "");
  const selfOffering = safeBoolean(selfRow && selfRow.offering_complete);
  const partnerOffering = safeBoolean(partnerRow && partnerRow.offering_complete);
  const selfCompleted = selfStatus === "completed";
  const partnerCompleted = partnerStatus === "completed";
  if (selfCompleted && partnerCompleted) return "completed";
  if (selfStatus === "active" || partnerStatus === "active") return "active";
  if (selfStatus === "paused" || partnerStatus === "paused") return "paused";
  if (selfCompleted || partnerCompleted) return "awaiting_partner_completion";
  if (selfOffering && partnerOffering) return "ready_to_start";
  if (selfOffering || partnerOffering) return "waiting_for_partner_offering";
  if (selfStatus === "available" || partnerStatus === "available") return "available";
  return "locked";
}

function getBondBookPriority(status) {
  const clean = safeLower(status);
  if (clean === "awaiting_partner_completion") return 1;
  if (clean === "active") return 2;
  if (clean === "paused") return 3;
  if (clean === "ready_to_start") return 4;
  if (clean === "waiting_for_partner_offering") return 5;
  if (clean === "available") return 6;
  if (clean === "completed") return 7;
  return 99;
}

function getBondBookTitle(volumeNumber, bookNumber, volumeTitle) {
  const label = BOND_BOOK_LABELS[bookNumber] || ("Book " + (bookNumber || "?"));
  return (volumeTitle || BOND_VOLUME_DEFAULT_TITLES[volumeNumber] || "Bond Volume") + " \u2014 " + label;
}

async function loadPartnerBondCore(partnershipUuid) {
  if (!safeText(partnershipUuid)) return null;
  const { data, error } = await partnerSupabase.from("partner_bonds").select("*").eq("partnership_id", partnershipUuid).maybeSingle();
  if (error) throw new Error("Failed to load partner bond core: " + error.message);
  return data || null;
}

async function loadPartnerBondMemberBookStates(partnershipUuid, memberIds) {
  if (!memberIds) memberIds = [];
  if (!safeText(partnershipUuid)) return [];
  let query = partnerSupabase.from("partner_bond_member_book_states").select("*").eq("partnership_uuid", partnershipUuid).order("bond_volume_number", { ascending: true }).order("bond_book_number", { ascending: true });
  const cleanMemberIds = safeArray(memberIds).map(function(id) { return safeText(id); }).filter(Boolean);
  if (cleanMemberIds.length) query = query.in("member_id", cleanMemberIds);
  const { data, error } = await query;
  if (error) throw new Error("Failed to load partner bond member book states: " + error.message);
  return data || [];
}

function buildUnresolvedBondLibrary(resolvedPartnership, partnerContext, partnerMember) {
  if (!resolvedPartnership.has_active_partnership) {
    return { available: false, has_partner: false, message: "Bond Library remains dormant until a partnership is formed.", partner: null, summary: { purchased_volumes: 0, total_books: 0, completed_books: 0, active_books: 0, unlocked_books: 0, bond_percent: 0, bond_stage: "Bond Seed", active_volume_number: null, active_book_number: null }, volumes: [], active_session: null };
  }
  let message = "Bond Library could not be resolved for the current partnership focus.";
  if (resolvedPartnership.selected_partnership_required) message = "Multiple active partnerships were found. Select a partnership first before opening the Bond Library.";
  else if (resolvedPartnership.selected_partnership_missing) message = "The saved selected partnership no longer exists. Re-select a partnership before opening the Bond Library.";
  else if (resolvedPartnership.selected_partnership_inactive) message = "The saved selected partnership is no longer active. Re-select an active partnership before opening the Bond Library.";
  else if (resolvedPartnership.selected_partnership_invalid) message = "The saved selected partnership is invalid for this member. Re-select a valid partnership before opening the Bond Library.";
  else if (resolvedPartnership.explicit_resolution_failed) message = "The requested partnership could not be resolved for the Bond Library.";
  return {
    available: false,
    has_partner: Boolean(resolvedPartnership.has_active_partnership),
    message: message,
    partner: (partnerContext && partnerContext.has_partner) ? { sl_avatar_key: partnerContext.partner_sl_avatar_key || null, sl_username: partnerContext.partner_sl_username || null, display_name: (partnerMember && partnerMember.display_name) ? partnerMember.display_name : null } : null,
    summary: { purchased_volumes: 0, total_books: 0, completed_books: 0, active_books: 0, unlocked_books: 0, bond_percent: 0, bond_stage: "Bond Seed", active_volume_number: null, active_book_number: null },
    volumes: [],
    active_session: null
  };
}

function volumeNumberFromBook(book, builtVolumes) {
  for (let vi = 0; vi < safeArray(builtVolumes).length; vi++) {
    const volume = builtVolumes[vi];
    for (let bi = 0; bi < safeArray(volume.books).length; bi++) {
      const entry = volume.books[bi];
      if (safeNumber(entry.book_number, 0) === safeNumber(book.book_number, 0) && safeText(entry.book_name) === safeText(book.book_name)) return safeNumber(volume.volume_number, 0);
    }
  }
  return 0;
}

function buildBondLibraryFromMemberStates(opts) {
  const resolvedPartnership = opts.resolvedPartnership;
  const partnerContext = opts.partnerContext;
  const partnerMember = opts.partnerMember;
  const bondCatalogRows = opts.bondCatalogRows;
  const selfMemberId = opts.selfMemberId;
  const partnerMemberId = opts.partnerMemberId;
  const memberBookRows = opts.memberBookRows;
  const bondCoreRow = opts.bondCoreRow;

  if (!partnerContext || !partnerContext.has_partner || !resolvedPartnership.partnership_uuid || !selfMemberId || !partnerMemberId) {
    return buildUnresolvedBondLibrary(resolvedPartnership, partnerContext, partnerMember);
  }

  const selfRowMap = new Map();
  const partnerRowMap = new Map();
  const volumeNumbers = new Set();
  safeArray(memberBookRows).forEach(function(row) {
    const volumeNumber = safeNumber(row && row.bond_volume_number, 0);
    const bookNumber = safeNumber(row && row.bond_book_number, 0);
    const memberId = safeText(row && row.member_id ? row.member_id : "");
    const key = volumeNumber + ":" + bookNumber;
    if (!volumeNumber || !bookNumber) return;
    volumeNumbers.add(volumeNumber);
    if (memberId === selfMemberId) selfRowMap.set(key, row);
    else if (memberId === partnerMemberId) partnerRowMap.set(key, row);
  });

  const catalogByVolume = new Map();
  safeArray(bondCatalogRows).forEach(function(row) {
    const volumeNumber = safeNumber(row && row.volume_number, 0);
    if (volumeNumber > 0) catalogByVolume.set(volumeNumber, row);
  });

  const candidateVolumes = Array.from(volumeNumbers).sort(function(a, b) { return a - b; });
  const builtVolumes = [];

  candidateVolumes.forEach(function(volumeNumber) {
    const catalogRow = catalogByVolume.get(volumeNumber) || null;
    const volumeTitle = firstFilled(catalogRow && catalogRow.item_name ? catalogRow.item_name : "", BOND_VOLUME_DEFAULT_TITLES[volumeNumber] || "") || ("Bond Volume " + volumeNumber);
    const description = firstFilled(catalogRow && catalogRow.description ? catalogRow.description : "") || null;

    const books = Object.keys(BOND_BOOK_LABELS).map(function(k) { return safeNumber(k, 0); }).filter(function(n) { return n > 0; }).map(function(bookNumber) {
      const key = volumeNumber + ":" + bookNumber;
      const selfRow = selfRowMap.get(key) || null;
      const partnerRow = partnerRowMap.get(key) || null;
      const status = getPairBookDisplayState(selfRow, partnerRow);
      const selfProgress = getBondProgressPercentFromMemberRow(selfRow);
      const partnerProgress = getBondProgressPercentFromMemberRow(partnerRow);
      const progressPercent = status === "completed" ? 100 : Number(((selfProgress + partnerProgress) / 2).toFixed(2));
      return {
        id: safeText((selfRow && selfRow.id) ? selfRow.id : (partnerRow && partnerRow.id ? partnerRow.id : "")) || null,
        book_number: bookNumber, book_name: getBondBookTitle(volumeNumber, bookNumber, volumeTitle),
        book_label: BOND_BOOK_LABELS[bookNumber] || ("Book " + bookNumber), status: status,
        progress_percent: progressPercent, is_completed: status === "completed",
        is_active: ["active", "paused", "awaiting_partner_completion", "ready_to_start"].includes(status),
        is_unlocked: status !== "locked",
        self_status: safeText(selfRow && selfRow.status ? selfRow.status : "") || null,
        partner_status: safeText(partnerRow && partnerRow.status ? partnerRow.status : "") || null,
        self_offering_complete: safeBoolean(selfRow && selfRow.offering_complete),
        partner_offering_complete: safeBoolean(partnerRow && partnerRow.offering_complete),
        self_minutes_accumulated: safeNumber(selfRow && selfRow.minutes_accumulated, 0),
        partner_minutes_accumulated: safeNumber(partnerRow && partnerRow.minutes_accumulated, 0),
        self_qi_accumulated: safeNumber(selfRow && selfRow.qi_accumulated, 0),
        partner_qi_accumulated: safeNumber(partnerRow && partnerRow.qi_accumulated, 0),
        started_at: pickFirstValue(selfRow, ["started_at", "offering_completed_at"]) || pickFirstValue(partnerRow, ["started_at", "offering_completed_at"]),
        completed_at: pickFirstValue(selfRow, ["completed_at"]) || pickFirstValue(partnerRow, ["completed_at"]),
        updated_at: pickFirstValue(selfRow, ["updated_at", "last_progress_at", "created_at"]) || pickFirstValue(partnerRow, ["updated_at", "last_progress_at", "created_at"])
      };
    });

    const hasAccessibleBook = books.some(function(book) { return book.status !== "locked"; });
    if (!hasAccessibleBook) return;

    const completedBooks = books.filter(function(book) { return book.is_completed; }).length;
    const currentFocusBook = books.slice().sort(function(a, b) {
      const ap = getBondBookPriority(a.status); const bp = getBondBookPriority(b.status);
      if (ap !== bp) return ap - bp;
      return safeNumber(a.book_number, 0) - safeNumber(b.book_number, 0);
    })[0] || null;

    let volumeStatus = "owned";
    if (completedBooks === books.length && books.length > 0) volumeStatus = "completed";
    else if (currentFocusBook) volumeStatus = currentFocusBook.status;

    builtVolumes.push({
      id: safeText(catalogRow && catalogRow.id ? catalogRow.id : "") || null,
      store_item_id: safeText(catalogRow && catalogRow.id ? catalogRow.id : "") || null,
      item_key: safeText(catalogRow && catalogRow.item_key ? catalogRow.item_key : "") || null,
      category: firstFilled(catalogRow && catalogRow.category ? catalogRow.category : "", "bond") || "bond",
      item_type: firstFilled(catalogRow && catalogRow.item_type ? catalogRow.item_type : "", "volume") || "volume",
      is_shared_purchase: safeBoolean(catalogRow && catalogRow.is_shared_purchase),
      volume_number: volumeNumber, item_name: volumeTitle, description: description,
      volume_status: volumeStatus,
      price_currency: firstFilled(catalogRow && catalogRow.price_currency ? catalogRow.price_currency : "") || null,
      price_amount: safeNumber(catalogRow && catalogRow.price_amount, 0),
      books_total: books.length, books_completed: completedBooks,
      active_book_number: currentFocusBook ? currentFocusBook.book_number : null,
      active_book_name: currentFocusBook ? currentFocusBook.book_name : null,
      current_focus_book_number: currentFocusBook ? currentFocusBook.book_number : null,
      current_focus_book_name: currentFocusBook ? currentFocusBook.book_name : null,
      owned_at: null, unlocked_at: null,
      completed_at: completedBooks === books.length ? pickFirstValue(currentFocusBook, ["completed_at"]) : null,
      updated_at: currentFocusBook ? currentFocusBook.updated_at : null,
      books: books
    });
  });

  const allBooks = builtVolumes.reduce(function(acc, v) { return acc.concat(v.books || []); }, []);
  const currentFocusBook = allBooks.slice().sort(function(a, b) {
    const ap = getBondBookPriority(a.status); const bp = getBondBookPriority(b.status);
    if (ap !== bp) return ap - bp;
    return safeNumber(a.book_number, 0) - safeNumber(b.book_number, 0);
  })[0] || null;

  const currentFocusVolume = currentFocusBook ? builtVolumes.find(function(v) { return safeNumber(v.volume_number, 0) === safeNumber(currentFocusBook.book_number ? volumeNumberFromBook(currentFocusBook, builtVolumes) : 0, 0); }) : null;
  const completedBooksFallback = allBooks.filter(function(b) { return b.is_completed; }).length;
  const computedBondPercent = Number(Math.min(100, completedBooksFallback * 5).toFixed(2));
  const bondPercent = safeNumber(pickFirstValue(bondCoreRow, ["bond_percent"]), computedBondPercent);
  const bondStage = firstFilled(pickFirstValue(bondCoreRow, ["current_stage_name", "bond_stage_name", "stage_name"]) || "", getBondStageName(bondPercent)) || "Bond Seed";
  const activeVolumeNumber = safeNumber(pickFirstValue(bondCoreRow, ["current_volume_number"]), 0) || safeNumber(currentFocusVolume && currentFocusVolume.volume_number, 0) || safeNumber(builtVolumes[0] && builtVolumes[0].volume_number, 0) || null;
  const activeBookNumber = safeNumber(pickFirstValue(bondCoreRow, ["current_book_number"]), 0) || safeNumber(currentFocusBook && currentFocusBook.book_number, 0) || null;

  return {
    available: true, has_partner: true,
    message: builtVolumes.length > 0 ? "Bond Library loaded successfully." : "Your Bond Library is ready, but no accessible Bond volumes are open yet.",
    partner: { sl_avatar_key: partnerContext.partner_sl_avatar_key || null, sl_username: partnerContext.partner_sl_username || null, display_name: (partnerMember && partnerMember.display_name) ? partnerMember.display_name : null },
    summary: {
      purchased_volumes: builtVolumes.length, total_books: allBooks.length, completed_books: completedBooksFallback,
      active_books: allBooks.filter(function(b) { return ["active", "paused", "awaiting_partner_completion", "ready_to_start"].includes(safeLower(b.status)); }).length,
      unlocked_books: allBooks.filter(function(b) { return b.status !== "locked"; }).length,
      bond_percent: Number(Math.min(100, bondPercent).toFixed(2)), bond_stage: bondStage,
      active_volume_number: activeVolumeNumber || null, active_book_number: activeBookNumber || null
    },
    active_session: currentFocusBook ? {
      status: safeLower(currentFocusBook.status) || "unknown",
      bond_percent: Number(Math.min(100, bondPercent).toFixed(2)),
      volume_number: safeNumber(currentFocusVolume && currentFocusVolume.volume_number, 0) || activeVolumeNumber || null,
      book_number: safeNumber(currentFocusBook.book_number, 0) || null,
      started_at: currentFocusBook.started_at || null, updated_at: currentFocusBook.updated_at || null
    } : null,
    volumes: builtVolumes
  };
}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return buildResponse(200, { ok: true });
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") return buildResponse(405, { success: false, message: "Method not allowed. Use GET or POST." });

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) return buildResponse(500, { success: false, message: "Missing Supabase environment variables." });

    // Read ap_session cookie for auth
    const cookieHeader = (event.headers && event.headers.cookie) ? event.headers.cookie : ((event.headers && event.headers.Cookie) ? event.headers.Cookie : "");
    const cookies = parseCookies(cookieHeader);
    const sessionCookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = cookies[sessionCookieName] || "";
    let resolvedAvatarKeyFromSession = "";

    if (sessionToken) {
      const sessionResult = await supabase.from("website_sessions").select("sl_avatar_key").eq("session_token", sessionToken).eq("is_active", true).maybeSingle();
      resolvedAvatarKeyFromSession = safeText(sessionResult.data && sessionResult.data.sl_avatar_key ? sessionResult.data.sl_avatar_key : "");
    }

    const body = parseBody(event);
    const query = event.queryStringParameters || {};

    const sl_avatar_key = resolvedAvatarKeyFromSession || safeText((query.sl_avatar_key || body.sl_avatar_key) || "");
    const inputUsername = sl_avatar_key ? "" : safeLower((query.sl_username || body.sl_username) || "");

    const requestedPartnershipUuid = safeText((query.partnership_uuid || body.partnership_uuid || body.selected_partnership_uuid) || "");
    const requestedLegacyPartnershipId = safeText((query.partnership_id || query.legacy_partnership_id || body.partnership_id || body.selected_partnership_id || body.legacy_partnership_id || body.partnership_legacy_id || body.legacyPartnershipId) || "");

    if (!sl_avatar_key && !inputUsername) return buildResponse(400, { success: false, message: "Missing required identity. Provide sl_avatar_key or sl_username." });

    let memberLookup = supabase.from("cultivation_members").select("*").limit(1);
    if (sl_avatar_key) memberLookup = memberLookup.eq("sl_avatar_key", sl_avatar_key);
    else memberLookup = memberLookup.eq("sl_username", inputUsername);

    const memberResult = await memberLookup;
    if (memberResult.error) return buildResponse(500, { success: false, message: "Failed to load cultivation member.", error: memberResult.error.message });

    const member = Array.isArray(memberResult.data) && memberResult.data.length > 0 ? memberResult.data[0] : null;
    if (!member) return buildResponse(404, { success: false, message: "No cultivation member record found for this user." });

    const resolvedAvatarKey = safeText(member.sl_avatar_key);
    const resolvedUsername = safeLower(member.sl_username || inputUsername);
    const meditationState = detectMeditationState(member);
    const nowMs = Date.now();

    const libraryResult = await supabase.from("member_library_view").select("id,sl_avatar_key,sl_username,store_item_id,item_key,realm_name,volume_number,item_name,description,volume_status,insight_current,insight_required,base_status,early_status,middle_status,late_status,current_section,owned_at,completed_at,created_at,updated_at").eq("sl_avatar_key", resolvedAvatarKey).order("volume_number", { ascending: true });
    if (libraryResult.error) return buildResponse(500, { success: false, message: "Failed to load member library.", error: libraryResult.error.message });

    let progressRows = [];
    let timedComprehensionEnabled = false;
    let timingMessage = "Timed section progress table not detected yet.";
    try {
      const progressResult = await supabase.from("cultivation_section_progress").select("*").eq("sl_avatar_key", resolvedAvatarKey);
      if (progressResult.error) { if (!isMissingTableError(progressResult.error)) { console.error("list-my-library progress error:", progressResult.error); timingMessage = "Timed section progress could not be loaded."; } }
      else { progressRows = progressResult.data || []; timedComprehensionEnabled = true; timingMessage = "Timed section progress loaded successfully."; }
    } catch (progressError) { console.error("list-my-library progress server error:", progressError); timingMessage = "Timed section progress could not be loaded."; }

    const progressMap = buildProgressMap(progressRows);
    const libraryRows = libraryResult.data || [];

    const rawItems = libraryRows.map(function(row) {
      const sectionSummary = formatSectionSummary(row);
      const sectionBookTitles = getSectionBookTitles(row.volume_number, row.realm_name);
      const baseStatus = row.base_status || "sealed";
      const earlyStatus = row.early_status || "sealed";
      const middleStatus = row.middle_status || "sealed";
      const lateStatus = row.late_status || "sealed";
      const currentSectionKey = safeLower(row.current_section) || "base";
      const currentSectionBookTitle = sectionBookTitles[currentSectionKey] || sectionBookTitles.base || "";
      const sectionTimers = {
        base: buildSectionTimer({ realmName: row.realm_name, volumeNumber: row.volume_number, sectionKey: "base", sectionStatus: baseStatus, progressRow: getProgressRow(progressMap, row.volume_number, "base"), meditationState: meditationState, nowMs: nowMs }),
        early: buildSectionTimer({ realmName: row.realm_name, volumeNumber: row.volume_number, sectionKey: "early", sectionStatus: earlyStatus, progressRow: getProgressRow(progressMap, row.volume_number, "early"), meditationState: meditationState, nowMs: nowMs }),
        middle: buildSectionTimer({ realmName: row.realm_name, volumeNumber: row.volume_number, sectionKey: "middle", sectionStatus: middleStatus, progressRow: getProgressRow(progressMap, row.volume_number, "middle"), meditationState: meditationState, nowMs: nowMs }),
        late: buildSectionTimer({ realmName: row.realm_name, volumeNumber: row.volume_number, sectionKey: "late", sectionStatus: lateStatus, progressRow: getProgressRow(progressMap, row.volume_number, "late"), meditationState: meditationState, nowMs: nowMs })
      };
      return {
        id: row.id, store_item_id: row.store_item_id, item_key: row.item_key, realm_name: row.realm_name,
        volume_number: row.volume_number, item_name: row.item_name, description: row.description,
        volume_status: row.volume_status, insight_current: safeNumber(row.insight_current, 0),
        insight_required: safeNumber(row.insight_required, 100), current_section: row.current_section,
        current_section_label: toTitle(row.current_section || "base", "Base"),
        current_section_book_title: currentSectionBookTitle,
        sections: { base: baseStatus, early: earlyStatus, middle: middleStatus, late: lateStatus },
        section_book_titles: { base: sectionBookTitles.base, early: sectionBookTitles.early, middle: sectionBookTitles.middle, late: sectionBookTitles.late },
        section_timers: sectionTimers, section_summary: sectionSummary,
        owned_at: row.owned_at, completed_at: row.completed_at, created_at: row.created_at, updated_at: row.updated_at
      };
    });

    const items = decorateEligibility(rawItems);
    const completedVolumes = items.filter(function(i) { return safeLower(i.volume_status) === "completed_volume"; }).length;
    const activeVolumes = items.filter(function(i) { return safeLower(i.volume_status) === "under_comprehension"; }).length;
    const ownedVolumes = items.filter(function(i) { return safeLower(i.volume_status) === "owned"; }).length;
    const eligibleVolumes = items.filter(function(i) { return !!i.eligible_for_comprehension; }).length;
    const lockedUntilEligibleVolumes = items.filter(function(i) { return !i.eligible_for_comprehension; }).length;
    const latestVolume = items.length > 0 ? items[items.length - 1] : null;

    const resolvedPartnership = await resolveLibraryPartnership({ member: member, requestedPartnershipUuid: requestedPartnershipUuid, requestedLegacyPartnershipId: requestedLegacyPartnershipId });
    const partnership = extractPartnerContextFromResolvedPartnership(member, resolvedPartnership);
    const partnerMember = partnership.has_partner ? await loadPartnerMemberRow({ partnerAvatarKey: partnership.partner_sl_avatar_key, partnerUsername: partnership.partner_sl_username }) : null;

    let bondCatalogRows = [];
    let bondBookRows = [];
    let bondCoreRow = null;
    const bondWarnings = [];

    try {
      const catalogResult = await supabase.from("library_store_items").select("id,item_key,category,item_type,is_shared_purchase,volume_number,item_name,description,price_currency,price_amount,stock,is_active,updated_at").eq("category", "bond").order("volume_number", { ascending: true });
      if (catalogResult.error) { console.error("bond catalog load error:", catalogResult.error); bondWarnings.push("Bond store catalog could not be loaded."); }
      else bondCatalogRows = catalogResult.data || [];
    } catch (catalogError) { console.error("bond catalog load server error:", catalogError); bondWarnings.push("Bond store catalog could not be loaded."); }

    if (resolvedPartnership.partnership_uuid) {
      try {
        const selfMemberId = getMemberId(member);
        const partnerMemberId = getMemberId(partnerMember);
        if (selfMemberId && partnerMemberId) {
          bondBookRows = await loadPartnerBondMemberBookStates(resolvedPartnership.partnership_uuid, [selfMemberId, partnerMemberId]);
          bondCoreRow = await loadPartnerBondCore(resolvedPartnership.partnership_uuid);
        } else bondWarnings.push("Bond member identity could not be resolved.");
      } catch (bondDataError) { console.error("partner schema bond load error:", bondDataError); bondWarnings.push("Bond library state could not be loaded from the partner schema."); }
    }

    const bondLibrary = buildBondLibraryFromMemberStates({ resolvedPartnership: resolvedPartnership, partnerContext: partnership, partnerMember: partnerMember, bondCatalogRows: bondCatalogRows, selfMemberId: getMemberId(member), partnerMemberId: getMemberId(partnerMember), memberBookRows: bondBookRows, bondCoreRow: bondCoreRow });

    return buildResponse(200, {
      success: true,
      message: "Library loaded successfully.",
      user: { member_id: getMemberId(member) || null, sl_avatar_key: resolvedAvatarKey, sl_username: resolvedUsername || null, display_name: member.display_name || null },
      meditation: { is_active: meditationState.is_active, raw_state: meditationState.raw_state, started_at: meditationState.started_at },
      timing: { timed_comprehension_enabled: timedComprehensionEnabled, message: timingMessage, scaling_rule: "Base=1h, Early=2h, Middle=3h, Late=4h, plus +30m per volume step." },
      summary: { total_owned_volumes: items.length, owned_volumes: ownedVolumes, active_volumes: activeVolumes, completed_volumes: completedVolumes, eligible_volumes: eligibleVolumes, locked_until_eligible_volumes: lockedUntilEligibleVolumes, latest_volume_number: latestVolume ? latestVolume.volume_number : null, latest_volume_name: latestVolume ? latestVolume.item_name : null },
      items: items,
      partnership: {
        has_partner: partnership.has_partner,
        has_active_partnership: Boolean(resolvedPartnership.has_active_partnership),
        has_multiple_active_partnerships: Boolean(resolvedPartnership.has_multiple_active_partnerships),
        selected_partnership_required: Boolean(resolvedPartnership.selected_partnership_required),
        selected_partnership_found: Boolean(resolvedPartnership.selected_partnership_found),
        selected_partnership_invalid: Boolean(resolvedPartnership.selected_partnership_invalid),
        selected_partnership_missing: Boolean(resolvedPartnership.selected_partnership_missing),
        selected_partnership_inactive: Boolean(resolvedPartnership.selected_partnership_inactive),
        explicit_resolution_failed: Boolean(resolvedPartnership.explicit_resolution_failed),
        explicit_resolution_reason: resolvedPartnership.explicit_resolution_reason || null,
        partnership_key: partnership.partnership_key || null,
        partnership_uuid: partnership.partnership_uuid || null,
        legacy_partnership_id: partnership.legacy_partnership_id || null,
        buyer_role: partnership.buyer_role || null,
        partnership_source: partnership.partnership_source || null,
        partner: partnership.has_partner ? {
          sl_avatar_key: partnership.partner_sl_avatar_key || (partnerMember && partnerMember.sl_avatar_key ? partnerMember.sl_avatar_key : null) || null,
          sl_username: partnership.partner_sl_username || (partnerMember ? safeLower(partnerMember.sl_username) : null) || null,
          display_name: (partnerMember && partnerMember.display_name) ? partnerMember.display_name : null
        } : null
      },
      bond_library: bondLibrary,
      warnings: bondWarnings
    });
  } catch (error) {
    return buildResponse(500, { success: false, message: "Unexpected error while loading library.", error: error.message });
  }
};