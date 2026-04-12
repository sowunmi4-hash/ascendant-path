const { createClient } = require("@supabase/supabase-js");

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const librarySupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "library" } }
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "breakthrough" } }
);

// =========================================================
// CONSTANTS
// =========================================================

const SESSION_COOKIE_NAME = process.env.SESSION_COOKIE_NAME || "ap_session";
const VALID_SECTIONS = ["base", "early", "middle", "late"];

// =========================================================
// UTILITIES
// =========================================================

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

function formatSectionName(sectionName) {
  const text = safeLower(sectionName);
  if (!text) return "Unknown";
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatVolumeStatus(value) {
  const text = safeLower(value);
  if (text === "unclaimed") return "Unclaimed";
  if (text === "owned") return "Owned";
  if (text === "under_comprehension") return "Under Comprehension";
  if (text === "completed_volume") return "Completed Volume";
  return text || "Unknown";
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

function unwrapRpcRow(data) {
  if (Array.isArray(data)) return data[0] || null;
  return data || null;
}

function maybeJson(value) {
  if (value == null) return value;
  if (typeof value === "object") return value;
  const text = String(value).trim();
  if (!text) return null;
  try { return JSON.parse(text); } catch { return value; }
}

function firstRow(value) {
  const parsed = maybeJson(value);
  if (Array.isArray(parsed)) return parsed[0] || null;
  return parsed || null;
}

function extractSnapshotRoot(raw) {
  const row = firstRow(raw);
  if (!row) return null;
  const nested = maybeJson(row.snapshot) || maybeJson(row.data) || maybeJson(row.result) || row;
  return firstRow(nested) || nested || null;
}

function parseCookies(cookieHeader) {
  if (!cookieHeader) return {};
  const cookies = {};
  for (const pair of cookieHeader.split(";")) {
    const [key, ...rest] = pair.trim().split("=");
    if (key) cookies[key.trim()] = decodeURIComponent(rest.join("=").trim());
  }
  return cookies;
}

function getSessionToken(event) {
  const cookieHeader =
    event.headers?.cookie || event.headers?.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  return cookies[SESSION_COOKIE_NAME] || null;
}

async function loadSessionIdentity(event) {
  const token = getSessionToken(event);
  if (!token) return null;
  const { data, error } = await publicSupabase
    .from("sessions")
    .select("sl_avatar_key, sl_username")
    .eq("token", token)
    .eq("is_active", true)
    .maybeSingle();
  if (error || !data) return null;
  return { sl_avatar_key: data.sl_avatar_key || null, sl_username: data.sl_username || null };
}

// =========================================================
// SHARED DB HELPERS
// =========================================================

async function loadMember(slAvatarKey, slUsername) {
  // Fix 2: removed `id` from select — member_id is the correct PK
  // Recommended 5: prefer sl_avatar_key, only fall back to sl_username
  let query = publicSupabase
    .from("cultivation_members")
    .select(`
      member_id, sl_avatar_key, sl_username, display_name, character_name,
      vestiges, realm_key, realm_index, realm_name,
      v2_active_stage_key, realm_display_name,
      v2_breakthrough_gate_open, v2_cultivation_status,
      v2_accumulated_seconds, v2_cultivation_started_at
    `)
    .limit(1);
  if (slAvatarKey) {
    query = query.eq("sl_avatar_key", slAvatarKey);
  } else if (slUsername) {
    query = query.eq("sl_username", slUsername);
  }
  const { data, error } = await query.maybeSingle();
  if (error) throw new Error(`Failed to load cultivation member: ${error.message}`);
  return data || null;
}

async function loadStoreVolume(volumeNumber, category) {
  // Category-aware: filters by volume_number + category + is_active.
  // Deterministic — never silently picks one row from multiple matches.
  const { data, error } = await librarySupabase
    .from("library_store_items")
    .select("id,item_key,category,item_type,realm_name,volume_number,item_name,description,price_currency,price_amount,is_active")
    .eq("volume_number", volumeNumber)
    .eq("category", category)
    .eq("is_active", true);
  if (error) throw new Error(`Failed to load store volume: ${error.message}`);
  const rows = Array.isArray(data) ? data : [];
  if (rows.length === 0) return null;
  if (rows.length > 1) throw new Error(`Ambiguous store volume: ${rows.length} active rows found for volume_number ${volumeNumber}, category "${category}". Contact an admin.`);
  return rows[0];
}

async function loadOwnedLibraryRow(member, storeItemId) {
  if (!storeItemId) return null;
  // Prefer sl_avatar_key, only fall back to sl_username — same rule as loadMember().
  // Applying both together risks a false miss if username is stale or formatted differently.
  let query = librarySupabase
    .from("member_library")
    .select(`
      id, sl_avatar_key, sl_username, store_item_id, volume_status,
      insight_current, insight_required, base_status, early_status,
      middle_status, late_status, current_section,
      owned_at, completed_at, created_at, updated_at
    `)
    .eq("store_item_id", storeItemId)
    .limit(1);
  if (member?.sl_avatar_key) {
    query = query.eq("sl_avatar_key", member.sl_avatar_key);
  } else if (member?.sl_username) {
    query = query.eq("sl_username", member.sl_username);
  }
  const { data, error } = await query.maybeSingle();
  if (error) throw new Error(`Failed to load owned library row: ${error.message}`);
  return data || null;
}

// =========================================================
// =========================================================
// CULTIVATION PATH — V2
// Routes to library.v2_open_stage() DB function.
// All validation, CP deduction, ledger, and state
// creation happens inside the DB function.
// =========================================================
// =========================================================

async function callV2OpenStage(slAvatarKey, volumeNumber, sectionKey) {
  // Fix 1: v2_open_stage lives in the library schema — use librarySupabase
  const { data, error } = await librarySupabase.rpc("v2_open_stage", {
    p_sl_avatar_key: slAvatarKey,
    p_volume_number: volumeNumber,
    p_section_key: sectionKey
  });
  if (error) throw new Error(`v2_open_stage RPC failed: ${error.message}`);
  const result = Array.isArray(data) ? data[0] : data;
  return result || null;
}

async function handleCultivationUnlock({ member, storeVolume, volumeNumber, sectionKey }) {
  // Cultivation V2 requires sl_avatar_key — v2_open_stage uses it as the member identifier.
  // If the member record has no avatar key, fail early with a clear message rather than
  // sending an empty string into the DB function.
  if (!member.sl_avatar_key) {
    return buildResponse(400, {
      success: false,
      error_code: "avatar_key_required",
      message: "Cultivation V2 requires sl_avatar_key. This member record has no avatar key populated."
    });
  }

  let result;
  try {
    result = await callV2OpenStage(member.sl_avatar_key, volumeNumber, sectionKey);
  } catch (rpcError) {
    console.error("unlock-library-section v2_open_stage error:", rpcError);
    return buildResponse(500, { success: false, message: "Failed to open stage.", error: rpcError.message });
  }

  if (!result) return buildResponse(500, { success: false, message: "v2_open_stage returned no result." });

  if (!result.success) {
    const errorCode = safeText(result.error_code);
    const message = safeText(result.message, "Open stage failed.");
    const statusMap = {
      member_not_found: 404,
      invalid_volume: 400,
      invalid_section: 400,
      stage_already_exists: 409,
      prerequisite_not_met: 409,
      cost_not_configured: 500,
      timer_not_configured: 500,
      insufficient_cp: 409
    };
    return buildResponse(statusMap[errorCode] || 500, {
      success: false, error_code: errorCode, message,
      ...(result.current_status && { current_status: result.current_status }),
      ...(result.required !== undefined && { required: result.required }),
      ...(result.current_balance !== undefined && { current_balance: result.current_balance }),
      ...(result.required_stage && { required_stage: result.required_stage })
    });
  }

  return buildResponse(200, {
    success: true,
    // Recommended 6: prefer DB message if present, fall back to JS-generated message
    message: safeText(result.message) || `${formatSectionName(sectionKey)} opened successfully for Volume ${volumeNumber}.`,
    stage_state_id: safeText(result.stage_state_id) || null,
    volume_number: safeNumber(result.volume_number, volumeNumber),
    section_key: safeText(result.section_key, sectionKey),
    stage_status: safeText(result.stage_status, "open"),
    required_seconds: safeNumber(result.required_seconds, 0),
    human_required: formatDuration(safeNumber(result.required_seconds, 0)),
    cp_cost_paid: safeNumber(result.cp_cost_paid, 0),
    cp_balance_before: safeNumber(result.cp_balance_before, 0),
    cp_balance_after: safeNumber(result.cp_balance_after, 0),
    store_volume: storeVolume ? {
      id: storeVolume.id, volume_number: storeVolume.volume_number,
      realm_name: storeVolume.realm_name, item_name: storeVolume.item_name,
      item_key: storeVolume.item_key, price_currency: storeVolume.price_currency,
      price_amount: safeNumber(storeVolume.price_amount, 0)
    } : null,
    v2_active_stage_key: `${volumeNumber}:${sectionKey}`,
    next_action: "begin_cultivation"
  });
}

// =========================================================
// =========================================================
// BOND PATH — V1 UNCHANGED
// Bond volume unlocking uses the original V1 logic exactly.
// Do not alter this path during cultivation V2 cutover.
// Protected system — must not be changed.
// =========================================================
// =========================================================

function normalizeSectionTruthStatus(value) {
  const status = safeLower(value);
  if (status === "comprehended") return "comprehended";
  if (["opened", "under_comprehension", "cultivating", "ready_to_complete"].includes(status)) return "opened";
  return "sealed";
}

function buildLibrarySectionTruth(libraryRow) {
  return {
    base: normalizeSectionTruthStatus(libraryRow?.base_status),
    early: normalizeSectionTruthStatus(libraryRow?.early_status),
    middle: normalizeSectionTruthStatus(libraryRow?.middle_status),
    late: normalizeSectionTruthStatus(libraryRow?.late_status)
  };
}

function normalizeRepairState(repairState, selectedVolumeNumber) {
  const row = repairState || null;
  const appliesToRequestedVolume = safeNumber(row?.volume_number, 0) === safeNumber(selectedVolumeNumber, 0);
  return {
    exists: !!row, applies_to_requested_volume: appliesToRequestedVolume,
    sl_avatar_key: safeText(row?.sl_avatar_key) || null,
    volume_number: safeNumber(row?.volume_number, 0) || null,
    section_key: safeLower(row?.section_key) || null,
    latest_outcome: safeText(row?.latest_outcome) || null,
    setback_levels: safeNumber(row?.setback_levels, 0),
    retained_comprehension_percent: safeNumber(row?.retained_comprehension_percent, 100),
    cp_surcharge_percent: safeNumber(row?.cp_surcharge_percent, 0),
    unlock_cp_cost: safeNumber(row?.unlock_cp_cost, 0),
    repair_cp_cost: safeNumber(row?.repair_cp_cost, 0),
    repair_currency: safeText(row?.repair_currency) || null,
    repair_formula: safeText(row?.repair_formula) || null,
    needs_repair: safeBoolean(row?.needs_repair)
  };
}

function normalizeBreakthroughLiveState({ target, timing, actions, appliesToSelectedVolume, repairState }) {
  if (!target || !appliesToSelectedVolume) return { live_state: "none", display_state: "No Breakthrough", display_message: null, next_action: null, can_battle: false, can_receive_verdict: false, cooldown_active: false, verdict_visible: false };
  if (repairState?.applies_to_requested_volume && repairState?.needs_repair) return { live_state: "resolved", display_state: "Resolved", display_message: "Breakthrough no longer blocks this section. Repair now owns the flow.", next_action: null, can_battle: false, can_receive_verdict: false, cooldown_active: false, verdict_visible: false };
  const stateStatus = safeLower(target?.state_status || timing?.state_status);
  const timingStatus = safeLower(timing?.timing_status);
  const progressionHint = safeLower(target?.progression_state || timing?.progression_state || actions?.progression_state);
  const battleStatus = safeLower(timing?.battle_status || target?.battle_status || "not_started");
  const startedAt = target?.breakthrough_started_at || timing?.breakthrough_started_at || null;
  const endsAt = target?.breakthrough_ends_at || timing?.breakthrough_ends_at || null;
  const verdictVisible = !!target?.verdict_revealed_at || !!timing?.verdict_revealed_at || battleStatus === "revealed";
  const canBattle = safeBoolean(actions?.can_battle ?? timing?.can_battle);
  const canReceiveVerdict = safeBoolean(actions?.can_receive_verdict ?? timing?.can_receive_verdict);
  const countdownComplete = safeBoolean(timing?.countdown_complete) || timingStatus === "completed" || (endsAt ? new Date(endsAt).getTime() <= Date.now() : false);
  if (stateStatus === "cooldown" || progressionHint === "cooldown") return { live_state: "cooldown", display_state: "Cooldown", display_message: safeText(actions?.message) || "The breakthrough path is under cooldown.", next_action: "wait_cooldown", can_battle: false, can_receive_verdict: false, cooldown_active: true, verdict_visible: false };
  if (verdictVisible || stateStatus === "completed" || progressionHint === "verdict_revealed" || progressionHint === "breakthrough_completed") return { live_state: "resolved", display_state: "Resolved", display_message: safeText(actions?.message) || "This breakthrough has already been resolved.", next_action: null, can_battle: false, can_receive_verdict: false, cooldown_active: false, verdict_visible: true };
  if (canReceiveVerdict || progressionHint === "verdict_ready" || progressionHint === "breakthrough_ready" || (battleStatus === "resolved" && countdownComplete)) return { live_state: "ready_for_verdict", display_state: "Receive Heaven's Verdict", display_message: safeText(actions?.message) || "The hidden battle has ended. Heaven's Verdict may now be received.", next_action: "receive_verdict", can_battle: false, can_receive_verdict: true, cooldown_active: false, verdict_visible: false };
  if (timingStatus === "active" || startedAt || endsAt || progressionHint === "breakthrough_active" || progressionHint === "karmic_sealed" || battleStatus === "resolved") return { live_state: "in_progress", display_state: "Breakthrough Active", display_message: safeText(actions?.message) || "Breakthrough is currently in progress.", next_action: "view_breakthrough", can_battle: false, can_receive_verdict: false, cooldown_active: false, verdict_visible: false };
  if (canBattle || timingStatus === "not_started" || progressionHint === "battle_available" || progressionHint === "breakthrough_required" || stateStatus === "active") return { live_state: "not_started", display_state: "Breakthrough Required", display_message: safeText(actions?.message) || "A breakthrough must be entered before progression can continue.", next_action: "enter_breakthrough", can_battle: canBattle, can_receive_verdict: false, cooldown_active: false, verdict_visible: false };
  return { live_state: "resolved", display_state: "Resolved", display_message: null, next_action: null, can_battle: false, can_receive_verdict: false, cooldown_active: false, verdict_visible: false };
}

function normalizeBreakthroughState(snapshotRoot, selectedVolumeNumber, repairState) {
  const root = snapshotRoot || {};
  const target = firstRow(root.target) || null;
  const timing = firstRow(root.timing) || null;
  const actions = firstRow(root.actions) || null;
  const fromVolumeNumber = safeNumber(target?.from_volume_number ?? timing?.from_volume_number, 0) || null;
  const toVolumeNumber = safeNumber(target?.to_volume_number ?? timing?.to_volume_number, 0) || null;
  const selectedVolume = safeNumber(selectedVolumeNumber, 0) || null;
  const appliesAsSourceVolume = !!selectedVolume && fromVolumeNumber === selectedVolume;
  const appliesAsTargetVolume = !!selectedVolume && toVolumeNumber === selectedVolume;
  const appliesToSelectedVolume = appliesAsSourceVolume || appliesAsTargetVolume;
  const startedAt = target?.breakthrough_started_at || timing?.breakthrough_started_at || null;
  const endsAt = target?.breakthrough_ends_at || timing?.breakthrough_ends_at || null;
  let remainingSeconds = safeNumber(timing?.countdown_remaining_seconds ?? timing?.remaining_seconds, 0);
  if ((!remainingSeconds || remainingSeconds < 0) && endsAt) {
    const endMs = new Date(endsAt).getTime();
    if (Number.isFinite(endMs)) remainingSeconds = Math.max(0, Math.ceil((endMs - Date.now()) / 1000));
  }
  const liveMeta = normalizeBreakthroughLiveState({ target, timing, actions, appliesToSelectedVolume, repairState });
  const preferredSectionKey = appliesAsTargetVolume ? safeLower(target?.to_section_key || timing?.to_section_key) : appliesAsSourceVolume ? safeLower(target?.from_section_key || timing?.from_section_key) : null;
  const normalizedPreferredSection = VALID_SECTIONS.includes(preferredSectionKey) ? preferredSectionKey : null;
  const isLive = ["not_started", "in_progress", "ready_for_verdict", "cooldown"].includes(liveMeta.live_state);
  return {
    exists: !!target, applies_to_selected_volume: appliesToSelectedVolume,
    applies_as_source_volume: appliesAsSourceVolume, applies_as_target_volume: appliesAsTargetVolume,
    preferred_section_key: normalizedPreferredSection,
    target_id: safeText(target?.id || timing?.target_id) || null,
    member_id: safeText(target?.member_id || timing?.member_id) || null,
    sl_avatar_key: safeText(target?.sl_avatar_key || timing?.sl_avatar_key) || null,
    sl_username: safeText(target?.sl_username || timing?.sl_username) || null,
    target_type: safeText(target?.target_type || timing?.target_type) || null,
    tribulation_family: safeText(target?.tribulation_family || timing?.tribulation_family) || null,
    from_volume_number: fromVolumeNumber, from_section_key: safeText(target?.from_section_key || timing?.from_section_key) || null,
    to_volume_number: toVolumeNumber, to_section_key: safeText(target?.to_section_key || timing?.to_section_key) || null,
    state_status: safeText(target?.state_status || timing?.state_status) || null,
    timing_status: safeText(timing?.timing_status) || null,
    breakthrough_started_at: startedAt, breakthrough_ends_at: endsAt,
    breakthrough_completed_at: target?.breakthrough_completed_at || timing?.breakthrough_completed_at || null,
    total_duration_minutes: safeNumber(timing?.total_duration_minutes ?? target?.breakthrough_duration_minutes, 0),
    total_duration_seconds: safeNumber(timing?.total_duration_seconds ?? target?.breakthrough_duration_seconds, 0),
    remaining_seconds: remainingSeconds, countdown_remaining_seconds: remainingSeconds,
    countdown_complete: safeBoolean(timing?.countdown_complete) || safeLower(timing?.timing_status) === "completed" || (endsAt ? new Date(endsAt).getTime() <= Date.now() : false),
    live_state: liveMeta.live_state, simple_state: liveMeta.live_state,
    chamber_status: liveMeta.live_state === "none" ? "none" : liveMeta.live_state,
    is_live: isLive, required: isLive, route_to_breakthrough: isLive,
    cooldown_active: liveMeta.cooldown_active, can_battle: liveMeta.can_battle,
    can_receive_verdict: liveMeta.can_receive_verdict, verdict_visible: liveMeta.verdict_visible,
    display_state: liveMeta.display_state, display_message: liveMeta.display_message, next_action: liveMeta.next_action,
    human_total_duration: formatDuration(safeNumber(timing?.total_duration_seconds ?? target?.breakthrough_duration_seconds, 0)),
    human_remaining: formatDuration(remainingSeconds)
  };
}

function buildAccessPayload(eligibility) {
  return {
    eligible_for_comprehension: !!eligibility?.eligible,
    display_access_status: eligibility?.eligible ? "Eligible for Comprehension" : "Sealed Until Eligible",
    eligibility_reason: eligibility?.eligibility_reason || null,
    previous_volume_number: eligibility?.previous_volume_number || null,
    previous_volume_completed: safeBoolean(eligibility?.previous_volume_completed)
  };
}

function getEarlierSections(requestedSection) {
  const targetIndex = VALID_SECTIONS.indexOf(requestedSection);
  if (targetIndex <= 0) return [];
  return VALID_SECTIONS.slice(0, targetIndex);
}

function validateSectionRequest({ sectionTruth, requestedSection, repairState, breakthroughState }) {
  if (!VALID_SECTIONS.includes(requestedSection)) return { ok: false, statusCode: 400, message: "Invalid section_name. Use base, early, middle, or late." };
  const currentStatus = normalizeSectionTruthStatus(sectionTruth?.[requestedSection]);
  if (currentStatus === "opened") return { ok: true, already_opened: true };
  if (currentStatus === "comprehended") return { ok: false, statusCode: 409, message: `${formatSectionName(requestedSection)} has already been comprehended.` };
  if (currentStatus !== "sealed") return { ok: false, statusCode: 409, message: `${formatSectionName(requestedSection)} cannot be opened from its current state.` };
  if (repairState?.applies_to_requested_volume && repairState?.needs_repair && safeLower(repairState?.section_key) === requestedSection) return { ok: false, statusCode: 409, message: `${formatSectionName(requestedSection)} is currently damaged and must be repaired before progression can continue.`, blocking_section: requestedSection };
  if (breakthroughState?.is_live && breakthroughState?.applies_to_selected_volume && safeLower(breakthroughState?.preferred_section_key) === requestedSection) return { ok: false, statusCode: 409, message: breakthroughState?.display_message || `${formatSectionName(requestedSection)} is blocked by an unresolved breakthrough.`, blocking_section: requestedSection };
  for (const earlierSection of getEarlierSections(requestedSection)) {
    if (normalizeSectionTruthStatus(sectionTruth?.[earlierSection]) !== "comprehended") return { ok: false, statusCode: 409, message: `${formatSectionName(earlierSection)} must be comprehended before ${formatSectionName(requestedSection)} can be opened.`, blocking_section: earlierSection };
  }
  return { ok: true, already_opened: false };
}

function buildLibraryUpdates(libraryRow, requestedSection) {
  const updates = { [`${requestedSection}_status`]: "opened", current_section: requestedSection, updated_at: new Date().toISOString() };
  if (safeLower(libraryRow?.volume_status) === "owned") updates.volume_status = "under_comprehension";
  return updates;
}

async function loadCultivationBookRepairState(slAvatarKey) {
  if (!slAvatarKey) return null;
  const { data, error } = await librarySupabase.rpc("get_cultivation_book_repair_state", { p_sl_avatar_key: slAvatarKey });
  if (error) throw new Error(`Failed to load cultivation book repair state: ${error.message}`);
  return unwrapRpcRow(data);
}

async function loadBreakthroughSnapshot(slAvatarKey) {
  if (!slAvatarKey) return null;
  const { data, error } = await breakthroughSupabase.rpc("get_member_breakthrough_snapshot", { p_sl_avatar_key: slAvatarKey });
  if (error) throw new Error(`Failed to load breakthrough snapshot: ${error.message}`);
  return extractSnapshotRoot(data);
}

async function loadSectionUnlockCost(volumeNumber, sectionName) {
  const { data, error } = await librarySupabase.rpc("get_cultivation_section_unlock_cost", { p_volume_number: volumeNumber, p_section_key: sectionName });
  if (error) throw new Error(`Failed to load section unlock cost: ${error.message}`);
  const row = unwrapRpcRow(data);
  if (!row) throw new Error(`Section unlock cost not found for volume ${volumeNumber}, section ${sectionName}.`);
  return safeNumber(row?.unlock_cp_cost, 0);
}

async function checkVolumeEligibility(member, volumeNumber, category) {
  // Category-aware: uses the same category as the requested volume for previous-volume lookup.
  if (volumeNumber <= 1) return { eligible: true, previous_volume_number: null, previous_volume_completed: true, eligibility_reason: "Volume 1 is the starting realm volume." };
  const previousVolumeNumber = volumeNumber - 1;
  const previousStoreVolume = await loadStoreVolume(previousVolumeNumber, category);
  if (!previousStoreVolume) throw new Error(`Previous volume ${previousVolumeNumber} (category: ${category}) was not found in library.library_store_items.`);
  const previousLibraryRow = await loadOwnedLibraryRow(member, previousStoreVolume.id);
  const previousCompleted = !!previousLibraryRow && safeLower(previousLibraryRow.volume_status) === "completed_volume";
  return { eligible: previousCompleted, previous_volume_number: previousVolumeNumber, previous_volume_completed: previousCompleted, eligibility_reason: previousCompleted ? `Volume ${previousVolumeNumber} has been completed.` : `Complete Volume ${previousVolumeNumber} before opening this volume.` };
}

async function handleBondUnlock({ member, storeVolume, libraryRow, volumeNumber, requestedSection }) {
  const volumeStatus = safeLower(libraryRow.volume_status);
  // Fix 2: use member_id only, no .id fallback
  const memberId = member.member_id;
  const currentCultivationPoints = safeNumber(member.vestiges, 0);

  if (volumeStatus === "completed_volume") return buildResponse(409, { success: false, message: `Volume ${volumeNumber} has already been completed.`, volume_status: formatVolumeStatus(libraryRow.volume_status) });
  if (volumeStatus !== "owned" && volumeStatus !== "under_comprehension") return buildResponse(409, { success: false, message: `Volume ${volumeNumber} is not in a usable state for progression.`, volume_status: formatVolumeStatus(libraryRow.volume_status) });

  const [eligibility, rawRepairState, breakthroughSnapshot] = await Promise.all([
    checkVolumeEligibility(member, volumeNumber, "bond"),
    loadCultivationBookRepairState(member.sl_avatar_key),
    loadBreakthroughSnapshot(member.sl_avatar_key)
  ]);

  const access = buildAccessPayload(eligibility);
  if (!access.eligible_for_comprehension) return buildResponse(409, { success: false, message: "This volume is sealed until eligible.", ...access });

  const repairState = normalizeRepairState(rawRepairState, volumeNumber);
  const breakthroughState = normalizeBreakthroughState(breakthroughSnapshot, volumeNumber, repairState);
  const sectionTruth = buildLibrarySectionTruth(libraryRow);
  const sectionValidation = validateSectionRequest({ sectionTruth, requestedSection, repairState, breakthroughState });

  if (!sectionValidation.ok) {
    return buildResponse(sectionValidation.statusCode, {
      success: false, message: sectionValidation.message, requested_section: requestedSection,
      blocking_section: sectionValidation.blocking_section || null,
      library: { id: libraryRow.id, volume_status: safeText(libraryRow.volume_status), current_section: safeText(libraryRow.current_section) || null, sections: sectionTruth },
      repair_state: repairState, breakthrough_state: breakthroughState, access
    });
  }

  if (sectionValidation.already_opened) {
    return buildResponse(200, {
      success: true, already_opened: true, message: `${formatSectionName(requestedSection)} is already opened.`,
      requested_section: requestedSection,
      store_volume: { id: storeVolume.id, volume_number: storeVolume.volume_number, realm_name: storeVolume.realm_name, item_name: storeVolume.item_name, item_key: storeVolume.item_key, price_currency: storeVolume.price_currency, price_amount: safeNumber(storeVolume.price_amount, 0) },
      member: { member_id: memberId, sl_avatar_key: member.sl_avatar_key, sl_username: member.sl_username, vestiges_before: currentCultivationPoints, vestiges_spent: 0, vestiges_after: currentCultivationPoints, v2_active_stage_key: safeText(member.v2_active_stage_key) || null, realm_display_name: safeText(member.realm_display_name || member.realm_name), v2_breakthrough_gate_open: safeBoolean(member.v2_breakthrough_gate_open) },
      library: { id: libraryRow.id, volume_status: safeText(libraryRow.volume_status), insight_current: safeNumber(libraryRow.insight_current, 0), insight_required: safeNumber(libraryRow.insight_required, 0), current_section: requestedSection, sections: sectionTruth },
      pricing: { section_name: requestedSection, section_cost: 0, pricing_source: "idempotent_already_opened" },
      repair_state: repairState, breakthrough_state: breakthroughState, access
    });
  }

  const unlockCost = await loadSectionUnlockCost(volumeNumber, requestedSection);
  if (currentCultivationPoints < unlockCost) {
    return buildResponse(409, {
      success: false, message: `Not enough Cultivation Points to open ${formatSectionName(requestedSection)}.`,
      vestiges_current: currentCultivationPoints, vestiges_required: unlockCost, vestiges_short: unlockCost - currentCultivationPoints,
      pricing: { section_name: requestedSection, section_cost: unlockCost, pricing_source: "library.get_cultivation_section_unlock_cost" },
      repair_state: repairState, breakthrough_state: breakthroughState, access
    });
  }

  const newCultivationPoints = currentCultivationPoints - unlockCost;
  const nowIso = new Date().toISOString();
  const previousMemberState = { vestiges: currentCultivationPoints };

  const { error: memberUpdateError } = await publicSupabase
    .from("cultivation_members")
    .update({ vestiges: newCultivationPoints, updated_at: nowIso })
    .eq("member_id", memberId)
    .eq("sl_avatar_key", member.sl_avatar_key);

  if (memberUpdateError) throw new Error(`Failed to deduct Cultivation Points: ${memberUpdateError.message}`);

  const libraryUpdates = buildLibraryUpdates(libraryRow, requestedSection);
  const { data: updatedLibraryRow, error: libraryUpdateError } = await librarySupabase
    .from("member_library")
    .update(libraryUpdates)
    .eq("id", libraryRow.id)
    .select("id,sl_avatar_key,sl_username,store_item_id,volume_status,insight_current,insight_required,base_status,early_status,middle_status,late_status,current_section,owned_at,completed_at,created_at,updated_at")
    .maybeSingle();

  if (libraryUpdateError || !updatedLibraryRow) {
    const { error: rollbackError } = await publicSupabase
      .from("cultivation_members")
      .update({ vestiges: previousMemberState.vestiges, updated_at: new Date().toISOString() })
      .eq("member_id", memberId)
      .eq("sl_avatar_key", member.sl_avatar_key);
    return buildResponse(500, { success: false, message: "Failed to open section after deducting Cultivation Points.", error: libraryUpdateError?.message || "Unknown library update error.", rollback_attempted: true, rollback_failed: !!rollbackError, rollback_error: rollbackError?.message || null });
  }

  return buildResponse(200, {
    success: true, already_opened: false, message: `${formatSectionName(requestedSection)} opened successfully for Volume ${volumeNumber}.`,
    requested_section: requestedSection,
    store_volume: { id: storeVolume.id, volume_number: storeVolume.volume_number, realm_name: storeVolume.realm_name, item_name: storeVolume.item_name, item_key: storeVolume.item_key, price_currency: storeVolume.price_currency, price_amount: safeNumber(storeVolume.price_amount, 0) },
    member: { member_id: memberId, sl_avatar_key: member.sl_avatar_key, sl_username: member.sl_username, vestiges_before: currentCultivationPoints, vestiges_spent: unlockCost, vestiges_after: newCultivationPoints, v2_active_stage_key: safeText(member.v2_active_stage_key) || null, realm_display_name: safeText(member.realm_display_name || member.realm_name), v2_breakthrough_gate_open: safeBoolean(member.v2_breakthrough_gate_open) },
    library: { id: updatedLibraryRow.id, volume_status: safeText(updatedLibraryRow.volume_status), insight_current: safeNumber(updatedLibraryRow.insight_current, 0), insight_required: safeNumber(updatedLibraryRow.insight_required, 0), current_section: safeText(updatedLibraryRow.current_section) || requestedSection, sections: buildLibrarySectionTruth(updatedLibraryRow) },
    pricing: { section_name: requestedSection, section_cost: unlockCost, pricing_source: "library.get_cultivation_section_unlock_cost" },
    repair_state: repairState, breakthrough_state: breakthroughState, access
  });
}

// =========================================================
// MAIN HANDLER
// Routes to cultivation (V2) or bond (V1) by store category
// =========================================================

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return buildResponse(200, { ok: true });
  if (event.httpMethod !== "POST") return buildResponse(405, { success: false, message: "Method not allowed. Use POST." });

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) return buildResponse(500, { success: false, message: "Missing Supabase environment variables." });

    const body = parseBody(event);
    const sessionIdentity = await loadSessionIdentity(event);
    const slAvatarKey = safeText(body.sl_avatar_key) || safeText(sessionIdentity?.sl_avatar_key);
    const slUsername = safeLower(body.sl_username) || safeLower(sessionIdentity?.sl_username);
    const volumeNumber = parsePositiveInteger(body.volume_number);
    const requestedSection = safeLower(body.section_name || body.section_key);
    // category is now required in the request body — drives deterministic store lookup
    const category = safeLower(body.category);

    if (!slAvatarKey && !slUsername) return buildResponse(400, { success: false, message: "sl_avatar_key or sl_username is required. Provide in body or via session cookie." });
    if (!volumeNumber) return buildResponse(400, { success: false, message: "A valid volume_number is required." });
    if (!VALID_SECTIONS.includes(requestedSection)) return buildResponse(400, { success: false, message: "section_name must be one of: base, early, middle, late." });
    if (category !== "cultivation" && category !== "bond") return buildResponse(400, { success: false, error_code: "invalid_category", message: "category is required and must be cultivation or bond." });

    const member = await loadMember(slAvatarKey, slUsername);
    if (!member) return buildResponse(404, { success: false, message: "Cultivation member not found." });

    // Category-aware store lookup — deterministic, no ambiguity possible
    const storeVolume = await loadStoreVolume(volumeNumber, category);
    if (!storeVolume) return buildResponse(404, { success: false, message: `Volume ${volumeNumber} (category: ${category}) was not found in the store library.` });

    // Explicit routing by validated category — cultivation → V2, bond → V1
    if (category === "cultivation") {
      return await handleCultivationUnlock({ member, storeVolume, volumeNumber, sectionKey: requestedSection });
    }

    // category === "bond" — already validated above, load library row and run V1 path
    const libraryRow = await loadOwnedLibraryRow(member, storeVolume.id);
    if (!libraryRow) return buildResponse(404, { success: false, message: `You do not own Volume ${volumeNumber}.` });
    return await handleBondUnlock({ member, storeVolume, libraryRow, volumeNumber, requestedSection });

  } catch (error) {
    console.error("unlock-library-section error:", error);
    return buildResponse(500, { success: false, message: "Failed to unlock library section.", error: error.message || "Unknown error." });
  }
};