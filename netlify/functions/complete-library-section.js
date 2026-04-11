const { createClient } = require("@supabase/supabase-js");

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const librarySupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "library" }
  }
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "breakthrough" }
  }
);

const VALID_SECTIONS = ["base", "early", "middle", "late"];
const SECTION_ORDER = {
  base: null,
  early: "base",
  middle: "early",
  late: "middle"
};
const NEXT_SECTION_MAP = {
  base: "early",
  early: "middle",
  middle: "late",
  late: null
};
const MAX_REALM_VOLUME = 10;

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

function parseCookies(event) {
  const header = (event.headers || {}).cookie || (event.headers || {}).Cookie || "";
  const map = {};
  for (const pair of header.split(";")) {
    const idx = pair.indexOf("=");
    if (idx < 1) continue;
    const key = pair.slice(0, idx).trim();
    const val = pair.slice(idx + 1).trim();
    map[key] = decodeURIComponent(val);
  }
  return map;
}

function getSessionToken(event) {
  const cookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
  const cookies = parseCookies(event);
  return cookies[cookieName] || null;
}

async function loadMemberFromSession(sessionToken) {
  if (!sessionToken) return null;

  const { data, error } = await publicSupabase
    .from("member_sessions")
    .select("sl_avatar_key, sl_username, member_id, expires_at")
    .eq("session_token", sessionToken)
    .gt("expires_at", new Date().toISOString())
    .maybeSingle();

  if (error || !data) return null;

  return loadMember(data.sl_avatar_key, data.sl_username);
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

function sectionField(sectionName) {
  return `${sectionName}_status`;
}

function formatSectionName(sectionName) {
  const text = safeLower(sectionName);
  if (!text) return "Unknown";
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatRealmName(value) {
  return safeText(value)
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getRealmStageDisplayName(realmName, sectionName) {
  return `${formatSectionName(sectionName)} ${formatRealmName(
    safeText(realmName, "mortal")
  )}`;
}

function formatVolumeStatus(value) {
  const text = safeLower(value);
  if (text === "unclaimed") return "Unclaimed";
  if (text === "owned") return "Owned";
  if (text === "under_comprehension") return "Under Comprehension";
  if (text === "completed_volume") return "Completed Volume";
  return text || "unknown";
}

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(safeNumber(totalSeconds, 0)));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (hours > 0 && minutes > 0) {
    return `${hours}h ${minutes}m`;
  }

  if (hours > 0) {
    return `${hours}h`;
  }

  if (minutes > 0) {
    return `${minutes}m`;
  }

  return "0m";
}

function getRequiredSeconds(volumeNumber, sectionKey) {
  const baseHoursBySection = {
    base: 1,
    early: 2,
    middle: 3,
    late: 4
  };

  const safeVolume = Math.max(1, safeNumber(volumeNumber, 1));
  const baseHours = baseHoursBySection[safeLower(sectionKey)] || 0;
  const extraHours = (safeVolume - 1) * 0.5;

  return Math.round((baseHours + extraHours) * 3600);
}

function getSectionInsightGain(sectionName) {
  const specificKey = `CULTIVATION_SECTION_INSIGHT_${safeText(sectionName).toUpperCase()}`;
  const specificValue = process.env[specificKey];
  const fallbackValue = process.env.CULTIVATION_SECTION_INSIGHT_GAIN;

  const parsed = Number(specificValue || fallbackValue || 25);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 25;
}

function unwrapRpcRow(data) {
  if (Array.isArray(data)) {
    return data[0] || null;
  }
  return data || null;
}

function normalizeRepairState(repairState, volumeNumber) {
  const row = repairState || null;
  const appliesToRequestedVolume =
    safeNumber(row?.volume_number, 0) === safeNumber(volumeNumber, 0);

  return {
    exists: !!row,
    applies_to_requested_volume: appliesToRequestedVolume,
    sl_avatar_key: safeText(row?.sl_avatar_key) || null,
    volume_number: safeNumber(row?.volume_number, 0) || null,
    section_key: safeLower(row?.section_key) || null,
    latest_outcome: safeText(row?.latest_outcome) || null,
    setback_levels: safeNumber(row?.setback_levels, 0),
    retained_comprehension_percent: safeNumber(
      row?.retained_comprehension_percent,
      100
    ),
    cp_surcharge_percent: safeNumber(row?.cp_surcharge_percent, 0),
    unlock_cp_cost: safeNumber(row?.unlock_cp_cost, 0),
    repair_cp_cost: safeNumber(row?.repair_cp_cost, 0),
    repair_currency: safeText(row?.repair_currency) || null,
    repair_formula: safeText(row?.repair_formula) || null,
    needs_repair: safeBoolean(row?.needs_repair)
  };
}

function getRemainingSecondsFromEnd(endsAt) {
  if (!endsAt) return 0;

  const nowMs = Date.now();
  const endMs = new Date(endsAt).getTime();

  if (!Number.isFinite(nowMs) || !Number.isFinite(endMs)) {
    return 0;
  }

  return Math.max(0, Math.floor((endMs - nowMs) / 1000));
}

function buildFallbackTimingStateFromTarget(targetRow) {
  if (!targetRow) return null;

  let timingStatus = "not_started";
  let canResolve = false;
  let remainingSeconds = 0;

  if (targetRow.breakthrough_completed_at) {
    timingStatus = "completed";
  } else if (targetRow.breakthrough_started_at && targetRow.breakthrough_ends_at) {
    remainingSeconds = getRemainingSecondsFromEnd(targetRow.breakthrough_ends_at);

    if (remainingSeconds > 0) {
      timingStatus = "active";
    } else {
      timingStatus = "ready_to_resolve";
      canResolve = true;
    }
  }

  let gateMessage = null;

  if (safeLower(targetRow.state_status) === "cooldown") {
    gateMessage = "The breakthrough path is under cooldown.";
  } else if (timingStatus === "not_started") {
    gateMessage =
      "The next breakthrough must be entered before progression can continue.";
  } else if (timingStatus === "active") {
    gateMessage = "Breakthrough in progress.";
  } else if (timingStatus === "ready_to_resolve") {
    gateMessage = "The breakthrough is ready for verdict.";
  } else if (timingStatus === "completed") {
    gateMessage = "The breakthrough has already been resolved.";
  }

  return {
    ...targetRow,
    timing_status: timingStatus,
    countdown_remaining_seconds: remainingSeconds,
    remaining_seconds: remainingSeconds,
    can_resolve: canResolve,
    gate_message: gateMessage
  };
}

function normalizeBreakthroughState(rawState, selectedVolumeNumber) {
  const row = rawState || null;

  const fromVolumeNumber = safeNumber(row?.from_volume_number, 0) || null;
  const toVolumeNumber = safeNumber(row?.to_volume_number, 0) || null;
  const targetType = safeLower(row?.target_type);
  const timingStatus = safeLower(row?.timing_status);
  const stateStatus = safeLower(row?.state_status);
  const selectedVolume = safeNumber(selectedVolumeNumber, 0) || null;

  const appliesAsSourceVolume =
    !!selectedVolume && fromVolumeNumber === selectedVolume;

  const appliesAsTargetVolume =
    !!selectedVolume && toVolumeNumber === selectedVolume;

  const appliesToSelectedVolume =
    appliesAsSourceVolume || appliesAsTargetVolume;

  const remainingSeconds = safeNumber(
    row?.countdown_remaining_seconds ?? row?.remaining_seconds,
    0
  );

  let progressionState = "none";
  let nextAction = null;
  let displayState = "No Breakthrough";
  let displayMessage = null;

  if (row) {
    if (stateStatus === "cooldown") {
      progressionState = "cooldown";
      nextAction = "wait_cooldown";
      displayState = "Cooldown";
      displayMessage =
        safeText(row?.gate_message) || "The breakthrough path is under cooldown.";
    } else if (timingStatus === "not_started") {
      progressionState = "breakthrough_required";
      nextAction = "enter_breakthrough";
      displayState = "Breakthrough Required";
      displayMessage =
        safeText(row?.gate_message) ||
        "A breakthrough must be entered before the next stage can open.";
    } else if (timingStatus === "active") {
      progressionState = "breakthrough_active";
      nextAction = "view_breakthrough";
      displayState = "Breakthrough Active";
      displayMessage =
        safeText(row?.gate_message) ||
        `Breakthrough in progress. ${remainingSeconds} seconds remaining.`;
    } else if (timingStatus === "ready_to_resolve") {
      progressionState = "breakthrough_ready";
      nextAction = "resolve_breakthrough";
      displayState = "Ready to Resolve";
      displayMessage =
        safeText(row?.gate_message) ||
        "The breakthrough timer is complete and can now be resolved.";
    } else if (timingStatus === "completed") {
      progressionState = "breakthrough_completed";
      nextAction = "refresh_progression";
      displayState = "Breakthrough Completed";
      displayMessage =
        safeText(row?.gate_message) ||
        "The breakthrough timing has completed.";
    } else {
      progressionState = "breakthrough_present";
      nextAction = "view_breakthrough";
      displayState = "Breakthrough Present";
      displayMessage =
        safeText(row?.gate_message) ||
        "A breakthrough target exists for this member.";
    }
  }

  return {
    exists: !!row,
    applies_to_selected_volume: appliesToSelectedVolume,
    applies_as_source_volume: appliesAsSourceVolume,
    applies_as_target_volume: appliesAsTargetVolume,
    target_id: safeText(row?.target_id || row?.id) || null,
    member_id: safeText(row?.member_id) || null,
    sl_avatar_key: safeText(row?.sl_avatar_key) || null,
    sl_username: safeText(row?.sl_username) || null,
    target_type: targetType || null,
    tribulation_family: safeText(row?.tribulation_family) || null,
    from_volume_number: fromVolumeNumber,
    from_section_key: safeText(row?.from_section_key) || null,
    from_section_label: formatSectionName(row?.from_section_key),
    to_volume_number: toVolumeNumber,
    to_section_key: safeText(row?.to_section_key) || null,
    to_section_label: formatSectionName(row?.to_section_key),
    state_status: stateStatus || null,
    breakthrough_started_at: row?.breakthrough_started_at || null,
    breakthrough_ends_at: row?.breakthrough_ends_at || null,
    breakthrough_completed_at: row?.breakthrough_completed_at || null,
    total_duration_minutes: safeNumber(
      row?.total_duration_minutes ?? row?.breakthrough_duration_minutes,
      0
    ),
    total_duration_seconds: safeNumber(
      row?.total_duration_seconds ?? row?.breakthrough_duration_seconds,
      0
    ),
    remaining_seconds: remainingSeconds,
    countdown_remaining_seconds: remainingSeconds,
    countdown_complete: safeBoolean(row?.countdown_complete),
    timing_status: timingStatus || null,
    can_resolve: safeBoolean(row?.can_resolve),
    gate_status: safeText(row?.gate_status) || null,
    gate_message: safeText(row?.gate_message) || null,
    progression_state: progressionState,
    next_action: nextAction,
    display_state: displayState,
    display_message: displayMessage,
    human_total_duration: formatDuration(
      safeNumber(row?.total_duration_seconds ?? row?.breakthrough_duration_seconds, 0)
    ),
    human_remaining: formatDuration(remainingSeconds)
  };
}

function breakthroughStateMatchesTarget(rawState, targetRow) {
  if (!rawState || !targetRow) return false;

  const rawTargetId = safeText(rawState?.target_id || rawState?.id);
  const targetId = safeText(targetRow?.id);

  if (rawTargetId && targetId && rawTargetId === targetId) {
    return true;
  }

  return (
    safeNumber(rawState?.from_volume_number, 0) === safeNumber(targetRow?.from_volume_number, 0) &&
    safeLower(rawState?.from_section_key) === safeLower(targetRow?.from_section_key) &&
    safeNumber(rawState?.to_volume_number, 0) === safeNumber(targetRow?.to_volume_number, 0) &&
    safeLower(rawState?.to_section_key) === safeLower(targetRow?.to_section_key) &&
    safeLower(rawState?.target_type) === safeLower(targetRow?.target_type)
  );
}

async function loadFreshBreakthroughStateForTarget(slAvatarKey, volumeNumber, targetRow) {
  let rawBreakthroughState = await loadBreakthroughTimingState(slAvatarKey);

  if (!breakthroughStateMatchesTarget(rawBreakthroughState, targetRow)) {
    rawBreakthroughState = buildFallbackTimingStateFromTarget(targetRow);
  }

  return normalizeBreakthroughState(rawBreakthroughState, volumeNumber);
}

function buildProgressMap(progressRows) {
  const map = new Map();

  for (const row of progressRows || []) {
    const sectionKey = safeLower(row?.section_key);
    if (!sectionKey) continue;
    map.set(sectionKey, row);
  }

  return map;
}

function normalizeSectionKey(value, fallback = "base") {
  const key = safeLower(value);
  return VALID_SECTIONS.includes(key) ? key : fallback;
}

function getProgressAccumulatedSeconds(progressRow) {
  if (!progressRow) return 0;

  return (
    safeNumber(progressRow.accumulated_seconds, 0) ||
    safeNumber(progressRow.stored_accumulated_seconds, 0) ||
    safeNumber(progressRow.progress_seconds, 0) ||
    safeNumber(progressRow.section_time_progress, 0)
  );
}

function isProgressStarted(progressRow) {
  if (!progressRow) return false;

  return (
    getProgressAccumulatedSeconds(progressRow) > 0 ||
    !!progressRow.comprehension_started_at ||
    !!progressRow.active_session_started_at ||
    !!progressRow.completed_at
  );
}

function isProgressCompleted(progressRow, volumeNumber = 0, sectionName = "base") {
  if (!progressRow) return false;
  if (progressRow.completed_at) return true;

  const requiredSeconds =
    safeNumber(progressRow.required_seconds, 0) ||
    getRequiredSeconds(volumeNumber, sectionName);

  const accumulatedSeconds = getProgressAccumulatedSeconds(progressRow);

  return requiredSeconds > 0 && accumulatedSeconds >= requiredSeconds;
}

function hasExistingVolumeProgress(libraryRow, progressRows) {
  if (safeNumber(libraryRow?.insight_current, 0) > 0) {
    return true;
  }

  return (progressRows || []).some((row) => isProgressStarted(row));
}

function deriveLiveActiveSection({
  member,
  libraryRow,
  volumeNumber,
  progressMap
}) {
  const currentSection = normalizeSectionKey(libraryRow?.current_section, "");
  if (currentSection) {
    const currentProgress = progressMap.get(currentSection) || null;
    if (!isProgressCompleted(currentProgress, volumeNumber, currentSection)) {
      return currentSection;
    }
  }

  for (const sectionName of VALID_SECTIONS) {
    const progressRow = progressMap.get(sectionName) || null;
    const status = safeLower(libraryRow?.[sectionField(sectionName)]);

    if (progressRow && !isProgressCompleted(progressRow, volumeNumber, sectionName)) {
      return sectionName;
    }

    if (status === "opened") {
      return sectionName;
    }
  }

  const memberVolume =
    parsePositiveInteger(member?.active_volume_number) || null;
  const memberStage = normalizeSectionKey(member?.v2_active_stage_key, "");

  if (
    memberVolume &&
    memberVolume === volumeNumber &&
    memberStage &&
    VALID_SECTIONS.includes(memberStage)
  ) {
    return memberStage;
  }

  for (const sectionName of VALID_SECTIONS) {
    const status = safeLower(libraryRow?.[sectionField(sectionName)]);
    if (status !== "comprehended") {
      return sectionName;
    }
  }

  return "late";
}

function buildBreakthroughTransition({
  member,
  volumeNumber,
  completedSection
}) {
  const sectionKey = safeLower(completedSection);
  const realmIndex = Math.max(
    1,
    safeNumber(member?.realm_index, volumeNumber || 1)
  );

  if (!VALID_SECTIONS.includes(sectionKey)) {
    return null;
  }

  if (sectionKey !== "late") {
    const nextSectionKey = NEXT_SECTION_MAP[sectionKey];
    if (!nextSectionKey) return null;

    return {
      realm_index: realmIndex,
      target_type: "stage",
      from_volume_number: volumeNumber,
      from_section_key: sectionKey,
      to_volume_number: volumeNumber,
      to_section_key: nextSectionKey,
      is_final_transition: false
    };
  }

  if (volumeNumber < MAX_REALM_VOLUME) {
    return {
      realm_index: realmIndex,
      target_type: "realm",
      from_volume_number: volumeNumber,
      from_section_key: "late",
      to_volume_number: volumeNumber + 1,
      to_section_key: "base",
      is_final_transition: false
    };
  }

  return {
    realm_index: realmIndex,
    target_type: "immortal",
    from_volume_number: volumeNumber,
    from_section_key: "late",
    to_volume_number: volumeNumber,
    to_section_key: "late",
    is_final_transition: true
  };
}

function getFallbackTribulationFamily(realmIndex) {
  const safeRealmIndex = Math.max(1, safeNumber(realmIndex, 1));

  if (safeRealmIndex >= 10) return "immortal";
  if (safeRealmIndex >= 5) return "karmic";
  return "tempering";
}

function isCooldownStillActive(targetRow, nowIso) {
  if (safeLower(targetRow?.state_status) !== "cooldown") return false;
  if (!targetRow?.cooldown_ends_at) return false;

  const nowMs = new Date(nowIso).getTime();
  const endsMs = new Date(targetRow.cooldown_ends_at).getTime();

  return Number.isFinite(nowMs) && Number.isFinite(endsMs) && endsMs > nowMs;
}

function isSectionAwaitingBreakthroughHandoff({
  member,
  libraryRow,
  volumeNumber,
  sectionName
}) {
  const transition = buildBreakthroughTransition({
    member,
    volumeNumber,
    completedSection: sectionName
  });

  if (!transition) return false;
  if (safeBoolean(member?.v2_breakthrough_gate_open)) return true;

  if (transition.target_type === "stage") {
    return (
      safeLower(libraryRow?.[sectionField(sectionName)]) === "comprehended" &&
      safeLower(libraryRow?.[sectionField(transition.to_section_key)]) === "sealed"
    );
  }

  if (transition.target_type === "realm" || transition.target_type === "immortal") {
    return (
      safeLower(libraryRow?.[sectionField(sectionName)]) === "comprehended" &&
      safeLower(libraryRow?.volume_status) !== "completed_volume"
    );
  }

  return false;
}

function findPendingBreakthroughHandoffSection({
  member,
  libraryRow,
  volumeNumber,
  progressMap
}) {
  const sectionsDescending = [...VALID_SECTIONS].reverse();

  for (const sectionName of sectionsDescending) {
    const progressRow = progressMap.get(sectionName) || null;

    if (!isProgressCompleted(progressRow, volumeNumber, sectionName)) {
      continue;
    }

    const awaitingHandoff = isSectionAwaitingBreakthroughHandoff({
      member,
      libraryRow,
      volumeNumber,
      sectionName
    });

    if (awaitingHandoff) {
      return sectionName;
    }
  }

  return null;
}

function validateSectionCompletion({
  member,
  libraryRow,
  volumeNumber,
  requestedSection,
  progressMap,
  repairState
}) {
  if (requestedSection && !VALID_SECTIONS.includes(requestedSection)) {
    return {
      ok: false,
      statusCode: 400,
      message: "section_name must be one of: base, early, middle, late."
    };
  }

  const liveSection = deriveLiveActiveSection({
    member,
    libraryRow,
    volumeNumber,
    progressMap
  });

  const pendingHandoffSection = findPendingBreakthroughHandoffSection({
    member,
    libraryRow,
    volumeNumber,
    progressMap
  });

  const targetSection = requestedSection || pendingHandoffSection || liveSection;

  if (
    requestedSection &&
    requestedSection !== liveSection &&
    requestedSection !== pendingHandoffSection
  ) {
    return {
      ok: false,
      statusCode: 409,
      message: `${formatSectionName(liveSection)} is the current live section. Finish that section before completing ${formatSectionName(
        requestedSection
      )}.`
    };
  }

  const targetProgress = progressMap.get(targetSection) || null;

  if (!targetProgress && targetSection !== pendingHandoffSection) {
    return {
      ok: false,
      statusCode: 409,
      message: `${formatSectionName(
        targetSection
      )} has no cultivation timing record yet. Start comprehension and meditate before completing it.`
    };
  }

  const targetNeedsRepair =
    repairState?.applies_to_requested_volume &&
    repairState.needs_repair &&
    safeLower(repairState.section_key) === targetSection;

  if (targetNeedsRepair) {
    return {
      ok: false,
      statusCode: 409,
      message: `${formatSectionName(
        targetSection
      )} is currently damaged and must be repaired before it can be completed.`
    };
  }

  const prerequisiteSection = SECTION_ORDER[targetSection];

  if (prerequisiteSection) {
    const prerequisiteProgress = progressMap.get(prerequisiteSection) || null;

    const prerequisiteNeedsRepair =
      repairState?.applies_to_requested_volume &&
      repairState.needs_repair &&
      safeLower(repairState.section_key) === prerequisiteSection;

    if (prerequisiteNeedsRepair) {
      return {
        ok: false,
        statusCode: 409,
        message: `${formatSectionName(
          prerequisiteSection
        )} must be repaired before ${formatSectionName(
          targetSection
        )} can be completed.`
      };
    }

    const prerequisiteCompleted = isProgressCompleted(
      prerequisiteProgress,
      volumeNumber,
      prerequisiteSection
    );

    if (!prerequisiteCompleted) {
      return {
        ok: false,
        statusCode: 409,
        message: `${formatSectionName(
          prerequisiteSection
        )} must be comprehended before ${formatSectionName(
          targetSection
        )} can be completed.`
      };
    }
  }

  return {
    ok: true,
    section_name: targetSection,
    live_section: liveSection,
    pending_handoff_section: pendingHandoffSection
  };
}

function buildLibraryCompletionUpdates({
  libraryRow,
  volumeNumber,
  sectionName,
  nowIso,
  breakthroughTransition
}) {
  const insightCurrent = safeNumber(libraryRow?.insight_current, 0);
  const insightRequired = safeNumber(libraryRow?.insight_required, 100);
  const insightGain = getSectionInsightGain(sectionName);
  const nextInsight = Math.min(insightCurrent + insightGain, insightRequired);

  const updates = {
    [sectionField(sectionName)]: "comprehended",
    current_section: null,
    insight_current: nextInsight,
    updated_at: nowIso,
    volume_status: "under_comprehension",
    completed_at: null
  };

  if (
    breakthroughTransition &&
    breakthroughTransition.target_type === "stage" &&
    safeNumber(breakthroughTransition.to_volume_number, 0) ===
      safeNumber(volumeNumber, 0) &&
    safeText(breakthroughTransition.to_section_key)
  ) {
    updates[sectionField(breakthroughTransition.to_section_key)] = "sealed";
  }

  const noFurtherBreakthrough = !breakthroughTransition;

  if (sectionName === "late" && noFurtherBreakthrough) {
    updates.volume_status = "completed_volume";
    updates.completed_at = nowIso;
  }

  return {
    updates,
    insight_before: insightCurrent,
    insight_gain: nextInsight - insightCurrent,
    insight_after: nextInsight,
    breakthrough_required: !!breakthroughTransition,
    volume_completed_after_completion:
      safeLower(updates.volume_status) === "completed_volume"
  };
}

function buildTimingSnapshot({
  progressRow,
  member,
  volumeNumber,
  sectionName,
  nowIso
}) {
  const nowMs = new Date(nowIso).getTime();

  const requiredSeconds =
    safeNumber(progressRow?.required_seconds, 0) ||
    getRequiredSeconds(volumeNumber, sectionName);

  const storedAccumulatedSeconds = getProgressAccumulatedSeconds(progressRow);

  let liveSessionSeconds = 0;
  const activeSessionStartedAt = progressRow?.active_session_started_at || null;

  const isRealmCultivationActive =
    safeLower(member?.v2_cultivation_status) === "cultivating";

  if (isRealmCultivationActive && activeSessionStartedAt) {
    const startMs = new Date(activeSessionStartedAt).getTime();

    if (Number.isFinite(startMs) && startMs > 0 && nowMs > startMs) {
      liveSessionSeconds = Math.floor((nowMs - startMs) / 1000);
    }
  }

  const accumulatedSeconds = Math.min(
    requiredSeconds,
    storedAccumulatedSeconds + liveSessionSeconds
  );

  const remainingSeconds = Math.max(0, requiredSeconds - accumulatedSeconds);

  let timerState = "paused";
  if (remainingSeconds <= 0) {
    timerState = "ready_to_complete";
  } else if (isRealmCultivationActive && activeSessionStartedAt) {
    timerState = "cultivating";
  }

  return {
    required_seconds: requiredSeconds,
    stored_accumulated_seconds: storedAccumulatedSeconds,
    live_session_seconds: liveSessionSeconds,
    accumulated_seconds: accumulatedSeconds,
    remaining_seconds: remainingSeconds,
    progress_percent:
      requiredSeconds > 0
        ? Number(((accumulatedSeconds / requiredSeconds) * 100).toFixed(2))
        : 0,
    human_required: formatDuration(requiredSeconds),
    human_accumulated: formatDuration(accumulatedSeconds),
    human_remaining: formatDuration(remainingSeconds),
    timer_state: timerState,
    active_session_started_at: activeSessionStartedAt,
    comprehension_started_at: progressRow?.comprehension_started_at || null,
    completed_at: progressRow?.completed_at || null,
    can_complete: remainingSeconds <= 0
  };
}

function getSimplifiedChamberStatus({
  required,
  breakthroughState
}) {
  if (!required) return null;

  const timingStatus = safeLower(breakthroughState?.timing_status);
  const stateStatus = safeLower(breakthroughState?.state_status);

  if (timingStatus === "ready_to_resolve") {
    return "ready_for_verdict";
  }

  if (timingStatus === "active") {
    return "in_progress";
  }

  if (timingStatus === "completed" || stateStatus === "resolved") {
    return "resolved";
  }

  return "not_started";
}

function buildSimplifiedBreakthroughPayload({
  breakthroughRequired,
  breakthroughTransition,
  breakthroughState,
  repairState,
  targetRow
}) {
  const cooldownActive = safeLower(breakthroughState?.state_status) === "cooldown";
  const repairNeeded =
    repairState?.applies_to_requested_volume && safeBoolean(repairState?.needs_repair);

  const chamberStatus = getSimplifiedChamberStatus({
    required: breakthroughRequired,
    breakthroughState
  });

  const finalOutcome =
    safeText(repairState?.latest_outcome) ||
    (chamberStatus === "resolved"
      ? safeText(breakthroughState?.gate_status) || "resolved"
      : null);

  return {
    required: breakthroughRequired,
    chamber_status: chamberStatus,
    can_battle:
      breakthroughRequired &&
      chamberStatus === "not_started" &&
      !cooldownActive &&
      !repairNeeded,
    can_receive_verdict:
      breakthroughRequired &&
      chamberStatus === "ready_for_verdict" &&
      !repairNeeded,
    repair_needed: repairNeeded,
    cooldown_active: cooldownActive,
    final_outcome: finalOutcome || null,
    route_to_breakthrough: breakthroughRequired,
    target_id:
      breakthroughState?.target_id ||
      safeText(targetRow?.id) ||
      null,
    target_type:
      breakthroughTransition?.target_type ||
      breakthroughState?.target_type ||
      null,
    tribulation_family:
      breakthroughState?.tribulation_family ||
      safeText(targetRow?.tribulation_family) ||
      null,
    from_volume_number:
      breakthroughTransition?.from_volume_number ||
      breakthroughState?.from_volume_number ||
      null,
    from_section_key:
      breakthroughTransition?.from_section_key ||
      breakthroughState?.from_section_key ||
      null,
    to_volume_number:
      breakthroughTransition?.to_volume_number ||
      breakthroughState?.to_volume_number ||
      null,
    to_section_key:
      breakthroughTransition?.to_section_key ||
      breakthroughState?.to_section_key ||
      null,
    timing_status: breakthroughState?.timing_status || null,
    state_status: breakthroughState?.state_status || null,
    remaining_seconds: safeNumber(breakthroughState?.remaining_seconds, 0),
    display_state:
      breakthroughState?.display_state ||
      (breakthroughRequired ? "Breakthrough Required" : "No Breakthrough"),
    display_message:
      breakthroughState?.display_message ||
      (breakthroughRequired
        ? "The next breakthrough must be entered before progression can continue."
        : null),
    next_action:
      breakthroughState?.next_action ||
      (breakthroughRequired ? "enter_breakthrough" : null)
  };
}

function buildCompletionMessage({
  sectionName,
  volumeNumber,
  breakthroughTransition,
  breakthroughState,
  handoffRefreshed = false,
  alreadyCompleted = false
}) {
  if (!breakthroughTransition) {
    if (sectionName === "late") {
      return `Late completed successfully for Volume ${volumeNumber}. The volume is now complete.`;
    }
    return `${formatSectionName(sectionName)} completed successfully for Volume ${volumeNumber}.`;
  }

  const prefix = alreadyCompleted
    ? `${formatSectionName(sectionName)} was already completed for Volume ${volumeNumber}.`
    : `${formatSectionName(sectionName)} completed successfully for Volume ${volumeNumber}.`;

  const suffix =
    breakthroughTransition.target_type === "stage"
      ? `A stage breakthrough into ${formatSectionName(
          breakthroughTransition.to_section_key
        )} is now required.`
      : breakthroughTransition.target_type === "realm"
      ? `A realm breakthrough into Volume ${breakthroughTransition.to_volume_number} is now required.`
      : `The final ascension breakthrough is now required.`;

  if (handoffRefreshed) {
    return `${prefix} Breakthrough handoff was refreshed. ${suffix}`;
  }

  return breakthroughState?.display_message
    ? `${prefix} ${breakthroughState.display_message}`
    : `${prefix} ${suffix}`;
}

function buildNextStep({
  breakthroughRequired,
  volumeCompleted
}) {
  if (breakthroughRequired) return "breakthrough";
  if (volumeCompleted) return "volume_complete";
  return "continue_cultivation";
}

function buildMemberProgressionUpdate({
  member,
  storeVolume,
  volumeNumber,
  realmStageKey,
  realmStageLabel,
  breakthroughRequired,
  nowIso
}) {
  return {
    v2_active_stage_key: realmStageKey,
    v2_breakthrough_gate_open: breakthroughRequired,
    v2_cultivation_status: breakthroughRequired ? "idle" : "idle",
    pause_reason: breakthroughRequired ? "awaiting_breakthrough" : null,
    updated_at: nowIso
  };
}

async function loadMember(slAvatarKey, slUsername) {
  let query = publicSupabase
    .from("cultivation_members")
    .select("*")
    .limit(1);

  if (slAvatarKey) {
    query = query.eq("sl_avatar_key", slAvatarKey);
  }

  if (slUsername) {
    query = query.eq("sl_username", slUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function loadStoreVolume(volumeNumber) {
  let { data, error } = await librarySupabase
    .from("library_store_items")
    .select(`
      id,
      item_key,
      category,
      item_type,
      realm_name,
      volume_number,
      item_name,
      description,
      is_active
    `)
    .eq("category", "cultivation")
    .eq("volume_number", volumeNumber)
    .eq("item_type", "cultivation_volume")
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load store volume: ${error.message}`);
  }

  if (data) return data;

  const fallback = await librarySupabase
    .from("library_store_items")
    .select(`
      id,
      item_key,
      category,
      item_type,
      realm_name,
      volume_number,
      item_name,
      description,
      is_active
    `)
    .eq("category", "cultivation")
    .eq("volume_number", volumeNumber)
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();

  if (fallback.error) {
    throw new Error(
      `Failed to load store volume fallback: ${fallback.error.message}`
    );
  }

  return fallback.data || null;
}

async function loadOwnedLibraryRow(member, storeItemId) {
  let query = librarySupabase
    .from("member_library")
    .select(`
      id,
      sl_avatar_key,
      sl_username,
      store_item_id,
      volume_status,
      insight_current,
      insight_required,
      base_status,
      early_status,
      middle_status,
      late_status,
      current_section,
      owned_at,
      completed_at,
      created_at,
      updated_at
    `)
    .eq("store_item_id", storeItemId)
    .limit(1);

  if (member?.sl_avatar_key) {
    query = query.eq("sl_avatar_key", member.sl_avatar_key);
  }

  if (member?.sl_username) {
    query = query.eq("sl_username", member.sl_username);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load owned library row: ${error.message}`);
  }

  return data || null;
}

async function loadSectionProgress(slAvatarKey, volumeNumber, sectionName) {
  const { data, error } = await librarySupabase
    .from("cultivation_section_progress")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .eq("volume_number", volumeNumber)
    .eq("section_key", sectionName)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load section progress: ${error.message}`);
  }

  return data || null;
}

async function loadVolumeProgressRows(slAvatarKey, volumeNumber) {
  if (!slAvatarKey || !volumeNumber) return [];

  const { data, error } = await librarySupabase
    .from("cultivation_section_progress")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .eq("volume_number", volumeNumber);

  if (error) {
    throw new Error(`Failed to load volume progress rows: ${error.message}`);
  }

  return data || [];
}

async function loadCultivationBookRepairState(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await librarySupabase.rpc(
    "get_cultivation_book_repair_state",
    {
      p_sl_avatar_key: slAvatarKey
    }
  );

  if (error) {
    throw new Error(
      `Failed to load cultivation book repair state: ${error.message}`
    );
  }

  return unwrapRpcRow(data);
}

async function loadBreakthroughTimingState(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await breakthroughSupabase.rpc(
    "get_member_breakthrough_timing_state",
    {
      p_member_id: null,
      p_sl_avatar_key: slAvatarKey
    }
  );

  if (error) {
    throw new Error(`Failed to load breakthrough timing state: ${error.message}`);
  }

  return unwrapRpcRow(data);
}

async function loadTribulationRule(realmIndex, targetType) {
  const { data, error } = await breakthroughSupabase
    .from("tribulation_rule_catalog")
    .select(`
      id,
      rule_key,
      rule_name,
      min_realm_index,
      max_realm_index,
      target_type,
      tribulation_family,
      severe_setback_allowed,
      max_setback_levels,
      minor_retained_comprehension_percent,
      severe_retained_comprehension_percent,
      cp_surcharge_percent,
      cooldown_protection_failure_threshold,
      is_active,
      display_order
    `)
    .eq("is_active", true)
    .eq("target_type", targetType)
    .lte("min_realm_index", realmIndex)
    .gte("max_realm_index", realmIndex)
    .order("display_order", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load tribulation rule: ${error.message}`);
  }

  return (
    data || {
      id: null,
      rule_key: null,
      rule_name: null,
      min_realm_index: realmIndex,
      max_realm_index: realmIndex,
      target_type: targetType,
      tribulation_family: getFallbackTribulationFamily(realmIndex),
      severe_setback_allowed: false,
      max_setback_levels: 0,
      minor_retained_comprehension_percent: 50,
      severe_retained_comprehension_percent: 25,
      cp_surcharge_percent: 0,
      cooldown_protection_failure_threshold: 3,
      is_active: true,
      display_order: 999
    }
  );
}

async function loadExistingBreakthroughTarget(memberId, transition) {
  const { data, error } = await breakthroughSupabase
    .from("member_breakthrough_targets")
    .select("*")
    .eq("member_id", memberId)
    .eq("from_volume_number", transition.from_volume_number)
    .eq("from_section_key", transition.from_section_key)
    .eq("to_volume_number", transition.to_volume_number)
    .eq("to_section_key", transition.to_section_key)
    .eq("target_type", transition.target_type)
    .order("updated_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load existing breakthrough target: ${error.message}`);
  }

  return data || null;
}

async function upsertBreakthroughTarget({
  member,
  transition,
  tribulationRule,
  nowIso
}) {
  if (!transition) return null;

  const existingTarget = await loadExistingBreakthroughTarget(
    member.member_id,
    transition
  );

  const keepCooldown = isCooldownStillActive(existingTarget, nowIso);
  const nextStateStatus = keepCooldown ? "cooldown" : "active";

  const basePayload = {
    sl_avatar_key: safeText(member.sl_avatar_key),
    sl_username: safeText(member.sl_username),
    member_id: member.member_id,
    from_volume_number: transition.from_volume_number,
    from_section_key: transition.from_section_key,
    to_volume_number: transition.to_volume_number,
    to_section_key: transition.to_section_key,
    target_type: transition.target_type,
    tribulation_family:
      safeText(tribulationRule?.tribulation_family) ||
      getFallbackTribulationFamily(transition.realm_index),
    state_status: nextStateStatus,
    breakthrough_started_at: null,
    breakthrough_duration_minutes: null,
    breakthrough_duration_seconds: null,
    breakthrough_ends_at: null,
    breakthrough_completed_at: null,
    updated_at: nowIso
  };

  const cooldownResetFields = keepCooldown
    ? {}
    : {
        cooldown_minutes_current: 0,
        cooldown_started_at: null,
        cooldown_ends_at: null
      };

  if (existingTarget?.id) {
    const { data, error } = await breakthroughSupabase
      .from("member_breakthrough_targets")
      .update({
        ...basePayload,
        ...cooldownResetFields
      })
      .eq("id", existingTarget.id)
      .select("*")
      .maybeSingle();

    if (error || !data) {
      throw new Error(
        error?.message || "Failed to update existing breakthrough target."
      );
    }

    return data;
  }

  const insertPayload = {
    ...basePayload,
    created_at: nowIso
  };

  const { data, error } = await breakthroughSupabase
    .from("member_breakthrough_targets")
    .insert(insertPayload)
    .select("*")
    .maybeSingle();

  if (error || !data) {
    throw new Error(error?.message || "Failed to create breakthrough target.");
  }

  return data;
}

async function checkVolumeEligibility(member, volumeNumber) {
  if (volumeNumber <= 1) {
    return {
      eligible: true,
      previous_volume_number: null,
      previous_volume_completed: true,
      eligibility_reason: "Volume 1 is the starting realm volume."
    };
  }

  const previousVolumeNumber = volumeNumber - 1;
  const previousStoreVolume = await loadStoreVolume(previousVolumeNumber);

  if (!previousStoreVolume) {
    throw new Error(
      `Previous volume ${previousVolumeNumber} was not found in library.library_store_items.`
    );
  }

  const previousLibraryRow = await loadOwnedLibraryRow(member, previousStoreVolume.id);
  const previousCompleted =
    !!previousLibraryRow &&
    safeLower(previousLibraryRow.volume_status) === "completed_volume";

  return {
    eligible: previousCompleted,
    previous_volume_number: previousVolumeNumber,
    previous_volume_completed: previousCompleted,
    eligibility_reason: previousCompleted
      ? `Volume ${previousVolumeNumber} has been completed.`
      : `Complete Volume ${previousVolumeNumber} before progressing this volume.`
  };
}

async function syncMemberAfterCompletion({
  member,
  storeVolume,
  volumeNumber,
  realmStageKey,
  realmStageLabel,
  breakthroughRequired,
  nowIso
}) {
  const updatePayload = buildMemberProgressionUpdate({
    member,
    storeVolume,
    volumeNumber,
    realmStageKey,
    realmStageLabel,
    breakthroughRequired,
    nowIso
  });

  const { error } = await publicSupabase
    .from("cultivation_members")
    .update(updatePayload)
    .eq("member_id", member.member_id)
    .eq("sl_avatar_key", member.sl_avatar_key);

  return error ? error.message : null;
}

function buildSuccessPayload({
  member,
  storeVolume,
  updatedLibraryRow,
  repairState,
  breakthroughTargetRow,
  breakthroughTransition,
  breakthroughState,
  targetSection,
  volumeNumber,
  timing,
  updatedProgressRow,
  eligibility,
  existingProgress,
  stageSyncWarning,
  completionPlan = null,
  handoffRefreshed = false,
  alreadyCompleted = false
}) {
  const realmStageKey = targetSection;
  const realmStageLabel = formatSectionName(targetSection);
  const realmStageDisplayName = getRealmStageDisplayName(
    storeVolume.realm_name,
    targetSection
  );

  const breakthroughRequired = !!breakthroughTransition;
  const volumeCompleted =
    safeLower(updatedLibraryRow?.volume_status) === "completed_volume";

  const normalizedBreakthroughState =
    breakthroughState || normalizeBreakthroughState(null, volumeNumber);

  const simplifiedBreakthrough = buildSimplifiedBreakthroughPayload({
    breakthroughRequired,
    breakthroughTransition,
    breakthroughState: normalizedBreakthroughState,
    repairState,
    targetRow: breakthroughTargetRow
  });

  const message = buildCompletionMessage({
    sectionName: targetSection,
    volumeNumber,
    breakthroughTransition,
    breakthroughState: normalizedBreakthroughState,
    handoffRefreshed,
    alreadyCompleted
  });

  return {
    success: true,
    message,
    next_step: buildNextStep({
      breakthroughRequired,
      volumeCompleted
    }),
    route_to:
      breakthroughRequired ? "breakthrough" : volumeCompleted ? "volume_complete" : "cultivation",
    stage_sync_warning: stageSyncWarning,
    store_volume: {
      id: storeVolume.id,
      category: storeVolume.category || null,
      item_type: storeVolume.item_type || null,
      volume_number: storeVolume.volume_number,
      realm_name: storeVolume.realm_name,
      item_name: storeVolume.item_name,
      item_key: storeVolume.item_key
    },
    member: {
      member_id: member.member_id,
      sl_avatar_key: member.sl_avatar_key,
      sl_username: member.sl_username,
      realm_index: safeNumber(member.realm_index, 0),
      realm_key: safeText(member.realm_key, ""),
      realm_name: safeText(member.realm_name, "Unknown"),
      realm_display_name: safeText(member.realm_display_name, ""),
      cultivation_points_current: safeNumber(member.cultivation_points, 0),
      v2_active_stage_key: realmStageKey,
      realm_stage_label: realmStageLabel,
      realm_stage_display_name: realmStageDisplayName,
      v2_breakthrough_gate_open: breakthroughRequired
    },
    library: {
      id: updatedLibraryRow.id,
      volume_status: updatedLibraryRow.volume_status,
      insight_current: updatedLibraryRow.insight_current,
      insight_required: updatedLibraryRow.insight_required,
      current_section: updatedLibraryRow.current_section,
      sections: {
        base: updatedLibraryRow.base_status,
        early: updatedLibraryRow.early_status,
        middle: updatedLibraryRow.middle_status,
        late: updatedLibraryRow.late_status
      }
    },
    repair_state: repairState,
    breakthrough: {
      created_or_refreshed: !!breakthroughTargetRow,
      handoff_refreshed: handoffRefreshed,
      requires_breakthrough: breakthroughRequired,
      target_id: simplifiedBreakthrough.target_id,
      target_type: simplifiedBreakthrough.target_type,
      tribulation_family: simplifiedBreakthrough.tribulation_family,
      from_volume_number: simplifiedBreakthrough.from_volume_number,
      from_section_key: simplifiedBreakthrough.from_section_key,
      to_volume_number: simplifiedBreakthrough.to_volume_number,
      to_section_key: simplifiedBreakthrough.to_section_key,
      chamber_status: simplifiedBreakthrough.chamber_status,
      can_battle: simplifiedBreakthrough.can_battle,
      can_receive_verdict: simplifiedBreakthrough.can_receive_verdict,
      repair_needed: simplifiedBreakthrough.repair_needed,
      cooldown_active: simplifiedBreakthrough.cooldown_active,
      final_outcome: simplifiedBreakthrough.final_outcome,
      state_status: simplifiedBreakthrough.state_status,
      timing_status: simplifiedBreakthrough.timing_status,
      remaining_seconds: simplifiedBreakthrough.remaining_seconds,
      display_state: simplifiedBreakthrough.display_state,
      display_message: simplifiedBreakthrough.display_message,
      next_action: simplifiedBreakthrough.next_action,
      route_to_breakthrough: simplifiedBreakthrough.route_to_breakthrough,

      progression_state:
        normalizedBreakthroughState.progression_state ||
        (breakthroughRequired ? "breakthrough_required" : "none"),
      breakthrough_started_at:
        normalizedBreakthroughState.breakthrough_started_at || null,
      breakthrough_ends_at:
        normalizedBreakthroughState.breakthrough_ends_at || null,
      breakthrough_completed_at:
        normalizedBreakthroughState.breakthrough_completed_at || null,
      total_duration_seconds:
        normalizedBreakthroughState.total_duration_seconds || 0,
      can_resolve: safeBoolean(normalizedBreakthroughState.can_resolve)
    },
    completion: {
      section_name: targetSection,
      already_completed,
      handoff_refreshed: handoffRefreshed,
      insight_before: completionPlan
        ? completionPlan.insight_before
        : safeNumber(updatedLibraryRow.insight_current, 0),
      insight_gain: completionPlan ? completionPlan.insight_gain : 0,
      insight_after: completionPlan
        ? completionPlan.insight_after
        : safeNumber(updatedLibraryRow.insight_current, 0),
      breakthrough_required: breakthroughRequired,
      volume_completed: volumeCompleted,
      target_type: breakthroughTransition?.target_type || null,
      next_volume_number: breakthroughTransition?.to_volume_number || null,
      next_section_key: breakthroughTransition?.to_section_key || null
    },
    timing: {
      required_seconds: timing.required_seconds,
      accumulated_seconds:
        safeNumber(updatedProgressRow?.accumulated_seconds, timing.accumulated_seconds),
      remaining_seconds: Math.max(
        0,
        safeNumber(timing.required_seconds, 0) -
          safeNumber(updatedProgressRow?.accumulated_seconds, timing.accumulated_seconds)
      ),
      progress_percent:
        timing.required_seconds > 0
          ? Number(
              (
                (safeNumber(updatedProgressRow?.accumulated_seconds, timing.accumulated_seconds) /
                  timing.required_seconds) *
                100
              ).toFixed(2)
            )
          : 0,
      human_required: formatDuration(timing.required_seconds),
      human_accumulated: formatDuration(
        safeNumber(updatedProgressRow?.accumulated_seconds, timing.accumulated_seconds)
      ),
      human_remaining: formatDuration(
        Math.max(
          0,
          safeNumber(timing.required_seconds, 0) -
            safeNumber(updatedProgressRow?.accumulated_seconds, timing.accumulated_seconds)
        )
      ),
      timer_state: updatedProgressRow?.completed_at ? "completed" : timing.timer_state,
      active_session_started_at: updatedProgressRow?.active_session_started_at || null,
      comprehension_started_at: updatedProgressRow?.comprehension_started_at || null,
      completed_at: updatedProgressRow?.completed_at || null
    },
    access: {
      eligible_for_comprehension: true,
      display_access_status: "Eligible for Comprehension",
      eligibility_reason:
        existingProgress && !eligibility.eligible
          ? "Existing progress found in this volume, so access was preserved."
          : eligibility.eligibility_reason,
      previous_volume_number: eligibility.previous_volume_number,
      previous_volume_completed: eligibility.previous_volume_completed
    }
  };
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
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, {
        success: false,
        message: "Missing Supabase environment variables."
      });
    }

    const body = parseBody(event);

    const sessionToken = getSessionToken(event);
    const slAvatarKey = safeText(body.sl_avatar_key);
    const slUsername = safeLower(body.sl_username);
    const volumeNumber = safeNumber(body.volume_number, 0);
    const requestedSection = safeLower(body.section_name);

    if (!sessionToken && !slAvatarKey && !slUsername) {
      return buildResponse(400, {
        success: false,
        message: "Session cookie or sl_avatar_key/sl_username is required."
      });
    }

    if (!Number.isInteger(volumeNumber) || volumeNumber <= 0) {
      return buildResponse(400, {
        success: false,
        message: "A valid volume_number is required."
      });
    }

    if (requestedSection && !VALID_SECTIONS.includes(requestedSection)) {
      return buildResponse(400, {
        success: false,
        message: "section_name must be one of: base, early, middle, late."
      });
    }

    let member = null;
    if (sessionToken) {
      member = await loadMemberFromSession(sessionToken);
    }
    if (!member && (slAvatarKey || slUsername)) {
      member = await loadMember(slAvatarKey, slUsername);
    }

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    const storeVolume = await loadStoreVolume(volumeNumber);

    if (!storeVolume) {
      return buildResponse(404, {
        success: false,
        message: `Volume ${volumeNumber} was not found in the cultivation store library.`
      });
    }

    const libraryRow = await loadOwnedLibraryRow(member, storeVolume.id);

    if (!libraryRow) {
      return buildResponse(404, {
        success: false,
        message: `You do not own Volume ${volumeNumber}.`
      });
    }

    const volumeStatus = safeLower(libraryRow.volume_status);

    if (volumeStatus === "completed_volume" && volumeNumber < MAX_REALM_VOLUME) {
      return buildResponse(409, {
        success: false,
        message: `Volume ${volumeNumber} has already been completed.`,
        volume_status: formatVolumeStatus(libraryRow.volume_status)
      });
    }

    if (
      volumeStatus !== "owned" &&
      volumeStatus !== "under_comprehension" &&
      volumeStatus !== "completed_volume"
    ) {
      return buildResponse(409, {
        success: false,
        message: `Volume ${volumeNumber} is not in a usable state for completion.`,
        volume_status: formatVolumeStatus(libraryRow.volume_status)
      });
    }

    const [eligibility, progressRows, rawRepairState] = await Promise.all([
      checkVolumeEligibility(member, volumeNumber),
      loadVolumeProgressRows(member.sl_avatar_key, volumeNumber),
      loadCultivationBookRepairState(member.sl_avatar_key)
    ]);

    const progressMap = buildProgressMap(progressRows);
    const repairState = normalizeRepairState(rawRepairState, volumeNumber);
    const existingProgress = hasExistingVolumeProgress(libraryRow, progressRows);

    if (!eligibility.eligible && !existingProgress) {
      return buildResponse(409, {
        success: false,
        message: "This volume is sealed until eligible.",
        display_access_status: "Sealed Until Eligible",
        eligibility_reason: eligibility.eligibility_reason,
        previous_volume_number: eligibility.previous_volume_number,
        previous_volume_completed: eligibility.previous_volume_completed
      });
    }

    const sectionValidation = validateSectionCompletion({
      member,
      libraryRow,
      volumeNumber,
      requestedSection,
      progressMap,
      repairState
    });

    if (!sectionValidation.ok) {
      return buildResponse(sectionValidation.statusCode, {
        success: false,
        message: sectionValidation.message,
        requested_section: requestedSection || null,
        repair_state: repairState
      });
    }

    const targetSection = sectionValidation.section_name;
    const progressRow = await loadSectionProgress(
      member.sl_avatar_key,
      volumeNumber,
      targetSection
    );

    if (!progressRow) {
      return buildResponse(409, {
        success: false,
        message: `${formatSectionName(
          targetSection
        )} has no cultivation timing record yet. Start comprehension and meditate before completing it.`,
        requested_section: targetSection
      });
    }

    const nowIso = new Date().toISOString();
    const alreadyCompleted = isProgressCompleted(progressRow, volumeNumber, targetSection);
    const breakthroughTransition = buildBreakthroughTransition({
      member,
      volumeNumber,
      completedSection: targetSection
    });

    if (alreadyCompleted) {
      const awaitingHandoff = isSectionAwaitingBreakthroughHandoff({
        member,
        libraryRow,
        volumeNumber,
        sectionName: targetSection
      });

      if (!awaitingHandoff || !breakthroughTransition) {
        return buildResponse(409, {
          success: false,
          message: `${formatSectionName(targetSection)} has already been completed.`,
          requested_section: targetSection
        });
      }

      const tribulationRule = await loadTribulationRule(
        breakthroughTransition.realm_index,
        breakthroughTransition.target_type
      );

      const breakthroughTargetRow = await upsertBreakthroughTarget({
        member,
        transition: breakthroughTransition,
        tribulationRule,
        nowIso
      });

      const breakthroughState = await loadFreshBreakthroughStateForTarget(
        member.sl_avatar_key,
        volumeNumber,
        breakthroughTargetRow
      );

      const stageSyncWarning = await syncMemberAfterCompletion({
        member,
        storeVolume,
        volumeNumber,
        realmStageKey: targetSection,
        realmStageLabel: formatSectionName(targetSection),
        breakthroughRequired: true,
        nowIso
      });

      const timing = buildTimingSnapshot({
        progressRow,
        member: {
          ...member,
          v2_cultivation_status: "idle"
        },
        volumeNumber,
        sectionName: targetSection,
        nowIso
      });

      return buildResponse(
        200,
        buildSuccessPayload({
          member,
          storeVolume,
          updatedLibraryRow: libraryRow,
          repairState,
          breakthroughTargetRow,
          breakthroughTransition,
          breakthroughState,
          targetSection,
          volumeNumber,
          timing,
          updatedProgressRow: progressRow,
          eligibility,
          existingProgress,
          stageSyncWarning,
          completionPlan: null,
          handoffRefreshed: true,
          alreadyCompleted: true
        })
      );
    }

    const timing = buildTimingSnapshot({
      progressRow,
      member,
      volumeNumber,
      sectionName: targetSection,
      nowIso
    });

    if (!timing.can_complete) {
      return buildResponse(409, {
        success: false,
        message: `${formatSectionName(
          targetSection
        )} is not ready to complete yet. More meditation time is required.`,
        requested_section: targetSection,
        timing
      });
    }

    const completionPlan = buildLibraryCompletionUpdates({
      libraryRow,
      volumeNumber,
      sectionName: targetSection,
      nowIso,
      breakthroughTransition
    });

    const { data: updatedLibraryRow, error: libraryUpdateError } =
      await librarySupabase
        .from("member_library")
        .update(completionPlan.updates)
        .eq("id", libraryRow.id)
        .select(`
          id,
          sl_avatar_key,
          sl_username,
          store_item_id,
          volume_status,
          insight_current,
          insight_required,
          base_status,
          early_status,
          middle_status,
          late_status,
          current_section,
          owned_at,
          completed_at,
          created_at,
          updated_at
        `)
        .maybeSingle();

    if (libraryUpdateError || !updatedLibraryRow) {
      throw new Error(
        libraryUpdateError?.message || "Failed to update library.member_library."
      );
    }

    const finalAccumulated = timing.accumulated_seconds;

    const { data: updatedProgressRow, error: progressUpdateError } =
      await librarySupabase
        .from("cultivation_section_progress")
        .update({
          required_seconds: timing.required_seconds,
          accumulated_seconds: finalAccumulated,
          active_session_started_at: null,
          completed_at: nowIso,
          updated_at: nowIso
        })
        .eq("id", progressRow.id)
        .select("*")
        .maybeSingle();

    if (progressUpdateError || !updatedProgressRow) {
      throw new Error(
        progressUpdateError?.message || "Failed to finalize section timing record."
      );
    }

    let breakthroughTargetRow = null;
    let breakthroughState = normalizeBreakthroughState(null, volumeNumber);

    if (breakthroughTransition) {
      const tribulationRule = await loadTribulationRule(
        breakthroughTransition.realm_index,
        breakthroughTransition.target_type
      );

      breakthroughTargetRow = await upsertBreakthroughTarget({
        member,
        transition: breakthroughTransition,
        tribulationRule,
        nowIso
      });

      breakthroughState = await loadFreshBreakthroughStateForTarget(
        member.sl_avatar_key,
        volumeNumber,
        breakthroughTargetRow
      );
    }

    const stageSyncWarning = await syncMemberAfterCompletion({
      member,
      storeVolume,
      volumeNumber,
      realmStageKey: targetSection,
      realmStageLabel: formatSectionName(targetSection),
      breakthroughRequired: !!breakthroughTransition,
      nowIso
    });

    return buildResponse(
      200,
      buildSuccessPayload({
        member,
        storeVolume,
        updatedLibraryRow,
        repairState,
        breakthroughTargetRow,
        breakthroughTransition,
        breakthroughState,
        targetSection,
        volumeNumber,
        timing: {
          ...timing,
          accumulated_seconds: finalAccumulated,
          remaining_seconds: 0,
          progress_percent: 100,
          timer_state: "completed"
        },
        updatedProgressRow,
        eligibility,
        existingProgress,
        stageSyncWarning,
        completionPlan,
        handoffRefreshed: false,
        alreadyCompleted: false
      })
    );
  } catch (error) {
    console.error("complete-library-section error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to complete library section.",
      error: error.message || "Unknown error."
    });
  }
};