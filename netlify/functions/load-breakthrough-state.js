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

// =========================================================
// CONSTANTS
// =========================================================

const TERMINAL_LIFECYCLES = [
  "abandoned"
];

const ELAPSED_OR_LATER_LIFECYCLES = [
  "timer_elapsed",
  "battle_resolved",
  "verdict_revealed",
  "cooldown",
  "success",
  "failed_stable",
  "failed_damaged",
  "abandoned"
];

// =========================================================
// UTILITIES
// =========================================================

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, OPTIONS"
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
    const rawValue = trimmed.slice(eqIndex + 1).trim();

    try {
      cookies[key] = decodeURIComponent(rawValue);
    } catch {
      cookies[key] = rawValue;
    }
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

function safeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1" || value === "true") return true;
  if (value === 0 || value === "0" || value === "false") return false;
  return fallback;
}

function toTitle(value, fallback = "") {
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

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(safeNumber(totalSeconds, 0)));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;

  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function firstNonNullNumber(...values) {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function firstNonEmptyText(...values) {
  for (const value of values) {
    const text = safeText(value);
    if (text) return text;
  }
  return null;
}

function diffSeconds(startValue, endValue) {
  const startText = safeText(startValue);
  const endText = safeText(endValue);

  if (!startText || !endText) return null;

  const startDate = new Date(startText);
  const endDate = new Date(endText);

  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return null;
  }

  const diffMs = endDate.getTime() - startDate.getTime();
  if (!Number.isFinite(diffMs) || diffMs <= 0) return null;

  return Math.round(diffMs / 1000);
}

function isNonFatalResolveStateError(error) {
  const message = safeLower(error?.message);

  return (
    message.includes("not elapsed") ||
    message.includes("not completed") ||
    message.includes("timer has not") ||
    message.includes("cannot resolve") ||
    message.includes("already resolved") ||
    message.includes("already terminal") ||
    message.includes("no active breakthrough") ||
    message.includes("still running") ||
    message.includes("not ready")
  );
}

// =========================================================
// V2 DATA LOADERS
// =========================================================

async function loadSession(sessionToken) {
  if (!sessionToken) return null;

  const { data, error } = await supabase
    .from("website_sessions")
    .select("*")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    throw new Error(`Session lookup failed: ${error.message}`);
  }

  return data || null;
}

async function loadMember(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      realm_index,
      realm_key,
      realm_name,
      realm_display_name,
      vestiges,
      gender,
      auric_current,
      auric_maximum,
      v2_cultivation_status,
      v2_active_stage_key,
      v2_breakthrough_gate_open,
      v2_stage_needs_repair,
      v2_accumulated_seconds,
      v2_updated_at
    `)
    .eq("sl_avatar_key", slAvatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Member lookup failed: ${error.message}`);
  }

  return data || null;
}

async function promoteCountdownToActive(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase.schema("breakthrough").rpc(
    "v2_promote_countdown_to_active",
    { p_sl_avatar_key: slAvatarKey }
  );

  if (error) {
    throw new Error(`Countdown promotion failed: ${error.message}`);
  }

  return data || null;
}

async function promoteActiveToTimerElapsed(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase.schema("breakthrough").rpc(
    "v2_promote_active_to_timer_elapsed",
    { p_sl_avatar_key: slAvatarKey }
  );

  if (error) {
    throw new Error(`Active promotion failed: ${error.message}`);
  }

  return data || null;
}

async function loadV2BreakthroughState(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase
    .schema("breakthrough")
    .from("v2_member_breakthrough_state")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .not("lifecycle_status", "in", `("${TERMINAL_LIFECYCLES.join('","')}")`)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`V2 breakthrough state load failed: ${error.message}`);
  }

  return data || null;
}

async function loadV2LatestAttemptLog(breakthroughStateId) {
  if (!breakthroughStateId) return null;

  const { data, error } = await supabase
    .schema("breakthrough")
    .from("v2_breakthrough_attempt_log")
    .select("*")
    .eq("breakthrough_state_id", breakthroughStateId)
    .order("attempt_number", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("load-breakthrough-state attempt log load error:", error);
    return null;
  }

  return data || null;
}

async function loadV2RecentAttemptHistory(slAvatarKey, limit = 5) {
  if (!slAvatarKey) return [];

  const { data, error } = await supabase
    .schema("breakthrough")
    .from("v2_breakthrough_attempt_log")
    .select(`
      id,
      breakthrough_state_id,
      attempt_number,
      from_volume_number,
      from_section_key,
      to_volume_number,
      to_section_key,
      target_type,
      outcome,
      verdict_key,
      verdict_text,
      setback_level,
      retained_comprehension_percent,
      repair_cp_surcharge_percent,
      cooldown_minutes_applied,
      heavens_forgiveness_applied,
      verdict_revealed_at
    `)
    .eq("sl_avatar_key", slAvatarKey)
    .order("verdict_revealed_at", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("load-breakthrough-state attempt history load error:", error);
    return [];
  }

  return Array.isArray(data) ? data : [];
}

async function loadV2LinkedStageState(stageStateId) {
  if (!stageStateId) return null;

  const { data, error } = await librarySupabase
    .from("v2_member_stage_state")
    .select(`
      id,
      volume_number,
      section_key,
      stage_status,
      required_seconds,
      accumulated_seconds,
      needs_repair,
      repair_cp_cost,
      repair_resume_from_seconds,
      open_cp_cost_paid,
      opened_at,
      cultivation_completed_at,
      comprehended_at
    `)
    .eq("id", stageStateId)
    .maybeSingle();

  if (error) {
    console.error("load-breakthrough-state linked stage state load error:", error);
    return null;
  }

  return data || null;
}

async function loadLifecycleCatalog() {
  const { data, error } = await supabase
    .schema("breakthrough")
    .from("v2_lifecycle_status_catalog")
    .select("status_key,label,is_terminal,notes,display_order")
    .order("display_order");

  if (error) {
    console.error("load-breakthrough-state lifecycle catalog load error:", error);
    return [];
  }

  return Array.isArray(data) ? data : [];
}

// =========================================================
// NEXT ACTION RESOLVER
// =========================================================

function resolveNextAction(lifecycle) {
  switch (lifecycle) {
    case "countdown":
    case "pending":
      return {
        action: "wait_for_countdown",
        label: "Countdown In Progress",
        route: null,
        can_act: false
      };

    case "active":
      return {
        action: "wait_for_timer",
        label: "Breakthrough In Progress",
        route: null,
        can_act: false
      };

    case "timer_elapsed":
      return {
        action: "resolve_battle",
        label: "Resolve Battle",
        route: "/breakthrough.html",
        can_act: true
      };

    case "battle_resolved":
      return {
        action: "reveal_verdict",
        label: "Receive Heaven's Verdict",
        route: "/breakthrough.html",
        can_act: true
      };

    case "verdict_revealed":
      return {
        action: "view_result",
        label: "View Result",
        route: "/breakthrough.html",
        can_act: true
      };

    case "cooldown":
      return {
        action: "wait_cooldown",
        label: "Cooldown Active",
        route: null,
        can_act: false
      };

    default:
      return {
        action: "none",
        label: "No Action Available",
        route: null,
        can_act: false
      };
  }
}

// =========================================================
// BREAKTHROUGH PAYLOAD BUILDER
// =========================================================

function buildBreakthroughPayload({
  btRow,
  latestAttempt,
  linkedStage,
  lifecycleCatalog
}) {
  if (!btRow) {
    return {
      exists: false,
      lifecycle_status: null,
      lifecycle_label: null,
      lifecycle_notes: null,
      is_terminal: false,
      next_action: {
        action: "none",
        label: "No Active Breakthrough",
        route: null,
        can_act: false
      },

      breakthrough_state_id: null,
      from_volume_number: null,
      from_section_key: null,
      to_volume_number: null,
      to_section_key: null,
      target_type: null,
      tribulation_family: null,
      stage_state_id: null,

      countdown_active: false,
      countdown_started_at: null,
      countdown_ends_at: null,
      countdown_duration_seconds: null,

      timer_started: false,
      timer_elapsed: false,
      seconds_remaining: null,
      human_remaining: null,
      breakthrough_started_at: null,
      breakthrough_duration_seconds: null,
      breakthrough_ends_at: null,
      breakthrough_elapsed_at: null,

      battle_status: "not_started",
      tribulation_power: null,
      battle_roll: null,
      battle_power: null,

      outcome: null,
      verdict_key: null,
      verdict_text: null,
      verdict_revealed_at: null,
      heavens_forgiveness_applied: false,

      reward_tokens: 0,
      insight_awarded: 0,
      repair_cp_cost: 0,
      new_realm_key: null,
      new_realm_stage_key: null,
      new_realm_index: null,
      is_realm_transition: false,

      setback_level: 0,
      retained_comprehension_percent: 100,
      repair_cp_surcharge_percent: 0,
      stage_damaged: false,

      cooldown_active: false,
      cooldown_minutes: 0,
      cooldown_started_at: null,
      cooldown_ends_at: null,

      total_attempts: 0,
      total_failures: 0,
      consecutive_failures: 0,
      protection_mode_active: false,

      linked_stage: null,
      latest_attempt: null,

      created_at: null,
      updated_at: null
    };
  }

  const lifecycle = safeText(btRow.lifecycle_status, "countdown");
  const lifecycleEntry =
    lifecycleCatalog.find((entry) => entry.status_key === lifecycle) || null;

  const remainingSeconds = firstNonNullNumber(
    btRow.countdown_remaining_seconds,
    btRow.remaining_seconds,
    btRow.seconds_remaining
  );

  const countdownDurationSeconds = firstNonNullNumber(
    btRow.countdown_duration_seconds,
    latestAttempt?.countdown_duration_seconds,
    diffSeconds(btRow.countdown_started_at, btRow.countdown_ends_at),
    (btRow.countdown_started_at || btRow.countdown_ends_at || lifecycle === "countdown") ? 60 : null
  );

  const breakthroughDurationSeconds = firstNonNullNumber(
    btRow.breakthrough_duration_seconds,
    btRow.total_duration_seconds,
    latestAttempt?.breakthrough_duration_seconds
  );

  const rewardTokens = firstNonNullNumber(
    btRow.reward_tokens,
    latestAttempt?.reward_tokens,
    0
  );

  const insightAwarded = firstNonNullNumber(
    btRow.insight_awarded,
    latestAttempt?.insight_awarded,
    0
  );

  const repairCpCost = firstNonNullNumber(
    btRow.repair_cp_cost,
    latestAttempt?.repair_cp_cost,
    linkedStage?.repair_cp_cost,
    0
  );

  const newRealmIndex = firstNonNullNumber(
    btRow.new_realm_index,
    latestAttempt?.new_realm_index
  );

  const newRealmKey = firstNonEmptyText(
    btRow.new_realm_key,
    latestAttempt?.new_realm_key
  );

  const newRealmStageKey = firstNonEmptyText(
    btRow.new_realm_stage_key,
    latestAttempt?.new_realm_stage_key
  );

  const timerStarted = Boolean(btRow.breakthrough_started_at);
  const timerElapsed =
    Boolean(btRow.breakthrough_elapsed_at) ||
    ELAPSED_OR_LATER_LIFECYCLES.includes(safeLower(lifecycle));

  const nextAction = resolveNextAction(lifecycle);

  let linkedStagePayload = null;
  if (linkedStage) {
    const required = safeNumber(linkedStage.required_seconds, 0);
    const accumulated = safeNumber(linkedStage.accumulated_seconds, 0);

    linkedStagePayload = {
      stage_state_id: linkedStage.id,
      volume_number: linkedStage.volume_number,
      section_key: linkedStage.section_key,
      stage_status: safeText(linkedStage.stage_status) || null,
      required_seconds: required,
      accumulated_seconds: accumulated,
      progress_pct:
        required > 0 ? Math.min(100, Math.round((accumulated / required) * 100)) : 0,
      needs_repair: safeBoolean(linkedStage.needs_repair),
      repair_cp_cost: safeNumber(linkedStage.repair_cp_cost, 0),
      repair_resume_from_seconds:
        linkedStage.repair_resume_from_seconds ?? null,
      open_cp_cost_paid: safeNumber(linkedStage.open_cp_cost_paid, 0),
      opened_at: linkedStage.opened_at || null,
      cultivation_completed_at: linkedStage.cultivation_completed_at || null,
      comprehended_at: linkedStage.comprehended_at || null
    };
  }

  let latestAttemptPayload = null;
  if (latestAttempt) {
    latestAttemptPayload = {
      log_id: latestAttempt.id,
      breakthrough_state_id: latestAttempt.breakthrough_state_id || null,
      attempt_number: safeNumber(latestAttempt.attempt_number, 0),
      from_volume_number: latestAttempt.from_volume_number ?? null,
      from_section_key: safeText(latestAttempt.from_section_key) || null,
      to_volume_number: latestAttempt.to_volume_number ?? null,
      to_section_key: safeText(latestAttempt.to_section_key) || null,
      outcome: safeText(latestAttempt.outcome) || null,
      verdict_key: safeText(latestAttempt.verdict_key) || null,
      verdict_text: safeText(latestAttempt.verdict_text) || null,
      setback_level: safeNumber(latestAttempt.setback_level, 0),
      retained_comprehension_percent: safeNumber(
        latestAttempt.retained_comprehension_percent,
        100
      ),
      repair_cp_surcharge_percent: safeNumber(
        latestAttempt.repair_cp_surcharge_percent,
        0
      ),
      repair_cp_cost: safeNumber(latestAttempt.repair_cp_cost, 0),
      cooldown_minutes_applied: safeNumber(
        latestAttempt.cooldown_minutes_applied,
        0
      ),
      heavens_forgiveness_applied: safeBoolean(
        latestAttempt.heavens_forgiveness_applied
      ),
      comprehension_ready: safeBoolean(latestAttempt.comprehension_ready),
      foundation_stable: safeBoolean(latestAttempt.foundation_stable),
      qi_ready: safeBoolean(latestAttempt.qi_ready),
      tribulation_power: latestAttempt.tribulation_power ?? null,
      battle_roll: latestAttempt.battle_roll ?? null,
      battle_power: latestAttempt.battle_power ?? null,
      reward_tokens: safeNumber(latestAttempt.reward_tokens, 0),
      insight_awarded: safeNumber(latestAttempt.insight_awarded, 0),
      new_realm_key: safeText(latestAttempt.new_realm_key) || null,
      new_realm_stage_key: safeText(latestAttempt.new_realm_stage_key) || null,
      new_realm_index: firstNonNullNumber(latestAttempt.new_realm_index),
      is_realm_transition: safeBoolean(latestAttempt.is_realm_transition),
      verdict_revealed_at: latestAttempt.verdict_revealed_at || null
    };
  }

  return {
    exists: true,
    lifecycle_status: lifecycle,
    lifecycle_label: lifecycleEntry?.label || toTitle(lifecycle, "Active"),
    lifecycle_notes: lifecycleEntry?.notes || null,
    is_terminal: safeBoolean(lifecycleEntry?.is_terminal, false),
    next_action: nextAction,

    breakthrough_state_id: btRow.id,
    from_volume_number: btRow.from_volume_number ?? null,
    from_section_key: safeText(btRow.from_section_key) || null,
    to_volume_number: btRow.to_volume_number ?? null,
    to_section_key: safeText(btRow.to_section_key) || null,
    target_type: safeText(btRow.target_type) || null,
    tribulation_family: safeText(btRow.tribulation_family) || null,
    stage_state_id: btRow.stage_state_id || null,

    countdown_active: lifecycle === "countdown" || lifecycle === "pending",
    countdown_started_at: btRow.countdown_started_at || null,
    countdown_ends_at: btRow.countdown_ends_at || null,
    countdown_duration_seconds: countdownDurationSeconds,

    timer_started: timerStarted,
    timer_elapsed: timerElapsed,
    seconds_remaining: remainingSeconds,
    human_remaining:
      remainingSeconds !== null ? formatDuration(remainingSeconds) : null,
    breakthrough_started_at: btRow.breakthrough_started_at || null,
    breakthrough_duration_seconds: breakthroughDurationSeconds,
    breakthrough_ends_at: btRow.breakthrough_ends_at || null,
    breakthrough_elapsed_at: btRow.breakthrough_elapsed_at || null,

    battle_status: safeText(btRow.battle_status, "not_started"),
    tribulation_power: btRow.tribulation_power ?? null,
    battle_roll: btRow.battle_roll ?? null,
    battle_power: btRow.battle_power ?? null,

    outcome: safeText(btRow.outcome) || null,
    verdict_key: safeText(btRow.verdict_key) || null,
    verdict_text: safeText(btRow.verdict_text) || null,
    verdict_revealed_at: btRow.verdict_revealed_at || null,
    heavens_forgiveness_applied: safeBoolean(
      btRow.heavens_forgiveness_applied,
      safeBoolean(latestAttempt?.heavens_forgiveness_applied)
    ),

    reward_tokens: safeNumber(rewardTokens, 0),
    insight_awarded: safeNumber(insightAwarded, 0),
    repair_cp_cost: safeNumber(repairCpCost, 0),
    new_realm_key: newRealmKey,
    new_realm_stage_key: newRealmStageKey,
    new_realm_index: newRealmIndex,
    is_realm_transition: safeBoolean(
      btRow.is_realm_transition,
      safeBoolean(latestAttempt?.is_realm_transition)
    ),

    setback_level: safeNumber(btRow.setback_level, 0),
    retained_comprehension_percent: safeNumber(
      btRow.retained_comprehension_percent,
      100
    ),
    repair_cp_surcharge_percent: safeNumber(
      btRow.repair_cp_surcharge_percent,
      0
    ),
    stage_damaged: safeBoolean(btRow.stage_damaged),

    cooldown_active: safeBoolean(btRow.cooldown_active),
    cooldown_minutes: safeNumber(btRow.cooldown_minutes, 0),
    cooldown_started_at: btRow.cooldown_started_at || null,
    cooldown_ends_at: btRow.cooldown_ends_at || null,

    total_attempts: safeNumber(btRow.total_attempts, 0),
    total_failures: safeNumber(btRow.total_failures, 0),
    consecutive_failures: safeNumber(btRow.consecutive_failures, 0),
    protection_mode_active: safeBoolean(btRow.protection_mode_active),

    linked_stage: linkedStagePayload,
    latest_attempt: latestAttemptPayload,

    created_at: btRow.created_at || null,
    updated_at: btRow.updated_at || null
  };
}

// =========================================================
// MAIN HANDLER
// =========================================================

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return json(200, { success: true });
  }

  if (event.httpMethod !== "GET") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const cookieHeader = event.headers.cookie || event.headers.Cookie || "";
    const cookies = parseCookies(cookieHeader);
    const cookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = cookies[cookieName];

    if (!sessionToken) {
      return json(401, {
        success: false,
        error: "not_logged_in"
      });
    }

    let sessionRow;
    try {
      sessionRow = await loadSession(sessionToken);
    } catch (e) {
      console.error("load-breakthrough-state session error:", e);
      return json(500, {
        success: false,
        error: "session_lookup_failed",
        details: e.message
      });
    }

    if (!sessionRow) {
      return json(401, {
        success: false,
        error: "invalid_session"
      });
    }

    let member;
    try {
      member = await loadMember(sessionRow.sl_avatar_key);
    } catch (e) {
      console.error("load-breakthrough-state member error:", e);
      return json(500, {
        success: false,
        error: "member_lookup_failed",
        details: e.message
      });
    }

    if (!member) {
      return json(404, {
        success: false,
        error: "member_not_found"
      });
    }

    let ascensionTokensBalance = 0;
    try {
      const { data: walletRow } = await supabase
        .from("member_wallets")
        .select("ascension_tokens_balance")
        .eq("sl_avatar_key", member.sl_avatar_key)
        .maybeSingle();
      ascensionTokensBalance = walletRow?.ascension_tokens_balance ?? 0;
    } catch (e) {
      ascensionTokensBalance = 0;
    }

    let btRow = null;
    let latestAttempt = null;
    let recentHistory = [];
    let linkedStage = null;
    let lifecycleCatalog = [];
    const warnings = [];

    try {
      await promoteCountdownToActive(member.sl_avatar_key);
    } catch (e) {
      warnings.push("Could not promote countdown breakthrough state.");
      console.error("load-breakthrough-state countdown promotion error:", e);
    }

    try {
      await promoteActiveToTimerElapsed(member.sl_avatar_key);
    } catch (e) {
      warnings.push("Could not promote active breakthrough to timer elapsed.");
      console.error("load-breakthrough-state active promotion error:", e);
    }

    try {
      const [btResult, catalogResult] = await Promise.allSettled([
        loadV2BreakthroughState(member.sl_avatar_key),
        loadLifecycleCatalog()
      ]);

      if (btResult.status === "fulfilled") {
        btRow = btResult.value;
      } else {
        warnings.push("Could not load breakthrough state.");
        console.error("load-breakthrough-state V2 bt state error:", btResult.reason);
      }

      if (catalogResult.status === "fulfilled") {
        lifecycleCatalog = catalogResult.value;
      } else {
        warnings.push("Could not load lifecycle catalog.");
        console.error("load-breakthrough-state lifecycle catalog error:", catalogResult.reason);
      }
    } catch (e) {
      console.error("load-breakthrough-state initial load error:", e);
      warnings.push("Could not fully load breakthrough data.");
    }

    try {
      const [historyResult, attemptResult, stageResult] = await Promise.allSettled([
        loadV2RecentAttemptHistory(member.sl_avatar_key, 5),
        btRow ? loadV2LatestAttemptLog(btRow.id) : Promise.resolve(null),
        btRow ? loadV2LinkedStageState(btRow.stage_state_id) : Promise.resolve(null)
      ]);

      if (historyResult.status === "fulfilled") {
        recentHistory = historyResult.value;
      } else {
        warnings.push("Could not load recent attempt history.");
        console.error("load-breakthrough-state history error:", historyResult.reason);
      }

      if (attemptResult.status === "fulfilled") {
        latestAttempt = attemptResult.value;
      } else {
        warnings.push("Could not load latest attempt log.");
        console.error("load-breakthrough-state latest attempt error:", attemptResult.reason);
      }

      if (stageResult.status === "fulfilled") {
        linkedStage = stageResult.value;
      } else {
        warnings.push("Could not load linked stage state.");
        console.error("load-breakthrough-state linked stage error:", stageResult.reason);
      }
    } catch (e) {
      console.error("load-breakthrough-state post-sync load error:", e);
      warnings.push("Could not load all breakthrough details.");
    }

    const breakthroughPayload = buildBreakthroughPayload({
      btRow,
      latestAttempt,
      linkedStage,
      lifecycleCatalog
    });

    // If bond breakthrough, fetch partner info
    let bondPartner = null;
    let bondBreakthroughState = null;
    const isBond = breakthroughPayload.target_type === "bond";

    if (isBond && btRow?.partnership_id) {
      try {
        const { data: partnership } = await supabase
          .schema("partner")
          .from("cultivation_partnerships")
          .select("requester_avatar_key, recipient_avatar_key")
          .eq("id", btRow.partnership_id)
          .maybeSingle();

        if (partnership) {
          const selfKey = safeText(member.sl_avatar_key).toLowerCase();
          const partnerKey = safeText(partnership.requester_avatar_key).toLowerCase() === selfKey
            ? partnership.recipient_avatar_key
            : partnership.requester_avatar_key;

          const { data: partnerMember } = await supabase
            .from("cultivation_members")
            .select("sl_avatar_key, sl_username, display_name, character_name, realm_display_name, realm_index, gender")
            .eq("sl_avatar_key", partnerKey)
            .maybeSingle();

          const { data: partnerStats } = await supabase
            .from("cultivator_stats")
            .select("vitality, will, resonance, insight")
            .eq("sl_avatar_key", partnerKey)
            .maybeSingle();

          const { data: partnerBt } = await supabase
            .schema("breakthrough")
            .from("v2_member_breakthrough_state")
            .select("id, lifecycle_status, countdown_ends_at, breakthrough_ends_at")
            .eq("sl_avatar_key", partnerKey)
            .eq("partnership_id", btRow.partnership_id)
            .not("lifecycle_status", "in", '("success","failed_stable","failed_damaged","abandoned")')
            .order("created_at", { ascending: false })
            .limit(1)
            .maybeSingle();

          bondPartner = {
            sl_avatar_key: safeText(partnerMember?.sl_avatar_key) || partnerKey,
            sl_username: safeText(partnerMember?.sl_username) || null,
            display_name: safeText(partnerMember?.display_name) || null,
            character_name: safeText(partnerMember?.character_name) || null,
            realm_display_name: safeText(partnerMember?.realm_display_name) || null,
            realm_index: safeNumber(partnerMember?.realm_index, 1),
            gender: partnerMember?.gender || "male",
            stats: partnerStats ? {
              vitality: safeNumber(partnerStats.vitality, 0),
              will: safeNumber(partnerStats.will, 0),
              resonance: safeNumber(partnerStats.resonance, 0),
              insight: safeNumber(partnerStats.insight, 0)
            } : null,
            breakthrough_state_id: partnerBt?.id || null,
            lifecycle_status: safeText(partnerBt?.lifecycle_status) || "pending",
            has_entered: !!partnerBt
          };
        }

        // Bond breakthrough state row
        const { data: bondBtState } = await supabase
          .schema("breakthrough")
          .from("bond_breakthrough_state")
          .select("*")
          .eq("partnership_id", btRow.partnership_id)
          .eq("bond_volume_number", btRow.bond_volume_number)
          .maybeSingle();

        bondBreakthroughState = bondBtState || null;
      } catch (bondErr) {
        console.error("load-breakthrough-state bond partner fetch error:", bondErr);
        warnings.push("Could not load bond partner information.");
      }
    }

    return json(200, {
      success: true,

      viewer: {
        member_id: safeText(member.member_id) || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username),
        display_name: safeText(member.display_name) || null,
        character_name: safeText(member.character_name) || null
      },

      member: {
        member_id: safeText(member.member_id) || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username),
        display_name: safeText(member.display_name) || null,
        character_name: safeText(member.character_name) || null,
        realm_index: safeNumber(member.realm_index, 1),
        realm_key: safeText(member.realm_key) || null,
        realm_name: safeText(member.realm_name) || null,
        realm_display_name: safeText(member.realm_display_name) || null,
        vestiges: safeNumber(member.vestiges, 0),
        gender: member.gender || 'male',
        ascension_tokens_balance: ascensionTokensBalance,
        auric_current: safeNumber(member.auric_current, 0),
        auric_maximum: safeNumber(member.auric_maximum, 0),
        v2_cultivation_status: safeText(member.v2_cultivation_status, "idle"),
        v2_active_stage_key: safeText(member.v2_active_stage_key) || null,
        v2_breakthrough_gate_open: safeBoolean(member.v2_breakthrough_gate_open),
        v2_stage_needs_repair: safeBoolean(member.v2_stage_needs_repair),
        v2_accumulated_seconds: safeNumber(member.v2_accumulated_seconds, 0),
        v2_updated_at: member.v2_updated_at || null
      },

      breakthrough: breakthroughPayload,
      recent_attempts: recentHistory,

      has_active_breakthrough: breakthroughPayload.exists,
      lifecycle_status: breakthroughPayload.lifecycle_status,
      next_action: breakthroughPayload.next_action,

      is_bond_breakthrough: isBond,
      bond_partner: bondPartner,
      bond_breakthrough_state: bondBreakthroughState,
      partnership_id: isBond ? (btRow?.partnership_id || null) : null,
      bond_volume_number: isBond ? (btRow?.bond_volume_number || null) : null,

      warnings: warnings.length > 0 ? warnings : null
    });
  } catch (err) {
    console.error("load-breakthrough-state server error:", err);
    return json(500, {
      success: false,
      error: "server_error",
      details: err?.message || String(err)
    });
  }
};
