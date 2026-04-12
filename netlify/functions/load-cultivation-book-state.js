const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

// =========================================================
// CONSTANTS
// =========================================================

const SECTION_KEYS = ["base", "early", "middle", "late"];
const TERMINAL_BREAKTHROUGH_LIFECYCLES = [
  "success",
  "failed_stable",
  "failed_damaged",
  "abandoned"
];

const VOLUME_SECTION_BOOK_TITLES = {
  1:  { base: "Scripture of Dust and Breath",    early: "Manual of the Waking Body",        middle: "Canon of Tempered Flesh",        late: "Sutra of the Earthbound Vessel"   },
  2:  { base: "Art of the First Meridian",       early: "Breath-Gathering Scripture",       middle: "Classic of Flowing Channels",    late: "Record of the Spiraling Tide"     },
  3:  { base: "Foundation Pillar Manual",        early: "Sutra of Rooted Essence",          middle: "Jade Pillar Canon",              late: "Scripture of the Unshaken Base"   },
  4:  { base: "Treatise on the Inner Crucible",  early: "Golden Core Refinement Art",       middle: "Canon of Condensed Radiance",    late: "Sutra of the Sealed Sun"          },
  5:  { base: "Awakening of the Inner Spirit",   early: "Scripture of the Infant Soul",     middle: "Mirror of Divine Consciousness", late: "Canon of the Living Spirit"       },
  6:  { base: "Manual of the Broken Fetters",    early: "Sutra of Cleaved Illusions",       middle: "Canon of the Empty Bond",        late: "Scripture of the Severed Self"    },
  7:  { base: "Art of the Hollow Expanse",       early: "Scripture of Silent Space",        middle: "Canon of the Formless Sky",      late: "Voidheart Refinement Sutra"       },
  8:  { base: "Ladder of the Rising Soul",       early: "Scripture of Heaven-Bound Spirit", middle: "Canon of Ascendant Will",        late: "Sutra of the Celestial Threshold" },
  9:  { base: "Edict of Sacred Presence",        early: "Scripture of the Saintly Flame",   middle: "Canon of Heaven's Mandate",      late: "Sutra of Crowned Divinity"        },
  10: { base: "Book of Undying Dawn",            early: "Scripture of Eternal Breath",      middle: "Canon of Boundless Heaven",      late: "Sutra of the Deathless Throne"    }
};

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
  return ["true", "1", "yes", "y", "on", "active"].includes(safeLower(value));
}

function parsePositiveInteger(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
}

function toTitle(value, fallback = "Unknown") {
  const text = safeLower(value);
  if (!text) return fallback;
  return text
    .replace(/[_-]+/g, " ")
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

function getSectionBookTitle(volumeNumber, sectionKey) {
  const titles = VOLUME_SECTION_BOOK_TITLES[safeNumber(volumeNumber, 0)];
  if (titles?.[safeLower(sectionKey)]) return titles[safeLower(sectionKey)];
  return `${toTitle(sectionKey)} Scripture`;
}

function normalizeV2StageStatus(value) {
  const text = safeLower(value);

  if (!text) return "sealed";
  if (["damaged"].includes(text)) return "damaged";
  if (["sealed", "locked"].includes(text)) return "sealed";
  if (["open", "opened", "ready"].includes(text)) return "open";
  if (["cultivating", "active", "in_progress", "under_comprehension"].includes(text)) return "cultivating";
  if (["paused"].includes(text)) return "paused";
  if (["complete", "completed", "cultivation_complete", "ready_for_breakthrough"].includes(text)) return "complete";
  if (["comprehended", "finished"].includes(text)) return "comprehended";

  return text;
}

function isLiveBreakthroughLifecycle(value) {
  return !TERMINAL_BREAKTHROUGH_LIFECYCLES.includes(safeLower(value));
}

// =========================================================
// DATABASE FETCH HELPERS
// =========================================================

async function loadMember(slAvatarKey, slUsername) {
  let query = supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      character_name,
      vestiges,
      auric_current,
      auric_maximum,
      realm_name,
      realm_display_name,
      v2_cultivation_status,
      v2_active_stage_key,
      v2_breakthrough_gate_open,
      v2_stage_needs_repair,
      v2_accumulated_seconds
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

async function loadStoreVolume(volumeNumber) {
  if (!volumeNumber) return null;

  const { data, error } = await supabase
    .schema("library")
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
      price_currency,
      price_amount,
      is_active,
      updated_at
    `)
    .eq("category", "cultivation")
    .eq("volume_number", volumeNumber)
    .eq("is_active", true);

  if (error) throw new Error(`Failed to load store volume: ${error.message}`);

  const rows = Array.isArray(data) ? data : [];
  if (rows.length === 0) return null;
  if (rows.length > 1) {
    throw new Error(
      `Ambiguous store: ${rows.length} active cultivation rows for volume ${volumeNumber}. Contact an admin.`
    );
  }

  return rows[0];
}

// =========================================================
// V2 STATE SYNC HELPERS
// DB-owned sync / promotion
// =========================================================

async function syncV2RealmCultivation(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase.schema("library").rpc(
    "v2_sync_realm_cultivation",
    { p_sl_avatar_key: slAvatarKey }
  );

  if (error) {
    throw new Error(`Failed to sync V2 realm cultivation: ${error.message}`);
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
    throw new Error(`Failed to promote countdown breakthrough: ${error.message}`);
  }

  return data || null;
}

// =========================================================
// V2 DATA LOADERS — V2 tables only, no V1
// =========================================================

async function loadV2AllStageStates(slAvatarKey, volumeNumber) {
  if (!slAvatarKey || !volumeNumber) return [];

  const { data, error } = await supabase
    .schema("library")
    .from("v2_member_stage_state")
    .select("*")
    .eq("sl_avatar_key", slAvatarKey)
    .eq("volume_number", volumeNumber);

  if (error) throw new Error(`Failed to load V2 stage states: ${error.message}`);
  return Array.isArray(data) ? data : [];
}

async function loadV2SectionCostCatalog(volumeNumber) {
  if (!volumeNumber) return {};

  const { data, error } = await supabase
    .schema("library")
    .from("v2_section_cost_catalog")
    .select("section_key,open_cp_cost,repair_cp_cost")
    .eq("volume_number", volumeNumber)
    .eq("is_active", true);

  if (error) throw new Error(`Failed to load V2 section cost catalog: ${error.message}`);

  const costs = {
    base: { open: 0, repair: 0 },
    early: { open: 0, repair: 0 },
    middle: { open: 0, repair: 0 },
    late: { open: 0, repair: 0 }
  };

  (data || []).forEach((row) => {
    const key = safeLower(row.section_key);
    if (SECTION_KEYS.includes(key)) {
      costs[key] = {
        open: safeNumber(row.open_cp_cost, 0),
        repair: safeNumber(row.repair_cp_cost, 0)
      };
    }
  });

  return costs;
}

async function loadV2TimerCatalog(volumeNumber) {
  if (!volumeNumber) return {};

  const { data, error } = await supabase
    .schema("library")
    .from("v2_cultivation_timer_catalog")
    .select("section_key,required_seconds")
    .eq("volume_number", volumeNumber)
    .eq("is_active", true);

  if (error) throw new Error(`Failed to load V2 timer catalog: ${error.message}`);

  const timers = { base: 0, early: 0, middle: 0, late: 0 };

  (data || []).forEach((row) => {
    const key = safeLower(row.section_key);
    if (SECTION_KEYS.includes(key)) {
      timers[key] = safeNumber(row.required_seconds, 0);
    }
  });

  return timers;
}

async function loadV2ActiveBreakthrough(slAvatarKey) {
  if (!slAvatarKey) return null;

  const { data, error } = await supabase
    .schema("breakthrough")
    .from("v2_member_breakthrough_state")
    .select(`
      id,
      lifecycle_status,
      countdown_started_at,
      countdown_ends_at,
      from_volume_number,
      from_section_key,
      to_volume_number,
      to_section_key,
      target_type,
      tribulation_family,
      breakthrough_started_at,
      breakthrough_ends_at,
      breakthrough_elapsed_at,
      battle_status,
      outcome,
      verdict_key,
      verdict_text,
      verdict_revealed_at,
      stage_damaged,
      cooldown_active,
      cooldown_ends_at,
      total_attempts,
      total_failures,
      consecutive_failures,
      protection_mode_active,
      created_at,
      updated_at
    `)
    .eq("sl_avatar_key", slAvatarKey)
    .not(
      "lifecycle_status",
      "in",
      `("${TERMINAL_BREAKTHROUGH_LIFECYCLES.join('","')}")`
    )
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw new Error(`Failed to load V2 active breakthrough: ${error.message}`);
  return data || null;
}

// =========================================================
// ELIGIBILITY
// =========================================================

async function checkVolumeEligibility(slAvatarKey, volumeNumber) {
  if (volumeNumber <= 1) {
    return {
      eligible: true,
      previous_volume_number: null,
      previous_volume_completed: true,
      reason: "Volume 1 is the starting realm volume."
    };
  }

  const previousVolumeNumber = volumeNumber - 1;

  const { data, error } = await supabase
    .schema("library")
    .from("v2_member_stage_state")
    .select("section_key,stage_status")
    .eq("sl_avatar_key", slAvatarKey)
    .eq("volume_number", previousVolumeNumber);

  if (error) {
    return {
      eligible: false,
      previous_volume_number: previousVolumeNumber,
      previous_volume_completed: false,
      reason: `Could not verify volume ${previousVolumeNumber} completion.`
    };
  }

  const rows = Array.isArray(data) ? data : [];
  const allComprehended = SECTION_KEYS.every((sectionKey) =>
    rows.some(
      (row) =>
        safeLower(row.section_key) === sectionKey &&
        normalizeV2StageStatus(row.stage_status) === "comprehended"
    )
  );

  return {
    eligible: allComprehended,
    previous_volume_number: previousVolumeNumber,
    previous_volume_completed: allComprehended,
    reason: allComprehended
      ? `Volume ${previousVolumeNumber} has been completed.`
      : `Complete Volume ${previousVolumeNumber} before studying this volume.`
  };
}

// =========================================================
// SECTION RECORD BUILDER
// =========================================================

function buildV2SectionRecord({
  volumeNumber,
  sectionKey,
  stageRow,
  catalogCosts,
  catalogTimers,
  breakthroughState,
  memberCpBalance,
  volumeEligible
}) {
  const normalizedStageStatus = normalizeV2StageStatus(stageRow?.stage_status);
  const requiredSeconds = stageRow
    ? safeNumber(stageRow.required_seconds, 0)
    : safeNumber(catalogTimers[sectionKey], 0);

  const accumulatedSeconds = stageRow
    ? safeNumber(stageRow.accumulated_seconds, 0)
    : 0;

  const progressPct =
    requiredSeconds > 0
      ? Math.min(100, Math.round((accumulatedSeconds / requiredSeconds) * 100))
      : 0;

  const openCpCost = safeNumber(catalogCosts[sectionKey]?.open, 0);
  const repairCpCost = stageRow
    ? safeNumber(
        stageRow.repair_cp_cost,
        safeNumber(catalogCosts[sectionKey]?.repair, 0)
      )
    : safeNumber(catalogCosts[sectionKey]?.repair, 0);

  const needsRepair = stageRow ? safeBoolean(stageRow.needs_repair) : false;

  const breakthroughApplies =
    breakthroughState !== null &&
    isLiveBreakthroughLifecycle(breakthroughState.lifecycle_status) &&
    safeNumber(breakthroughState.from_volume_number, 0) === safeNumber(volumeNumber, 0) &&
    safeLower(breakthroughState.from_section_key) === sectionKey;

  let uiState = normalizedStageStatus;
  if (needsRepair) {
    uiState = "damaged";
  } else if (breakthroughApplies && normalizedStageStatus === "complete") {
    uiState = "breakthrough_pending";
  }

  let nextAction = "none";
  let canAct = false;

  if (needsRepair) {
    nextAction = "repair_stage";
    canAct = memberCpBalance >= repairCpCost;
  } else if (uiState === "sealed") {
    if (!volumeEligible) {
      nextAction = "volume_not_eligible";
      canAct = false;
    } else {
      nextAction = "open_stage";
      canAct = memberCpBalance >= openCpCost;
    }
  } else if (uiState === "open") {
    nextAction = "begin_cultivation";
    canAct = true;
  } else if (uiState === "cultivating") {
    nextAction = "pause_cultivation";
    canAct = true;
  } else if (uiState === "paused") {
    nextAction = "resume_cultivation";
    canAct = true;
  } else if (uiState === "complete" && !breakthroughApplies) {
    nextAction = "enter_breakthrough";
    canAct = true;
  } else if (uiState === "breakthrough_pending") {
    const lifecycle = safeLower(breakthroughState?.lifecycle_status);

    if (lifecycle === "countdown" || lifecycle === "pending") {
      nextAction = "wait_for_countdown";
      canAct = false;
    } else if (lifecycle === "active") {
      nextAction = "wait_for_timer";
      canAct = false;
    } else if (lifecycle === "timer_elapsed") {
      nextAction = "resolve_battle";
      canAct = true;
    } else if (lifecycle === "battle_resolved") {
      nextAction = "reveal_verdict";
      canAct = true;
    } else if (lifecycle === "cooldown") {
      nextAction = "wait_cooldown";
      canAct = false;
    } else {
      nextAction = "view_breakthrough";
      canAct = true;
    }
  } else if (uiState === "comprehended") {
    nextAction = "none";
    canAct = false;
  }

  return {
    section_key: sectionKey,
    section_label: toTitle(sectionKey, "Base Scroll") + (sectionKey ? " Scroll" : ""),
    book_title: getSectionBookTitle(volumeNumber, sectionKey),

    stage_status: normalizedStageStatus,
    ui_state: uiState,
    needs_repair: needsRepair,

    required_seconds: requiredSeconds,
    accumulated_seconds: accumulatedSeconds,
    progress_pct: progressPct,
    human_required: formatDuration(requiredSeconds),
    human_accumulated: formatDuration(accumulatedSeconds),
    human_remaining: formatDuration(Math.max(0, requiredSeconds - accumulatedSeconds)),

    session_started_at: stageRow?.session_started_at || null,
    paused_at: stageRow?.paused_at || null,
    opened_at: stageRow?.opened_at || null,
    cultivation_completed_at: stageRow?.cultivation_completed_at || null,
    comprehended_at: stageRow?.comprehended_at || null,
    repaired_at: stageRow?.repaired_at || null,

    open_cp_cost: openCpCost,
    repair_cp_cost: repairCpCost,
    repair_resume_from_seconds:
      stageRow?.repair_resume_from_seconds ?? null,

    next_action: nextAction,
    can_act: canAct,

    stage_state_id: stageRow?.id || null,

    breakthrough_applies: breakthroughApplies,
    breakthrough_lifecycle: breakthroughApplies
      ? safeText(breakthroughState?.lifecycle_status)
      : null
  };
}

// =========================================================
// FOCUS SECTION RESOLVER
// =========================================================

function resolveFocusSection(member, volumeNumber, sectionRecords) {
  const activeStageKey = safeText(member?.v2_active_stage_key);
  if (activeStageKey) {
    const [activeVolumeRaw, activeSectionRaw] = activeStageKey.split(":");
    const activeVolume = parsePositiveInteger(activeVolumeRaw);
    const activeSection = safeLower(activeSectionRaw);

    if (
      activeVolume === volumeNumber &&
      SECTION_KEYS.includes(activeSection) &&
      sectionRecords[activeSection]
    ) {
      return activeSection;
    }
  }

  const priority = [
    "damaged",
    "breakthrough_pending",
    "cultivating",
    "paused",
    "complete",
    "open"
  ];

  for (const targetState of priority) {
    const found = SECTION_KEYS.find(
      (sectionKey) => sectionRecords[sectionKey]?.ui_state === targetState
    );
    if (found) return found;
  }

  const firstSealed = SECTION_KEYS.find(
    (sectionKey) => sectionRecords[sectionKey]?.ui_state === "sealed"
  );
  if (firstSealed) return firstSealed;

  return "late";
}

// =========================================================
// PRIMARY ACTION BUILDER
// =========================================================

function buildPrimaryAction(focusRecord, memberCpBalance, volumeNumber) {
  if (!focusRecord) {
    return {
      type: "disabled",
      label: "No Action Available",
      endpoint: null,
      section_key: null,
      note: "No valid section found.",
      cost_text: "Unavailable",
      action_params: null
    };
  }

  const { next_action, section_key, open_cp_cost, repair_cp_cost } = focusRecord;

  switch (next_action) {
    case "volume_not_eligible":
      return {
        type: "disabled",
        label: `Open ${toTitle(section_key)} Scroll`,
        endpoint: null,
        section_key,
        note: "Previous volume must be completed before this one can be opened.",
        cost_text: `${open_cp_cost} CP`,
        action_params: null
      };

    case "open_stage":
      if (memberCpBalance < open_cp_cost) {
        return {
          type: "disabled",
          label: `Open ${toTitle(section_key)} Scroll`,
          endpoint: null,
          section_key,
          note: `Requires ${open_cp_cost} CP.`,
          cost_text: `${open_cp_cost} CP`,
          action_params: null
        };
      }

      return {
        type: "api",
        label: `Open ${toTitle(section_key)} Scroll`,
        endpoint: "/.netlify/functions/unlock-library-section",
        section_key,
        note: "Spend Cultivation Points to open this stage.",
        cost_text: `${open_cp_cost} CP`,
        action_params: {
          category: "cultivation",
          volume_number: volumeNumber,
          section_name: section_key
        }
      };

    case "begin_cultivation":
      return {
        type: "api",
        label: `Begin ${toTitle(section_key)} Scroll Cultivation`,
        endpoint: "/.netlify/functions/start-section-comprehension",
        section_key,
        note: "Start the cultivation timer for this stage.",
        cost_text: "No extra cost",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "pause_cultivation":
      return {
        type: "api",
        label: "Pause Cultivation",
        endpoint: "/.netlify/functions/pause-cultivation",
        section_key,
        note: "Pause and bank current session progress.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "resume_cultivation":
      return {
        type: "api",
        label: "Resume Cultivation",
        endpoint: "/.netlify/functions/resume-cultivation",
        section_key,
        note: "Continue from where you paused.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "enter_breakthrough":
      return {
        type: "api",
        label: "Enter Breakthrough",
        endpoint: "/.netlify/functions/enter-breakthrough",
        section_key,
        note: "Cultivation complete. Enter the breakthrough chamber.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "repair_stage":
      if (memberCpBalance < repair_cp_cost) {
        return {
          type: "disabled",
          label: "Repair Blocked",
          endpoint: null,
          section_key,
          note: `Repair requires ${repair_cp_cost} CP.`,
          cost_text: `${repair_cp_cost} CP`,
          action_params: null
        };
      }

      return {
        type: "api",
        label: "Repair Stage",
        endpoint: "/.netlify/functions/repair-cultivation-book",
        section_key,
        note: "Pay repair cost to restore stage from retained progress.",
        cost_text: `${repair_cp_cost} CP`,
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "wait_for_countdown":
      return {
        type: "disabled",
        label: "Breakthrough Countdown",
        endpoint: null,
        section_key,
        note: "Heaven's countdown is underway. The database will activate the battle automatically.",
        cost_text: "Countdown",
        action_params: null
      };

    case "begin_breakthrough":
      return {
        type: "route",
        label: "Begin Breakthrough",
        endpoint: "/breakthrough.html",
        section_key,
        note: "Legacy route only. Countdown flow should already be DB-owned.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "wait_for_timer":
      return {
        type: "disabled",
        label: "Breakthrough Active",
        endpoint: null,
        section_key,
        note: "The breakthrough timer is running. Database owns the timer.",
        cost_text: "In Progress",
        action_params: null
      };

    case "wait_cooldown":
      return {
        type: "disabled",
        label: "Cooldown Active",
        endpoint: null,
        section_key,
        note: "Breakthrough is in cooldown. Wait for it to expire.",
        cost_text: "Cooldown",
        action_params: null
      };

    case "resolve_battle":
      return {
        type: "route",
        label: "Resolve Battle",
        endpoint: "/breakthrough.html",
        section_key,
        note: "Timer has elapsed. Resolve the tribulation battle.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "reveal_verdict":
      return {
        type: "route",
        label: "Receive Heaven's Verdict",
        endpoint: "/breakthrough.html",
        section_key,
        note: "The battle has been resolved. Receive your verdict.",
        cost_text: "Free",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "view_breakthrough":
      return {
        type: "route",
        label: "View Breakthrough",
        endpoint: "/breakthrough.html",
        section_key,
        note: "Return to the breakthrough chamber.",
        cost_text: "In Progress",
        action_params: {
          volume_number: volumeNumber,
          section_key
        }
      };

    case "none":
    default:
      return {
        type: "disabled",
        label: "No Action Available",
        endpoint: null,
        section_key,
        note: "There is no valid action right now.",
        cost_text: "Unavailable",
        action_params: null
      };
  }
}

// =========================================================
// UI MESSAGE
// =========================================================

function buildUiMessage(bookUiState, focusRecord, breakthroughSummary) {
  const sectionLabel = toTitle(focusRecord?.section_key, "Scroll") + " Scroll";

  switch (bookUiState) {
    case "damaged":
      return `${sectionLabel} is damaged. Repair it to resume cultivation from retained progress.`;

    case "breakthrough_pending": {
      const lifecycle = safeLower(breakthroughSummary?.lifecycle_status || "countdown");

      if (lifecycle === "countdown" || lifecycle === "pending") {
        return `Breakthrough countdown is underway for ${sectionLabel}.`;
      }

      if (lifecycle === "active") {
        return `Breakthrough is in progress for ${sectionLabel}.`;
      }

      if (lifecycle === "timer_elapsed") {
        return `The breakthrough timer has elapsed. Resolve the battle.`;
      }

      if (lifecycle === "battle_resolved") {
        return `The battle has been resolved. Receive Heaven's Verdict.`;
      }

      if (lifecycle === "cooldown") {
        return `Breakthrough is in cooldown. Wait for it to expire.`;
      }

      return `${sectionLabel} is ready for breakthrough. Enter the chamber to continue.`;
    }

    case "cultivating":
      return `Cultivation is active on ${sectionLabel}.`;

    case "paused":
      return `Cultivation is paused on ${sectionLabel}. Resume when ready.`;

    case "complete":
      return `${sectionLabel} cultivation is complete. Enter breakthrough to advance.`;

    case "open":
      return `${sectionLabel} is open and ready to begin cultivation.`;

    case "sealed":
      return focusRecord?.next_action === "volume_not_eligible"
        ? `${sectionLabel} is sealed. Complete the previous volume before opening this one.`
        : `${sectionLabel} is sealed. Open it to begin comprehension.`;

    case "comprehended":
      return "All sections of this volume have been comprehended.";

    default:
      return "The cultivation record is ready.";
  }
}

// =========================================================
// FALLBACK / UNAVAILABLE
// =========================================================

function buildUnavailable(message, volumeNumber = null) {
  return {
    available: false,
    message,
    selected_volume_number: volumeNumber,
    store_volume: null,
    access: {
      eligible: false,
      previous_volume_number: null,
      previous_volume_completed: false,
      reason: message
    },
    sections: null,
    section_summary: null,
    focus_section: null,
    focus_section_label: null,
    focus_section_record: null,
    active_breakthrough: null,
    ui: {
      book_ui_state: "sealed",
      focus_section: null,
      focus_section_label: null,
      message,
      primary_action: {
        type: "disabled",
        label: "No Action Available",
        endpoint: null,
        section_key: null,
        note: message,
        cost_text: "Unavailable",
        action_params: null
      }
    }
  };
}

// =========================================================
// MAIN BUILD FUNCTION
// =========================================================

async function buildV2CultivationBookState({ member, requestedVolumeNumber }) {
  const slAvatarKey = safeText(member?.sl_avatar_key);

  if (!slAvatarKey) {
    return buildUnavailable(
      "V2 cultivation state requires sl_avatar_key. This member record has no avatar key populated.",
      null
    );
  }

  const volumeNumber =
    parsePositiveInteger(requestedVolumeNumber) ||
    parsePositiveInteger(member?.v2_active_stage_key?.split(":")?.[0]) ||
    null;

  const storeVolume = volumeNumber ? await loadStoreVolume(volumeNumber) : null;

  if (!volumeNumber || !storeVolume) {
    return buildUnavailable("No cultivation volume found.", volumeNumber);
  }

  const eligibility = await checkVolumeEligibility(slAvatarKey, volumeNumber);
  const access = {
    eligible: eligibility.eligible,
    previous_volume_number: eligibility.previous_volume_number,
    previous_volume_completed: eligibility.previous_volume_completed,
    reason: eligibility.reason
  };

  const [stageRows, catalogCosts, catalogTimers, breakthroughState] = await Promise.all([
    loadV2AllStageStates(slAvatarKey, volumeNumber),
    loadV2SectionCostCatalog(volumeNumber),
    loadV2TimerCatalog(volumeNumber),
    loadV2ActiveBreakthrough(slAvatarKey)
  ]);

  const stageBySection = {};
  stageRows.forEach((row) => {
    const key = safeLower(row.section_key);
    if (SECTION_KEYS.includes(key)) {
      stageBySection[key] = row;
    }
  });

  const memberCpBalance = safeNumber(member?.vestiges, 0);

  const sectionRecords = {};
  for (const sectionKey of SECTION_KEYS) {
    sectionRecords[sectionKey] = buildV2SectionRecord({
      volumeNumber,
      sectionKey,
      stageRow: stageBySection[sectionKey] || null,
      catalogCosts,
      catalogTimers,
      breakthroughState,
      memberCpBalance,
      volumeEligible: eligibility.eligible
    });
  }

  const focusSectionKey = resolveFocusSection(member, volumeNumber, sectionRecords);
  const focusRecord = sectionRecords[focusSectionKey] || null;
  const bookUiState = safeText(focusRecord?.ui_state, "sealed");
  const primaryAction = buildPrimaryAction(focusRecord, memberCpBalance, volumeNumber);

  let breakthroughSummary = null;
  if (breakthroughState) {
    breakthroughSummary = {
      breakthrough_state_id: breakthroughState.id,
      lifecycle_status: safeText(breakthroughState.lifecycle_status),
      countdown_started_at: breakthroughState.countdown_started_at || null,
      countdown_ends_at: breakthroughState.countdown_ends_at || null,
      from_volume_number: breakthroughState.from_volume_number,
      from_section_key: breakthroughState.from_section_key,
      to_volume_number: breakthroughState.to_volume_number,
      to_section_key: breakthroughState.to_section_key,
      target_type: breakthroughState.target_type,
      tribulation_family: breakthroughState.tribulation_family,
      timer_started: safeBoolean(breakthroughState.breakthrough_started_at),
      breakthrough_started_at: breakthroughState.breakthrough_started_at || null,
      breakthrough_ends_at: breakthroughState.breakthrough_ends_at || null,
      breakthrough_elapsed_at: breakthroughState.breakthrough_elapsed_at || null,
      battle_status: safeText(breakthroughState.battle_status, "not_started"),
      outcome: safeText(breakthroughState.outcome) || null,
      verdict_key: safeText(breakthroughState.verdict_key) || null,
      verdict_text: safeText(breakthroughState.verdict_text) || null,
      verdict_revealed_at: breakthroughState.verdict_revealed_at || null,
      stage_damaged: safeBoolean(breakthroughState.stage_damaged),
      cooldown_active: safeBoolean(breakthroughState.cooldown_active),
      cooldown_ends_at: breakthroughState.cooldown_ends_at || null,
      total_attempts: safeNumber(breakthroughState.total_attempts, 0),
      total_failures: safeNumber(breakthroughState.total_failures, 0),
      consecutive_failures: safeNumber(breakthroughState.consecutive_failures, 0),
      protection_mode_active: safeBoolean(breakthroughState.protection_mode_active)
    };
  }

  const uiMessage = buildUiMessage(bookUiState, focusRecord, breakthroughSummary);

  const sectionSummary = {
    sealed_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "sealed").length,
    open_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "open").length,
    cultivating_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "cultivating").length,
    paused_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "paused").length,
    damaged_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "damaged").length,
    complete_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "complete").length,
    breakthrough_pending_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "breakthrough_pending").length,
    comprehended_count: SECTION_KEYS.filter((k) => sectionRecords[k]?.ui_state === "comprehended").length,
    total_sections: SECTION_KEYS.length
  };

  return {
    available: true,
    selected_volume_number: volumeNumber,
    requested_volume_number: parsePositiveInteger(requestedVolumeNumber) || null,

    store_volume: {
      id: storeVolume.id,
      item_key: storeVolume.item_key,
      realm_name: storeVolume.realm_name,
      volume_number: storeVolume.volume_number,
      item_name: storeVolume.item_name,
      description: storeVolume.description,
      price_currency: storeVolume.price_currency || null,
      price_amount: safeNumber(storeVolume.price_amount, 0),
      is_active: safeBoolean(storeVolume.is_active)
    },

    access,
    sections: sectionRecords,
    section_summary: sectionSummary,

    focus_section: focusSectionKey,
    focus_section_label: toTitle(focusSectionKey, "Base Scroll") + (focusSectionKey ? " Scroll" : ""),
    focus_section_record: focusRecord,

    active_breakthrough: breakthroughSummary,

    v2_cultivation_status: safeText(member?.v2_cultivation_status, "idle"),
    v2_active_stage_key: safeText(member?.v2_active_stage_key) || null,
    v2_breakthrough_gate_open: safeBoolean(member?.v2_breakthrough_gate_open),
    v2_stage_needs_repair: safeBoolean(member?.v2_stage_needs_repair),
    v2_accumulated_seconds: safeNumber(member?.v2_accumulated_seconds, 0),

    ui: {
      book_ui_state: bookUiState,
      focus_section: focusSectionKey,
      focus_section_label: toTitle(focusSectionKey, "Base Scroll") + (focusSectionKey ? " Scroll" : ""),
      message: uiMessage,
      primary_action: primaryAction
    }
  };
}

// =========================================================
// MAIN HANDLER
// =========================================================

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

    const slAvatarKey = safeText(query.sl_avatar_key || body.sl_avatar_key);
    const slUsername = safeLower(query.sl_username || body.sl_username);
    const requestedVolumeNumber =
      parsePositiveInteger(query.volume_number || body.volume_number) ||
      parsePositiveInteger(query.volume || body.volume) ||
      null;

    if (!slAvatarKey && !slUsername) {
      return buildResponse(400, {
        success: false,
        message: "sl_avatar_key or sl_username is required."
      });
    }

    const warnings = [];

    let member = await loadMember(slAvatarKey, slUsername);
    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    try {
      await syncV2RealmCultivation(member.sl_avatar_key);
    } catch (syncError) {
      console.error("load-cultivation-book-state V2 sync error:", syncError);
      warnings.push("Could not sync active Realm Cultivation state.");
    }

    try {
      await promoteCountdownToActive(member.sl_avatar_key);
    } catch (promoError) {
      console.error("load-cultivation-book-state countdown promotion error:", promoError);
      warnings.push("Could not promote breakthrough countdown state.");
    }

    try {
      const refreshedMember = await loadMember(member.sl_avatar_key, null);
      if (refreshedMember) {
        member = refreshedMember;
      }
    } catch (reloadError) {
      console.error("load-cultivation-book-state member reload error:", reloadError);
      warnings.push("Could not reload refreshed member state after sync.");
    }

    let cultivationRecord;

    try {
      cultivationRecord = await buildV2CultivationBookState({
        member,
        requestedVolumeNumber
      });
    } catch (buildError) {
      console.error("load-cultivation-book-state V2 build error:", buildError);
      warnings.push("Cultivation record could not be fully loaded.");
      cultivationRecord = buildUnavailable(
        buildError.message || "Cultivation record could not be loaded.",
        requestedVolumeNumber || null
      );
    }

    return buildResponse(200, {
      success: true,
      message: "Cultivation book state loaded successfully.",
      user: {
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username),
        display_name: safeText(member.display_name) || null,
        character_name: safeText(member.character_name) || null,
        vestiges: safeNumber(member.vestiges, 0),
        auric_current: safeNumber(member.auric_current, 0),
        auric_maximum: safeNumber(member.auric_maximum, 0),
        realm_name: safeText(member.realm_name) || null,
        realm_stage_key: member.v2_active_stage_key ? (member.v2_active_stage_key.split(":")[1] || null) : null,
        realm_display_name: safeText(member.realm_display_name) || null,
        v2_cultivation_status: safeText(member.v2_cultivation_status, "idle"),
        v2_active_stage_key: safeText(member.v2_active_stage_key) || null,
        v2_breakthrough_gate_open: safeBoolean(member.v2_breakthrough_gate_open),
        v2_stage_needs_repair: safeBoolean(member.v2_stage_needs_repair),
        v2_accumulated_seconds: safeNumber(member.v2_accumulated_seconds, 0),
        gender: safeText(member.gender, "male")
      },
      cultivation_record: cultivationRecord,
      warnings
    });
  } catch (error) {
    console.error("load-cultivation-book-state error:", error);
    return buildResponse(500, {
      success: false,
      message: "Failed to load cultivation book state.",
      error: error.message || "Unknown error."
    });
  }
}

module.exports = { handler };