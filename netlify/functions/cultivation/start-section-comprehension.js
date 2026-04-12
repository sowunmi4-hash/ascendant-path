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

// =========================================================
// WHAT THIS FILE DOES (V2)
//
// Calls library.v2_begin_cultivation(p_sl_avatar_key).
//
// The DB function owns all logic:
//   - validates member exists
//   - finds the active open/paused stage
//   - guards against repair-required
//   - starts the session timer
//   - writes session_started_at to v2_member_stage_state
//   - updates member v2_cultivation_status = 'cultivating'
//
// This file does none of that itself.
// =========================================================

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

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
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

// =========================================================
// MEMBER LOADER
// =========================================================

async function loadMember(slAvatarKey, slUsername) {
  let query = publicSupabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      vestiges,
      v2_cultivation_status,
      v2_active_stage_key,
      personal_cultivation_preference
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

// =========================================================
// V2 ACTION CALLER
// Calls library.v2_begin_cultivation(p_sl_avatar_key).
// The DB function owns all timer and state logic.
// =========================================================

async function callV2BeginCultivation(slAvatarKey) {
  const { data, error } = await librarySupabase.rpc("v2_begin_cultivation", {
    p_sl_avatar_key: slAvatarKey
  });

  if (error) {
    throw new Error(`v2_begin_cultivation RPC failed: ${error.message}`);
  }

  const result = Array.isArray(data) ? data[0] : data;
  return result || null;
}

// =========================================================
// MAIN HANDLER
// =========================================================

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
    const slAvatarKey = safeText(body.sl_avatar_key);
    const slUsername = safeText(body.sl_username);

    if (!slAvatarKey && !slUsername) {
      return buildResponse(400, {
        success: false,
        message: "sl_avatar_key or sl_username is required."
      });
    }

    // -------------------------------------------------------
    // LOAD MEMBER
    // -------------------------------------------------------
    const member = await loadMember(slAvatarKey, slUsername);

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    // -------------------------------------------------------
    // AVATAR KEY GUARD
    // v2_begin_cultivation uses sl_avatar_key as identifier.
    // -------------------------------------------------------
    if (!member.sl_avatar_key) {
      return buildResponse(400, {
        success: false,
        error_code: "avatar_key_required",
        message: "Cultivation V2 requires sl_avatar_key. This member record has no avatar key populated."
      });
    }

    // -------------------------------------------------------
    // CALL V2 BEGIN CULTIVATION
    // All validation, timer start, and state writes happen
    // inside the DB function. This file is a thin caller.
    // -------------------------------------------------------
    let result;

    try {
      result = await callV2BeginCultivation(member.sl_avatar_key);
    } catch (rpcError) {
      console.error("start-section-comprehension v2_begin_cultivation error:", rpcError);
      return buildResponse(500, {
        success: false,
        message: "Failed to begin cultivation.",
        error: rpcError.message
      });
    }

    if (!result) {
      return buildResponse(500, {
        success: false,
        message: "v2_begin_cultivation returned no result."
      });
    }

    // -------------------------------------------------------
    // MAP DB ERROR CODES TO HTTP STATUS CODES
    // -------------------------------------------------------
    if (!result.success) {
      const errorCode = safeText(result.error_code);
      const message = safeText(result.message, "Begin cultivation failed.");

      const statusMap = {
        member_not_found: 404,
        no_open_stage: 409,
        already_cultivating: 409,
        stage_needs_repair: 409
      };

      return buildResponse(statusMap[errorCode] || 500, {
        success: false,
        error_code: errorCode,
        message,
        ...(result.v2_active_stage_key && {
          v2_active_stage_key: result.v2_active_stage_key
        }),
        ...(result.repair_cp_cost !== undefined && {
          repair_cp_cost: result.repair_cp_cost
        }),
        ...(result.stage_state_id && {
          stage_state_id: result.stage_state_id
        }),
        ...(result.volume_number !== undefined && {
          volume_number: safeNumber(result.volume_number, 0)
        }),
        ...(result.section_key && {
          section_key: safeText(result.section_key)
        })
      });
    }

    // -------------------------------------------------------
    // SUCCESS — prefer DB-owned values returned by the RPC
    // -------------------------------------------------------
    const requiredSeconds = safeNumber(result.required_seconds, 0);
    const accumulatedSeconds = safeNumber(result.accumulated_seconds, 0);
    const volumeNumber = safeNumber(result.volume_number, 0);
    const sectionKey = safeText(result.section_key) || null;

    const dbCultivationStatus = safeText(
      result.v2_cultivation_status,
      safeText(member.v2_cultivation_status, "cultivating")
    );

    const dbActiveStageKey = safeText(
      result.v2_active_stage_key,
      volumeNumber > 0 && sectionKey ? `${volumeNumber}:${sectionKey}` : safeText(member.v2_active_stage_key)
    ) || null;

    return buildResponse(200, {
      success: true,
      message: safeText(
        result.message,
        sectionKey && volumeNumber > 0
          ? `Cultivation started for ${sectionKey} (Volume ${volumeNumber}).`
          : "Cultivation started successfully."
      ),

      // DB result
      stage_state_id: safeText(result.stage_state_id) || null,
      volume_number: volumeNumber,
      section_key: sectionKey,
      stage_status: (safeText(result.stage_status) === 'complete' && result.session_started_at) ? 'cultivating' : safeText(result.stage_status, "cultivating"),

      // Timer fields — raw from DB, frontend renders, never calculates
      session_started_at: safeText(result.session_started_at) || null,
      accumulated_seconds: accumulatedSeconds,
      required_seconds: requiredSeconds,
      human_accumulated: formatDuration(accumulatedSeconds),
      human_required: formatDuration(requiredSeconds),
      human_remaining: formatDuration(Math.max(0, requiredSeconds - accumulatedSeconds)),

      // Updated member V2 status — prefer DB-owned values
      v2_cultivation_status: (safeText(result.stage_status) === 'complete' && result.session_started_at) ? 'cultivating' : dbCultivationStatus,
      v2_active_stage_key: dbActiveStageKey,

      // Member context
      member: {
        member_id: safeText(member.member_id) || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username),
        vestiges: safeNumber(member.vestiges, 0)
      },
      personal_cultivation_preference: safeText(member.personal_cultivation_preference, "manual")
    });
  } catch (error) {
    console.error("start-section-comprehension error:", error);
    return buildResponse(500, {
      success: false,
      message: "Failed to start section comprehension.",
      error: error.message || "Unknown error."
    });
  }
};