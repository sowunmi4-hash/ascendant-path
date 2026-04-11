const { createClient } = require("@supabase/supabase-js");

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "breakthrough" } }
);

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
  try { return event.body ? JSON.parse(event.body) : {}; } catch (e) { return {}; }
}

function safeText(value, fallback) {
  if (fallback === undefined) fallback = "";
  const text = String(value !== null && value !== undefined ? value : "").trim();
  return text || fallback;
}

function safeNumber(value, fallback) {
  if (fallback === undefined) fallback = 0;
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  return ["true", "1", "yes", "y", "on"].includes(String(value !== null && value !== undefined ? value : "").trim().toLowerCase());
}

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  cookieHeader.split(";").forEach(function(part) {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const k = trimmed.slice(0, eq).trim();
    const v = trimmed.slice(eq + 1).trim();
    try { cookies[k] = decodeURIComponent(v); } catch(e) { cookies[k] = v; }
  });
  return cookies;
}

async function loadMember(slAvatarKey, slUsername) {
  let query = publicSupabase
    .from("cultivation_members")
    .select("member_id,sl_avatar_key,sl_username,display_name")
    .limit(1);
  if (slAvatarKey) { query = query.eq("sl_avatar_key", slAvatarKey); }
  else if (slUsername) { query = query.eq("sl_username", slUsername); }
  const { data, error } = await query.maybeSingle();
  if (error) throw new Error("Failed to load cultivation member: " + error.message);
  return data || null;
}

async function callV2BeginBreakthrough(slAvatarKey) {
  const { data, error } = await breakthroughSupabase.rpc(
    "v2_begin_breakthrough",
    { p_sl_avatar_key: slAvatarKey }
  );
  if (error) throw new Error("v2_begin_breakthrough RPC failed: " + error.message);
  const result = Array.isArray(data) ? data[0] : data;
  return result || null;
}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return buildResponse(200, { ok: true });
  if (event.httpMethod !== "POST") return buildResponse(405, { success: false, message: "Method not allowed. Use POST." });

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, { success: false, message: "Missing Supabase environment variables." });
    }

    // Read ap_session cookie for auth
    const cookieHeader = (event.headers && event.headers.cookie)
      ? event.headers.cookie
      : ((event.headers && event.headers.Cookie) ? event.headers.Cookie : "");
    const cookies = parseCookies(cookieHeader);
    const sessionToken = cookies.ap_session || "";
    let sessionAvatarKey = "";

    if (sessionToken) {
      const sessionResult = await publicSupabase
        .from("website_sessions")
        .select("sl_avatar_key")
        .eq("session_token", sessionToken)
        .eq("is_active", true)
        .maybeSingle();
      sessionAvatarKey = (sessionResult.data && sessionResult.data.sl_avatar_key)
        ? sessionResult.data.sl_avatar_key : "";
    }

    const body = parseBody(event);
    const slAvatarKey = sessionAvatarKey || safeText(body.sl_avatar_key);
    const slUsername = slAvatarKey ? "" : safeText(body.sl_username);

    if (!slAvatarKey && !slUsername) {
      return buildResponse(400, { success: false, message: "sl_avatar_key or sl_username is required." });
    }

    const member = await loadMember(slAvatarKey, slUsername);
    if (!member) return buildResponse(404, { success: false, message: "Cultivation member not found." });

    if (!member.sl_avatar_key) {
      return buildResponse(400, {
        success: false,
        error_code: "avatar_key_required",
        message: "Breakthrough V2 requires sl_avatar_key. This member record has no avatar key populated."
      });
    }

    let result;
    try {
      result = await callV2BeginBreakthrough(member.sl_avatar_key);
    } catch (rpcError) {
      console.error("begin-breakthrough v2_begin_breakthrough error:", rpcError);
      return buildResponse(500, { success: false, message: "Failed to begin breakthrough.", error: rpcError.message });
    }

    if (!result) return buildResponse(500, { success: false, message: "v2_begin_breakthrough returned no result." });

    if (!result.success) {
      const errorCode = safeText(result.error_code);
      const message = safeText(result.message, "Begin breakthrough failed.");
      const statusMap = {
        member_not_found: 404, avatar_key_required: 400, no_pending_breakthrough: 409,
        breakthrough_not_entered: 409, invalid_lifecycle_status: 409,
        breakthrough_already_active: 409, cooldown_active: 409, stage_needs_repair: 409
      };
      const responseBody = { success: false, error_code: errorCode, message: message };
      if (result.breakthrough_state_id) responseBody.breakthrough_state_id = result.breakthrough_state_id;
      if (result.stage_state_id) responseBody.stage_state_id = result.stage_state_id;
      if (result.current_lifecycle_status) responseBody.current_lifecycle_status = result.current_lifecycle_status;
      if (result.cooldown_ends_at) responseBody.cooldown_ends_at = result.cooldown_ends_at;
      if (result.from_volume_number !== undefined) responseBody.from_volume_number = result.from_volume_number;
      if (result.from_section_key) responseBody.from_section_key = result.from_section_key;
      return buildResponse(statusMap[errorCode] || 500, responseBody);
    }

    return buildResponse(200, {
      success: true,
      message: safeText(result.message) || "Breakthrough begun successfully. The timer is now active.",
      stage_state_id: safeText(result.stage_state_id) || null,
      breakthrough_state_id: safeText(result.breakthrough_state_id) || null,
      lifecycle_status: safeText(result.lifecycle_status, "active"),
      next_action: safeText(result.next_action) || "wait_for_timer",
      from_volume_number: safeNumber(result.from_volume_number, 0) || null,
      from_section_key: safeText(result.from_section_key) || null,
      to_volume_number: safeNumber(result.to_volume_number, 0) || null,
      to_section_key: safeText(result.to_section_key) || null,
      target_type: safeText(result.target_type) || null,
      tribulation_family: safeText(result.tribulation_family) || null,
      battle_status: safeText(result.battle_status, "not_started"),
      breakthrough_started_at: result.breakthrough_started_at || null,
      breakthrough_ends_at: result.breakthrough_ends_at || null,
      breakthrough_elapsed_at: result.breakthrough_elapsed_at || null,
      verdict_revealed_at: result.verdict_revealed_at || null,
      breakthrough_duration_seconds: result.breakthrough_duration_seconds !== undefined ? safeNumber(result.breakthrough_duration_seconds, 0) : null,
      stage_damaged: safeBoolean(result.stage_damaged),
      cooldown_active: safeBoolean(result.cooldown_active),
      cooldown_ends_at: result.cooldown_ends_at || null,
      total_attempts: safeNumber(result.total_attempts, 0),
      total_failures: safeNumber(result.total_failures, 0),
      consecutive_failures: safeNumber(result.consecutive_failures, 0),
      protection_mode_active: safeBoolean(result.protection_mode_active),
      member: {
        member_id: safeText(member.member_id) || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username: safeText(member.sl_username)
      }
    });

  } catch (error) {
    console.error("begin-breakthrough error:", error);
    return buildResponse(500, { success: false, message: "Failed to begin breakthrough.", error: error.message || "Unknown error." });
  }
};