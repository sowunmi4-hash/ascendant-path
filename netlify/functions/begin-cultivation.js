// cultivation/begin.js
// Starts scroll cultivation explicitly.
// Called by:
//   - Manual mode: "Resume Cultivation" button on website
//   - Auto mode: triggered automatically when meditation starts (via start-meditation)
//
// Flow:
//   1. Validates member is in a startable state (meditating / paused / idle)
//   2. Calls v2_begin_cultivation → v2_resume_cultivation fallback
//   3. Sets v2_cultivation_status = 'cultivating', clears last_hud_sync_at anchor
//
// Dual auth: ap_session cookie OR sl_avatar_key in body.

const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

function parseCookies(header) {
  const cookies = {};
  if (!header) return cookies;
  header.split(";").forEach((part) => {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    try { cookies[key] = decodeURIComponent(val); } catch { cookies[key] = val; }
  });
  return cookies;
}

function json(status, body) {
  return {
    statusCode: status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    body: JSON.stringify(body)
  };
}

async function resolveAvatarKey(event, body) {
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  const sessionToken = cookies[COOKIE_NAME] || "";

  if (sessionToken) {
    const { data: sessionRow } = await supabase
      .from("website_sessions")
      .select("sl_avatar_key")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();
    if (sessionRow?.sl_avatar_key) return sessionRow.sl_avatar_key;
  }

  const avatarKey = (body.sl_avatar_key || "").trim();
  if (avatarKey) {
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("sl_avatar_key")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();
    if (member?.sl_avatar_key) return member.sl_avatar_key;
  }

  return null;
}

// Try v2_begin_cultivation first, fall back to v2_resume_cultivation
async function beginScrollCultivation(avatarKey) {
  const { data: beginResult, error: beginError } = await supabase
    .schema("library")
    .rpc("v2_begin_cultivation", { p_sl_avatar_key: avatarKey });

  if (beginError) {
    return { success: false, action: null, result: null, error: beginError.message };
  }

  if (beginResult?.success) {
    return { success: true, action: "started", result: beginResult, error: null };
  }

  if (beginResult?.error_code === "no_open_stage") {
    const { data: resumeResult, error: resumeError } = await supabase
      .schema("library")
      .rpc("v2_resume_cultivation", { p_sl_avatar_key: avatarKey });

    if (resumeError) {
      return { success: false, action: null, result: null, error: resumeError.message };
    }

    if (resumeResult?.success) {
      return { success: true, action: "resumed", result: resumeResult, error: null };
    }

    return {
      success: false,
      action: null,
      result: resumeResult,
      error: resumeResult?.message || "Cannot resume cultivation"
    };
  }

  if (beginResult?.error_code === "already_active") {
    return { success: true, action: "already_active", result: beginResult, error: null };
  }

  return {
    success: false,
    action: null,
    result: beginResult,
    error: beginResult?.message || "Cannot start cultivation"
  };
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const avatarKey = await resolveAvatarKey(event, body);
  if (!avatarKey) return json(401, { error: "Not authenticated" });

  // Load member state
  const { data: member } = await supabase
    .from("cultivation_members")
    .select("v2_cultivation_status, personal_cultivation_preference, cultivation_mode, auric_current")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (!member) return json(404, { error: "Member not found" });

  const status     = member.v2_cultivation_status || "idle";
  const preference = (member.personal_cultivation_preference || member.cultivation_mode || "manual").toLowerCase();

  // Already cultivating or at breakthrough — return current state
  if (status === "cultivating") {
    return json(200, {
      success: true,
      action: "already_cultivating",
      message: "Scroll cultivation already active.",
      v2_cultivation_status: status,
      cultivation_preference: preference
    });
  }

  if (status === "in_breakthrough") {
    return json(200, {
      success: true,
      action: "blocked_by_breakthrough",
      message: "A breakthrough is already underway. Complete it first.",
      v2_cultivation_status: status,
      cultivation_preference: preference
    });
  }
  // breakthrough_ready: allow meditation so cultivator can channel Auric before entering

  // In manual mode, only meditating/paused/idle are valid starting states
  // In auto mode this endpoint is also called when meditation starts
  if (!["meditating", "paused", "idle"].includes(status)) {
    return json(409, {
      error: "Cannot begin cultivation from current state",
      error_code: "invalid_state",
      v2_cultivation_status: status
    });
  }

  // Begin scroll cultivation
  const started = await beginScrollCultivation(avatarKey);

  if (!started.success) {
    const errCode = started.result?.error_code || "unknown";
    console.error("beginScrollCultivation error:", started.error);
    return json(409, {
      error: started.error,
      error_code: errCode,
      cultivation_preference: preference
    });
  }

  // Clear last_hud_sync_at so the meditation fill anchor resets on next sync
  // (We are now cultivating — auric fills no longer apply via meditation path)
  await supabase
    .from("cultivation_members")
    .update({ last_hud_sync_at: null })
    .eq("sl_avatar_key", avatarKey);

  return json(200, {
    success: true,
    action: started.action,
    message: "Scroll cultivation started. Auric drains via drift debt. Use Pause to stop.",
    cultivation_preference: preference,
    v2_cultivation_status: "cultivating",
    ...started.result
  });
};
