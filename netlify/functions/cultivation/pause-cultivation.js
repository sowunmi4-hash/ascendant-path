// cultivation/pause-cultivation.js
// Pauses active scroll cultivation — Phase 2 rewrite.
// Called by the "Pause" button on the cultivation website.
//
// Manual mode:
//   - Calls v2_pause_cultivation to settle drift_debt and close the stage session.
//   - Then sets v2_cultivation_status = 'meditating' so HUD stays active.
//   - If already in 'meditating' (no active scroll): returns success with no change.
//
// Auto mode:
//   - Calls v2_pause_cultivation — sets status = 'paused', stops everything.
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

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const avatarKey = await resolveAvatarKey(event, body);
  if (!avatarKey) return json(401, { error: "Not authenticated" });

  const { data: member } = await supabase
    .from("cultivation_members")
    .select("personal_cultivation_preference, cultivation_mode, v2_cultivation_status")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  const preference  = (member?.personal_cultivation_preference || member?.cultivation_mode || "manual").toLowerCase();
  const currentStatus = member?.v2_cultivation_status || "idle";

  // Already in pure meditation — scroll is already paused, just confirm
  if (currentStatus === "meditating") {
    return json(200, {
      success: true,
      action: "already_paused",
      message: "No active scroll session. Meditation continues.",
      cultivation_preference: preference,
      v2_cultivation_status: "meditating"
    });
  }

  // Nothing to pause
  if (["paused", "idle"].includes(currentStatus)) {
    return json(200, {
      success: true,
      action: "already_paused",
      message: "Nothing active to pause.",
      cultivation_preference: preference,
      v2_cultivation_status: currentStatus
    });
  }

  // Settle drift_debt and close the active stage session
  const { data: pauseResult, error: pauseError } = await supabase
    .schema("library")
    .rpc("v2_pause_cultivation", { p_sl_avatar_key: avatarKey });

  if (pauseError) {
    console.error("v2_pause_cultivation error:", pauseError);
    return json(500, { error: "Failed to pause cultivation", detail: pauseError.message });
  }

  if (!pauseResult?.success) {
    if (pauseResult?.error_code === "not_cultivating") {
      // No active session — treat as success
      const fallbackStatus = preference === "manual" ? "meditating" : "paused";
      if (preference === "manual") {
        await supabase
          .from("cultivation_members")
          .update({ v2_cultivation_status: "meditating" })
          .eq("sl_avatar_key", avatarKey);
      }
      return json(200, {
        success: true,
        action: "already_paused",
        message: "No active cultivation session found.",
        cultivation_preference: preference,
        v2_cultivation_status: fallbackStatus
      });
    }
    return json(409, {
      error: pauseResult?.message || "Cannot pause cultivation",
      error_code: pauseResult?.error_code || "unknown"
    });
  }

  // Manual mode: restore to 'meditating' so HUD still shows active
  if (preference === "manual") {
    const { error: updateError } = await supabase
      .from("cultivation_members")
      .update({
        v2_cultivation_status: "meditating",
        last_hud_sync_at: new Date().toISOString()  // reset anchor for fresh meditation fills
      })
      .eq("sl_avatar_key", avatarKey);

    if (updateError) {
      console.error("Failed to restore meditating status:", updateError);
      return json(500, { error: "Failed to restore meditation state", detail: updateError.message });
    }

    return json(200, {
      success: true,
      action: "cultivation_paused_meditation_continues",
      message: "Scroll paused. Meditation continues — auric and vestiges filling.",
      cultivation_preference: "manual",
      v2_cultivation_status: "meditating",
      auric_before:           pauseResult.auric_before,
      auric_after:            pauseResult.auric_after,
      drift_debt_deducted:    pauseResult.drift_debt_deducted,
      drift_debt_remainder:   pauseResult.drift_debt_remainder,
      accumulated_seconds:    pauseResult.accumulated_seconds,
      required_seconds:       pauseResult.required_seconds,
      breakthrough_gate_open: pauseResult.breakthrough_gate_open
    });
  }

  // Auto mode: v2_pause_cultivation already set status = 'paused'
  return json(200, {
    success: true,
    action: "paused",
    cultivation_preference: "auto",
    ...pauseResult
  });
};
