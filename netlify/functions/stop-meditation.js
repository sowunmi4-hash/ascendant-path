// stop-meditation.js
// Pauses personal cultivation using v2 system.
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).

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
    try { cookies[key] = decodeURIComponent(val); }
    catch { cookies[key] = val; }
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
  if (!avatarKey) {
    return json(401, { error: "Not authenticated" });
  }

  // First check current status — if in pure 'meditating' state (no active scroll session),
  // v2_pause_cultivation would return not_cultivating. Handle this directly.
  const { data: member } = await supabase
    .from("cultivation_members")
    .select("v2_cultivation_status")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (member?.v2_cultivation_status === "meditating" || member?.v2_cultivation_status === "breakthrough_ready") {
    // No active scroll session — set to paused so HUD animation stops
    // Gate flag stays untouched — breakthrough remains available
    const newStatus = "paused";
    const { error: updateError } = await supabase
      .from("cultivation_members")
      .update({ v2_cultivation_status: newStatus })
      .eq("sl_avatar_key", avatarKey);

    if (updateError) {
      console.error("Failed to stop meditation:", updateError);
      return json(500, { error: "Failed to stop meditation", detail: updateError.message });
    }

    return json(200, {
      success: true,
      action: "meditation_stopped",
      message: "Meditation ended. Auric and vestige gains stopped.",
      v2_cultivation_status: newStatus
    });
  }

  // Active scroll session (status = 'cultivating' or 'breakthrough_ready') —
  // call v2_pause_cultivation to settle drift_debt and close the stage session.
  const { data: result, error: rpcError } = await supabase
    .schema("library")
    .rpc("v2_pause_cultivation", { p_sl_avatar_key: avatarKey });

  if (rpcError) {
    console.error("v2_pause_cultivation error:", rpcError);
    return json(500, { error: "Failed to stop cultivation", detail: rpcError.message });
  }

  if (!result?.success) {
    if (result?.error_code === "not_cultivating") {
      return json(200, {
        success: true,
        action: "already_stopped",
        message: "No active cultivation session found."
      });
    }
    return json(409, {
      error: result?.message || "Cannot stop cultivation",
      error_code: result?.error_code || "unknown"
    });
  }

  return json(200, { success: true, action: "stopped", ...result });
};
