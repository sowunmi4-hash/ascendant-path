// start-meditation.js
// Starts personal cultivation using v2 system.
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
  // Try cookie session first (website callers)
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

  // Fall back to body sl_avatar_key (HUD/LSL callers)
  const avatarKey = (body.sl_avatar_key || "").trim();
  if (avatarKey) {
    // Verify the avatar key exists in cultivation_members
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

  // Load member to check personal cultivation preference
  const { data: member } = await supabase
    .from('cultivation_members')
    .select('personal_cultivation_preference')
    .eq('sl_avatar_key', avatarKey)
    .maybeSingle();

  const preference = (member?.personal_cultivation_preference || 'manual').toLowerCase();

  // In manual mode: meditation fills auric but doesn't trigger cultivation start
  // User must click "Begin Cultivation" button on cultivation-book.html
  if (preference === 'manual') {
    return json(200, {
      success: true,
      action: "meditation_started",
      message: "Meditation started. In manual mode, auric fills freely. Click Begin Cultivation on the book to start burning auric on scroll progress.",
      cultivation_preference: 'manual',
      auric_filling: true,
      cultivation_active: false
    });
  }

  // In auto mode: meditation auto-triggers cultivation (existing behavior)
  // Try begin first (open/paused stage), fall back to resume if paused
  const { data: beginResult, error: beginError } = await supabase
    .schema("library")
    .rpc("v2_begin_cultivation", { p_sl_avatar_key: avatarKey });

  if (beginError) {
    console.error("v2_begin_cultivation error:", beginError);
    return json(500, { error: "Failed to start cultivation", detail: beginError.message });
  }

  if (!beginResult?.success && beginResult?.error_code === "no_open_stage") {
    const { data: resumeResult, error: resumeError } = await supabase
      .schema("library")
      .rpc("v2_resume_cultivation", { p_sl_avatar_key: avatarKey });

    if (resumeError) {
      console.error("v2_resume_cultivation error:", resumeError);
      return json(500, { error: "Failed to resume cultivation", detail: resumeError.message });
    }

    if (!resumeResult?.success) {
      return json(409, {
        error: resumeResult?.message || "Cannot start cultivation",
        error_code: resumeResult?.error_code || "unknown"
      });
    }

    return json(200, { success: true, action: "resumed", cultivation_preference: 'auto', ...resumeResult });
  }

  if (!beginResult?.success) {
    return json(409, {
      error: beginResult?.message || "Cannot start cultivation",
      error_code: beginResult?.error_code || "unknown"
    });
  }

  return json(200, { success: true, action: "started", cultivation_preference: 'auto', ...beginResult });
};
