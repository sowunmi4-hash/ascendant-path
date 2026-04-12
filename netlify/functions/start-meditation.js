// start-meditation.js
// Starts personal cultivation using v2 system.
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).
//
// Manual mode behaviour:
//   - Calls v2_begin_cultivation (or v2_resume_cultivation as fallback) to set
//     meditation_active = true so auric fills and the HUD shows "Meditating: Yes".
//   - Immediately calls v2_pause_cultivation to keep scroll progress idle.
//
// Auto mode behaviour (unchanged):
//   - Calls v2_begin_cultivation / v2_resume_cultivation and lets cultivation run
//     fully (scroll advances as auric is consumed).

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

// Starts cultivation via begin or resume fallback.
async function startCultivation(avatarKey) {
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
  if (!avatarKey) {
    return json(401, { error: "Not authenticated" });
  }

  const { data: member } = await supabase
    .from("cultivation_members")
    .select("personal_cultivation_preference")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  const preference = (member?.personal_cultivation_preference || "manual").toLowerCase();

  if (preference === "manual") {
    const started = await startCultivation(avatarKey);

    if (!started.success) {
      console.error("startCultivation (manual) error:", started.error);
      return json(500, { error: "Failed to start meditation", detail: started.error });
    }

    const { error: pauseError } = await supabase
      .schema("library")
      .rpc("v2_pause_cultivation", { p_sl_avatar_key: avatarKey });

    if (pauseError) {
      console.warn("v2_pause_cultivation (manual) warn:", pauseError.message);
    }

    return json(200, {
      success: true,
      action: "meditation_started",
      message: "Meditation started. Auric is filling. In manual mode, scroll is paused.",
      cultivation_preference: "manual",
      auric_filling: true,
      cultivation_active: false
    });
  }

  // Auto mode
  const started = await startCultivation(avatarKey);

  if (!started.success) {
    const errCode = started.result?.error_code || "unknown";
    console.error("startCultivation (auto) error:", started.error);
    return json(409, {
      error: started.error,
      error_code: errCode
    });
  }

  return json(200, {
    success: true,
    action: started.action,
    cultivation_preference: "auto",
    ...started.result
  });
};
