// start-meditation.js
// Starts personal cultivation using v2 system.
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).
//
// Manual mode: calls v2_start_meditation — sets v2_cultivation_status = 'meditating'.
//   HUD shows "Meditating: Yes". Cultivation book is NOT started.
//   Auric fills freely. Player uses "Resume Cultivation" on website to start scroll.
//   sync-meditation-progress returns early when status = 'meditating' (no scroll advance).
//   When player uses "Resume Cultivation", status becomes 'cultivating' and sync runs normally.
//
// Auto mode: calls v2_begin_cultivation (or v2_resume fallback) — sets status = 'cultivating'.
//   HUD shows "Meditating: Yes". Sync drives scroll normally.

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

// Starts cultivation: tries v2_begin_cultivation, falls back to v2_resume_cultivation.
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

  // Manual mode: call v2_start_meditation — sets status = 'meditating'.
  // Does NOT call v2_begin_cultivation, so the cultivation book stays idle.
  // HUD shows "Meditating: Yes". Player uses "Resume Cultivation" on website to start scroll.
  if (preference === "manual") {
    const { data: result, error: rpcError } = await supabase
      .schema("library")
      .rpc("v2_start_meditation", { p_sl_avatar_key: avatarKey });

    if (rpcError) {
      console.error("v2_start_meditation error:", rpcError);
      return json(500, { error: "Failed to start meditation", detail: rpcError.message });
    }

    if (!result?.success) {
      console.error("v2_start_meditation failed:", result);
      return json(500, { error: "Failed to start meditation", detail: result?.message });
    }

    return json(200, {
      success: true,
      action: result.action || "meditation_started",
      message: "Meditation started. Auric is filling. Use the cultivation book to begin cultivating when ready.",
      cultivation_preference: "manual",
      v2_cultivation_status: result.v2_cultivation_status || "meditating",
      auric_filling: true,
      cultivation_active: false
    });
  }

  // Auto mode: start cultivation and let sync drive scroll normally.
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
