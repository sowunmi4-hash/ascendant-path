// cultivation/start-meditation.js
// Starts personal cultivation — Phase 2 rewrite.
// Dual auth: ap_session cookie OR sl_avatar_key in body.
//
// Manual mode: calls v2_start_meditation (status → 'meditating').
//   Sets last_hud_sync_at = now() so sync has a clean fill anchor.
//   HUD shows "Meditating: Yes". Player clicks "Resume Cultivation" to start scroll.
//
// Auto mode: calls v2_begin_cultivation → v2_resume_cultivation fallback.
//   Sets status = 'cultivating'. Scroll advances on next sync.

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

async function beginScrollCultivation(avatarKey) {
  const { data: beginResult, error: beginError } = await supabase
    .schema("library")
    .rpc("v2_begin_cultivation", { p_sl_avatar_key: avatarKey });

  if (beginError) return { success: false, action: null, result: null, error: beginError.message };
  if (beginResult?.success) return { success: true, action: "started", result: beginResult, error: null };

  if (beginResult?.error_code === "no_open_stage") {
    const { data: resumeResult, error: resumeError } = await supabase
      .schema("library")
      .rpc("v2_resume_cultivation", { p_sl_avatar_key: avatarKey });

    if (resumeError) return { success: false, action: null, result: null, error: resumeError.message };
    if (resumeResult?.success) return { success: true, action: "resumed", result: resumeResult, error: null };
    return { success: false, action: null, result: resumeResult, error: resumeResult?.message || "Cannot resume" };
  }

  if (beginResult?.error_code === "already_active") {
    return { success: true, action: "already_active", result: beginResult, error: null };
  }

  return { success: false, action: null, result: beginResult, error: beginResult?.message || "Cannot start" };
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
    .select("personal_cultivation_preference, cultivation_mode")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  const preference = (member?.personal_cultivation_preference || member?.cultivation_mode || "manual").toLowerCase();

  // Manual mode: start meditation only — scroll stays idle
  if (preference === "manual") {
    const { data: result, error: rpcError } = await supabase
      .schema("library")
      .rpc("v2_start_meditation", { p_sl_avatar_key: avatarKey });

    if (rpcError) {
      console.error("v2_start_meditation error:", rpcError);
      return json(500, { error: "Failed to start meditation", detail: rpcError.message });
    }

    if (!result?.success) {
      // Already active states — return without error
      if (["already_active", "already_meditating"].includes(result?.action)) {
        // Still stamp the anchor in case it's stale
        await supabase
          .from("cultivation_members")
          .update({ last_hud_sync_at: new Date().toISOString() })
          .eq("sl_avatar_key", avatarKey)
          .eq("v2_cultivation_status", "meditating"); // only update if still meditating
        return json(200, {
          success: true,
          action: result.action,
          cultivation_preference: "manual",
          v2_cultivation_status: result.v2_cultivation_status || "meditating"
        });
      }
      console.error("v2_start_meditation failed:", result);
      return json(500, { error: "Failed to start meditation", detail: result?.message });
    }

    // Stamp last_hud_sync_at so the first sync has a clean anchor
    await supabase
      .from("cultivation_members")
      .update({ last_hud_sync_at: new Date().toISOString() })
      .eq("sl_avatar_key", avatarKey);

    return json(200, {
      success: true,
      action: result.action || "meditation_started",
      message: "Meditation started. Auric and vestiges filling. Use Resume Cultivation to advance your scroll.",
      cultivation_preference: "manual",
      v2_cultivation_status: result.v2_cultivation_status || "meditating",
      auric_filling: true,
      cultivation_active: false
    });
  }

  // Auto mode: start scroll cultivation immediately
  const started = await beginScrollCultivation(avatarKey);

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
