// start-meditation.js
// Starts personal cultivation using v2 system.
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).
//
// Manual mode behaviour:
//   - Calls v2_begin_cultivation (or v2_resume_cultivation as fallback) to set
//     meditation_active = true so auric fills and the HUD shows "Meditating: Yes".
//   - Immediately calls v2_pause_cultivation to keep scroll progress idle.
//   - Returns success so the HUD knows meditation has started.
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
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("sl_avatar_key")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();
    if (member?.sl_avatar_key) return member.sl_avatar_key;
  }

  return null;
}

// Starts cultivation (begin or resume fallback).
// Returns { success, action, result, error } where error is non-null on failure.
async function startCultivation(avatarKey) {
  // Try begin first (handles open/new stage)
  const { data: beginResult, error: beginError } = await supabase
    .schema("library")
    .rpc("v2_begin_cultivation", { p_sl_avatar_key: avatarKey });

  if (beginError) {
    return { success: false, action: null, result: null, error: beginError.message };
  }

  if (beginResult?.success) {
    return { success: true, action: "started", result: beginResult, error: null };
  }

  // If no open stage, try resuming a paused session
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

  // already