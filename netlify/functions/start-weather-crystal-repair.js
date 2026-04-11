// start-weather-crystal-repair.js
// Thin action helper for parcel Weather Crystal repair.
// Database is the single source of truth.
// This file only triggers repair -- no duration calc, no Qi calc, no status logic.
//
// Auth: Two paths --
//   1. Cookie session (browser)  -> avatar key resolved from session
//   2. sl_avatar_key in body (LSL in-world) -> verified against members table

const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

// -- Helpers --

function parseCookies(header) {
  var cookies = {};
  if (!header) return cookies;
  header.split(";").forEach(function(part) {
    var trimmed = part.trim();
    var eq = trimmed.indexOf("=");
    if (eq === -1) return;
    var key = trimmed.slice(0, eq).trim();
    var val = trimmed.slice(eq + 1).trim();
    try { cookies[key] = decodeURIComponent(val); } catch(e) { cookies[key] = val; }
  });
  return cookies;
}

function json(statusCode, body) {
  return {
    statusCode: statusCode,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    },
    body: JSON.stringify(body)
  };
}

function safeText(value, fallback) {
  var t = String(value || "").trim();
  return t || (fallback || "");
}

// -- Handler --

exports.handler = async function(event) {

  // Method check
  if (event.httpMethod === "OPTIONS") {
    return json(200, { ok: true });
  }
  if (event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed. Use POST." });
  }

  // Parse request body first (needed for both auth paths)
  var body = {};
  try {
    if (event.body) body = JSON.parse(event.body);
  } catch(e) {
    return json(400, { success: false, message: "Invalid JSON body." });
  }

  var parcelKey = safeText(body.parcel_key);
  if (!parcelKey) {
    return json(400, { success: false, message: "parcel_key is required." });
  }

  // -- Auth: try cookie session first, fall back to sl_avatar_key --
  var avatarKey = "";

  // Path 1: Cookie session (browser callers)
  var cookieHeader = (event.headers && event.headers.cookie) || (event.headers && event.headers.Cookie) || "";
  var sessionToken = parseCookies(cookieHeader)[COOKIE_NAME] || "";

  if (sessionToken) {
    var sessionCheck = await supabase
      .from("website_sessions")
      .select("sl_avatar_key, sl_username")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .limit(1)
      .maybeSingle();

    if (sessionCheck.data && sessionCheck.data.sl_avatar_key) {
      avatarKey = safeText(sessionCheck.data.sl_avatar_key);
    }
  }

  // Path 2: sl_avatar_key in body (LSL in-world callers)
  if (!avatarKey) {
    var bodyAvatarKey = safeText(body.sl_avatar_key);

    if (!bodyAvatarKey) {
      return json(401, {
        success: false,
        message: "No valid session cookie or sl_avatar_key provided."
      });
    }

    // Verify the avatar key exists in the members table
    var memberCheck = await supabase
      .from("members")
      .select("sl_avatar_key")
      .eq("sl_avatar_key", bodyAvatarKey)
      .limit(1)
      .maybeSingle();

    if (memberCheck.error || !memberCheck.data) {
      return json(403, {
        success: false,
        message: "Avatar key not recognised as a registered member."
      });
    }

    avatarKey = bodyAvatarKey;
  }

  // -- Call database function --
  try {
    var result = await supabase.schema("weather").rpc("start_parcel_crystal_repair", {
      p_parcel_key: parcelKey,
      p_sl_avatar_key: avatarKey
    });

    if (result.error) {
      console.error("start-weather-crystal-repair DB error:", result.error.message);
      return json(500, {
        success: false,
        message: "Failed to start crystal repair.",
        error: result.error.message
      });
    }

    var data = result.data;

    // DB function returns jsonb with success field
    if (!data || data.success === false) {
      var errorCode = data && data.error ? data.error : "unknown_error";
      var httpStatus = 400;

      if (errorCode === "parcel_crystal_not_found") httpStatus = 404;
      if (errorCode === "member_not_found") httpStatus = 404;
      if (errorCode === "insufficient_qi") httpStatus = 422;
      if (errorCode === "already_repairing") httpStatus = 409;
      if (errorCode === "no_repair_needed") httpStatus = 409;

      return json(httpStatus, data);
    }

    // Return the DB result as-is
    return json(200, data);

  } catch(err) {
    console.error("start-weather-crystal-repair unexpected error:", err);
    return json(500, {
      success: false,
      message: "Unexpected error starting crystal repair."
    });
  }
};
