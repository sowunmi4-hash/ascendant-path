// weather-loader.js
// Thin loader for Weather Crystal parcel state.
// Database is the single source of truth.
// This file only reads — no damage calc, no repair calc, no mutations.

const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

// ── Helpers ──────────────────────────────────────────────────

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

// ── Handler ──────────────────────────────────────────────────

exports.handler = async function(event) {

  // Method check
  if (event.httpMethod === "OPTIONS") {
    return json(200, { ok: true });
  }
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." });
  }

  // Session check (optional — crystal state is parcel-level,
  // but we verify session to ensure the caller is authenticated)
  var cookieHeader = event.headers.cookie || event.headers.Cookie || "";
  var sessionToken = parseCookies(cookieHeader)[COOKIE_NAME] || "";

  if (!sessionToken) {
    return json(401, { success: false, message: "No active session." });
  }

  var sessionCheck = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();

  if (sessionCheck.error || !sessionCheck.data) {
    return json(401, { success: false, message: "Session expired or invalid." });
  }

  // Read parcel_key from query or body
  var query = event.queryStringParameters || {};
  var body = {};
  try {
    if (event.body) body = JSON.parse(event.body);
  } catch(e) {
    // ignore parse errors — fall through to query params
  }

  var parcelKey = safeText(query.parcel_key || body.parcel_key);

  if (!parcelKey) {
    return json(400, {
      success: false,
      message: "parcel_key is required."
    });
  }

  // Call database loader — single source of truth
  try {
    var result = await supabase.rpc("load_parcel_crystal_state", {
      p_parcel_key: parcelKey
    });

    if (result.error) {
      console.error("weather-loader DB error:", result.error.message);
      return json(500, {
        success: false,
        message: "Failed to load weather crystal state.",
        error: result.error.message
      });
    }

    var data = result.data;

    // DB function returns jsonb — Supabase parses it automatically
    if (!data || data.success === false) {
      return json(404, {
        success: false,
        message: data && data.error ? data.error : "Parcel crystal not found.",
        parcel_key: parcelKey
      });
    }

    // Return the DB result as-is — no mutations, no extra math
    return json(200, data);

  } catch(err) {
    console.error("weather-loader unexpected error:", err);
    return json(500, {
      success: false,
      message: "Unexpected error loading weather crystal state."
    });
  }
};
