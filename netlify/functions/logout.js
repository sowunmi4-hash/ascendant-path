// logout.js
// Self-contained — no dependency on _auth-utils.
// Reads ap_session cookie, deactivates session in DB, clears cookie.
// Works from both ascendantpath.org and nexus.ascendantpath.org.
// v2 — 2026-04-08

const { createClient } = require("@supabase/supabase-js");

exports.handler = async (event) => {
  // ── Config from env vars ──────────────────────────────────────
  const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
  const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
  const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME   || "ap_session").trim();
  const COOKIE_DOMAIN       = (process.env.SESSION_COOKIE_DOMAIN || ".ascendantpath.org").trim();

  const ALLOWED_ORIGINS = [
    "https://ascendantpath.org",
    "https://www.ascendantpath.org",
    "https://nexus.ascendantpath.org"
  ];

  // ── Helpers ───────────────────────────────────────────────────
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

  // Max-Age=0 tells the browser to delete the cookie immediately
  function clearCookie() {
    return [
      `${COOKIE_NAME}=`,
      `Domain=${COOKIE_DOMAIN}`,
      "Path=/",
      "Max-Age=0",
      "Expires=Thu, 01 Jan 1970 00:00:00 GMT",
      "HttpOnly",
      "Secure",
      "SameSite=Lax"
    ].join("; ");
  }

  function json(status, body) {
    return {
      statusCode: status,
      headers: {
        "Content-Type":  "application/json",
        "Cache-Control": "no-store",
        "Pragma":        "no-cache",
        "Set-Cookie":    clearCookie()  // always clear the cookie on every response
      },
      body: JSON.stringify(body)
    };
  }

  // ── Only POST is allowed ──────────────────────────────────────
  if (event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." });
  }

  try {
    // ── Origin check ──────────────────────────────────────────
    const origin = event.headers?.origin || event.headers?.Origin || "";
    if (origin && !ALLOWED_ORIGINS.includes(origin)) {
      // Still clear the cookie even on bad origin
      return json(403, { success: false, message: "Origin not allowed." });
    }

    // ── 1. Get session token — cookie first, body fallback ────
    const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
    const cookies = parseCookies(cookieHeader);
    let sessionToken = cookies[COOKIE_NAME] || "";

    // Body fallback for backward-compat with any existing JS that
    // sends session_token in the request body
    if (!sessionToken) {
      try {
        const raw = event.body || "";
        if (raw) {
          const body = JSON.parse(raw);
          sessionToken = String(body.session_token || "").trim();
        }
      } catch { /* no body or bad JSON — that's fine */ }
    }

    // ── 2. Deactivate session in DB ───────────────────────────
    if (sessionToken) {
      const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

      const { error } = await supabase
        .from("website_sessions")
        .update({
          is_active:  false,
          updated_at: new Date().toISOString()
        })
        .eq("session_token", sessionToken)
        .eq("is_active", true);

      if (error) {
        console.error("[logout] DB deactivate error:", error);
        // Still return success — the cookie is cleared regardless
      }
    }

    // ── 3. Always return success with cleared cookie ──────────
    return json(200, { success: true });

  } catch (err) {
    console.error("[logout] unexpected error:", err);
    // Even on error, clear the cookie
    return json(500, { success: false, message: "Server error during logout." });
  }
};