// verify-session.js
// Self-contained — no dependency on _auth-utils.
// Reads ap_session cookie, validates against website_sessions table,
// returns authenticated identity or 401.
// v2 — 2026-04-08

const { createClient } = require("@supabase/supabase-js");
const crypto = require("crypto");

exports.handler = async (event) => {
  // ── Config from env vars ──────────────────────────────────────
  const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
  const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
  const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME   || "ap_session").trim();
  const COOKIE_DOMAIN       = (process.env.SESSION_COOKIE_DOMAIN || ".ascendantpath.org").trim();
  const MAX_AGE             = Number(process.env.SESSION_MAX_AGE_SECONDS || 60 * 60 * 24 * 7);
  const CSRF_SECRET         = (process.env.SESSION_CSRF_SECRET || "").trim();

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
      try { cookies[key] = decodeURIComponent(val); }
      catch { cookies[key] = val; }
    });
    return cookies;
  }

  function buildSetCookie(token) {
    return [
      `${COOKIE_NAME}=${token}`,
      `Domain=${COOKIE_DOMAIN}`,
      "Path=/",
      `Max-Age=${MAX_AGE}`,
      "HttpOnly",
      "Secure",
      "SameSite=Lax"
    ].join("; ");
  }

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

  function json(status, body, cookie) {
    const headers = {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "Pragma": "no-cache"
    };
    if (cookie) headers["Set-Cookie"] = cookie;
    return { statusCode: status, headers, body: JSON.stringify(body) };
  }

  async function hmacSign(secret, message) {
    if (!secret || !message) return "";
    try {
      // Try Web Crypto first (Node 19+)
      if (crypto.webcrypto && crypto.webcrypto.subtle) {
        const enc = new TextEncoder();
        const key = await crypto.webcrypto.subtle.importKey(
          "raw", enc.encode(secret),
          { name: "HMAC", hash: "SHA-256" },
          false, ["sign"]
        );
        const sig = await crypto.webcrypto.subtle.sign("HMAC", key, enc.encode(message));
        return Array.from(new Uint8Array(sig))
          .map(b => b.toString(16).padStart(2, "0"))
          .join("");
      }
      // Fallback: Node crypto (always available)
      return crypto.createHmac("sha256", secret).update(String(message)).digest("hex");
    } catch (e) {
      console.error("[verify-session] hmacSign error:", e);
      return "";
    }
  }

  // ── Only GET is allowed ───────────────────────────────────────
  if (event.httpMethod !== "GET") {
    return json(405, { success: false, authenticated: false, message: "Method not allowed." });
  }

  try {
    // ── 1. Read cookie ────────────────────────────────────────
    const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
    const cookies = parseCookies(cookieHeader);
    const sessionToken = cookies[COOKIE_NAME] || "";

    if (!sessionToken) {
      return json(401, {
        success: false,
        authenticated: false,
        message: "No active session."
      }, clearCookie());
    }

    // ── 2. Validate against DB ────────────────────────────────
    const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

    const { data: sessionRow, error: sessionError } = await supabase
      .from("website_sessions")
      .select("*")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();

    if (sessionError) {
      console.error("[verify-session] DB error:", sessionError);
      return json(500, {
        success: false,
        authenticated: false,
        message: "Failed to verify session."
      }, clearCookie());
    }

    if (!sessionRow) {
      return json(401, {
        success: false,
        authenticated: false,
        message: "Session not found or inactive."
      }, clearCookie());
    }

    // ── 3. Check server-side expiry (24h safety cap) ──────────
    if (sessionRow.expires_at) {
      const expiresAt = new Date(sessionRow.expires_at);
      if (expiresAt < new Date()) {
        // Deactivate the expired session
        await supabase
          .from("website_sessions")
          .update({ is_active: false, updated_at: new Date().toISOString() })
          .eq("session_token", sessionToken);

        return json(401, {
          success: false,
          authenticated: false,
          message: "Session expired."
        }, clearCookie());
      }
    }

    // ── 4. Touch last_seen_at ─────────────────────────────────
    const now = new Date().toISOString();
    await supabase
      .from("website_sessions")
      .update({ last_seen_at: now, updated_at: now })
      .eq("session_token", sessionToken);

    // ── 5. Generate CSRF token ────────────────────────────────
    let csrfToken = "";
    if (CSRF_SECRET) {
      csrfToken = await hmacSign(CSRF_SECRET, sessionToken);
    }

    // ── 6. Build viewer identity ──────────────────────────────
    const username = String(sessionRow.sl_username || "").trim().toLowerCase();

    const viewer = {
      sl_avatar_key: sessionRow.sl_avatar_key  || "",
      sl_username:   sessionRow.sl_username    || "",
      display_name:  sessionRow.display_name   || "",
      character_name: sessionRow.display_name  || sessionRow.sl_username || "",
      is_element_admin: username === "safareehills"
    };

    // ── 7. Refresh cookie and respond ─────────────────────────
    return json(200, {
      success:       true,
      authenticated: true,
      message:       "Session verified.",
      csrf_token:    csrfToken,
      viewer,
      session: {
        session_token_present: true,
        sl_avatar_key: sessionRow.sl_avatar_key || "",
        sl_username:   sessionRow.sl_username   || "",
        display_name:  sessionRow.display_name  || "",
        updated_at:    now,
        csrf_token:    csrfToken
      }
    }, buildSetCookie(sessionToken));

  } catch (err) {
    console.error("[verify-session] unexpected error:", err);
    return json(500, {
      success: false,
      authenticated: false,
      message: "Server error during session verification."
    }, clearCookie());
  }
};