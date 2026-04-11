// verify-login.js
// Self-contained — no dependency on _auth-utils.
// Validates SL username + HUD PIN, creates session, sets cookie.
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

  const ALLOWED_ORIGINS = [
    "https://ascendantpath.org",
    "https://www.ascendantpath.org",
    "https://nexus.ascendantpath.org",
  ];

  // ── Helpers ───────────────────────────────────────────────────
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

  function parseBody(event) {
    const raw = event.body || "";
    if (!raw) return {};
    const ct = String(
      event.headers?.["content-type"] ||
      event.headers?.["Content-Type"] || ""
    ).toLowerCase();
    if (ct.includes("application/json")) {
      try { return JSON.parse(raw); } catch { return {}; }
    }
    try {
      return Object.fromEntries(new URLSearchParams(raw).entries());
    } catch { return {}; }
  }

  async function hmacSign(secret, message) {
    if (!secret || !message) return "";
    try {
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
      return crypto.createHmac("sha256", secret).update(String(message)).digest("hex");
    } catch (e) {
      console.error("[verify-login] hmacSign error:", e);
      return "";
    }
  }

  // ── Method check ──────────────────────────────────────────────
  if (event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." }, clearCookie());
  }

  try {
    // ── Origin check ────────────────────────────────────────────
    const origin = event.headers?.origin || event.headers?.Origin || "";
    // Allow any ascendantpath.org origin (covers preview deploys and subdomains)
    const originAllowed = !origin
      || ALLOWED_ORIGINS.includes(origin)
      || origin.endsWith(".ascendantpath.org")
      || origin.endsWith(".netlify.app");
    if (!originAllowed) {
      return json(403, { success: false, message: "Origin not allowed." }, clearCookie());
    }

    // ── Parse credentials ────────────────────────────────────────
    const body = parseBody(event);
    const sl_username = String(body.sl_username || "").trim().toLowerCase();
    const login_pin   = String(body.login_pin   || "").trim();

    if (!sl_username || !login_pin) {
      return json(400, {
        success: false,
        message: "Missing username or PIN."
      }, clearCookie());
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

    // ── Admin impersonation code check ──────────────────────────
    if (login_pin.startsWith("AP-ADMIN-")) {
      const { data: codeRow } = await supabase
        .from("admin_impersonation_codes")
        .select("*")
        .eq("code", login_pin)
        .ilike("target_username", sl_username)
        .eq("used", false)
        .gt("expires_at", new Date().toISOString())
        .maybeSingle();

      if (!codeRow) {
        return json(401, { success: false, message: "Invalid or expired admin code." }, clearCookie());
      }

      // Mark code as used
      await supabase
        .from("admin_impersonation_codes")
        .update({ used: true, used_at: new Date().toISOString() })
        .eq("id", codeRow.id);

      // Look up the target member
      const { data: targetMember } = await supabase
        .from("cultivation_members")
        .select("sl_avatar_key, sl_username, display_name")
        .ilike("sl_username", sl_username)
        .maybeSingle();

      if (!targetMember) {
        return json(404, { success: false, message: "Target member not found." }, clearCookie());
      }

      const now = new Date().toISOString();

      // Create new session
      const sessionToken = crypto.randomBytes(32).toString("hex");
      const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

      await supabase.from("website_sessions").insert({
        session_token:  sessionToken,
        sl_avatar_key:  targetMember.sl_avatar_key,
        sl_username:    targetMember.sl_username,
        display_name:   targetMember.display_name,
        is_active:      true,
        origin_domain:  origin || "ascendantpath.org",
        expires_at:     expiresAt,
        last_seen_at:   now,
        created_at:     now,
        updated_at:     now,
      });

      return json(200, {
        success:       true,
        session_token: sessionToken,
        redirect_to:   "/dashboard.html",
        sl_avatar_key: targetMember.sl_avatar_key,
        sl_username:   targetMember.sl_username,
        display_name:  targetMember.display_name,
      }, buildSetCookie(sessionToken));
    }
    // ── End admin impersonation ──────────────────────────────────

    // ── 1. Find active PIN ────────────────────────────────────────
    const { data: pinRow, error: pinError } = await supabase
      .from("hud_login_pins")
      .select("*")
      .ilike("sl_username", sl_username)
      .eq("login_pin", login_pin)
      .eq("pin_active", true)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (pinError) {
      console.error("[verify-login] PIN lookup error:", pinError);
      return json(500, { success: false, message: "Server error during PIN lookup." }, clearCookie());
    }

    if (!pinRow) {
      // Check if the username exists at all for a better error message
      const { data: userExists } = await supabase
        .from("hud_login_pins")
        .select("pin_id")
        .ilike("sl_username", sl_username)
        .limit(1)
        .maybeSingle();

      if (!userExists) {
        return json(404, { success: false, message: "No account found for that username." }, clearCookie());
      }
      return json(401, { success: false, message: "Invalid PIN." }, clearCookie());
    }

    const sl_avatar_key  = String(pinRow.sl_avatar_key  || "").trim();
    const actual_username = String(pinRow.sl_username   || "").trim();
    const display_name   = String(pinRow.display_name   || actual_username || "").trim();
    const now = new Date().toISOString();

    if (!sl_avatar_key || !actual_username) {
      console.error("[verify-login] PIN row missing identity fields:", pinRow);
      return json(500, { success: false, message: "PIN row missing identity fields." }, clearCookie());
    }

    // ── 2. Deactivate PIN (single-use) ────────────────────────────
    await supabase
      .from("hud_login_pins")
      .update({ pin_active: false })
      .eq("pin_id", pinRow.pin_id);

    // ── 3. Update cultivation member if exists ─────────────────────
    // Only updates username/display_name — never resets progress
    const { data: existingMember } = await supabase
      .from("cultivation_members")
      .select("member_id")
      .eq("sl_avatar_key", sl_avatar_key)
      .maybeSingle();

    if (existingMember) {
      await supabase
        .from("cultivation_members")
        .update({ sl_username: actual_username, display_name, updated_at: now })
        .eq("sl_avatar_key", sl_avatar_key);
    }
    // Note: new member creation is handled by the HUD/SL side, not login

    // ── 4. Deactivate old active sessions for this user ───────────
    await supabase
      .from("website_sessions")
      .update({ is_active: false, updated_at: now })
      .eq("sl_avatar_key", sl_avatar_key)
      .eq("is_active", true);

    // ── 5. Create new session ─────────────────────────────────────
    const sessionToken = crypto.randomBytes(32).toString("hex");
    const expiresAt    = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(); // 24h cap

    const { error: sessionError } = await supabase
      .from("website_sessions")
      .insert({
        session_token:  sessionToken,
        sl_avatar_key,
        sl_username:    actual_username,
        display_name,
        is_active:      true,
        origin_domain:  origin || "ascendantpath.org",
        expires_at:     expiresAt,
        last_seen_at:   now,
        created_at:     now,
        updated_at:     now
      });

    if (sessionError) {
      console.error("[verify-login] session insert error:", sessionError);
      return json(500, { success: false, message: "Failed to create session." }, clearCookie());
    }

    // ── 6. Generate CSRF token ────────────────────────────────────
    let csrfToken = "";
    if (CSRF_SECRET) {
      csrfToken = await hmacSign(CSRF_SECRET, sessionToken);
    }

    // ── 7. Success ────────────────────────────────────────────────
    return json(200, {
      success:      true,
      session_token: sessionToken,   // backward-compat
      csrf_token:    csrfToken,
      redirect_to:   "/dashboard.html",
      sl_avatar_key,
      sl_username:   actual_username,
      display_name:  display_name || null
    }, buildSetCookie(sessionToken));

  } catch (err) {
    console.error("[verify-login] unexpected error:", err);
    return json(500, { success: false, message: "Server error." }, clearCookie());
  }
};