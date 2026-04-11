// generate-admin-code.js
// Admin-only. Generates a short-lived impersonation code for a target username.
// Only callable by safareehills.
// v1 — 2026-04-09

const { createClient } = require("@supabase/supabase-js");
const crypto = require("crypto");

const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();
const ADMIN_USERNAME      = "safareehills";

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
    body: JSON.stringify(body),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." });
  }

  // Auth — must be logged in as admin
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return json(401, { success: false, message: "No active session." });

  const { data: session } = await supabase
    .from("website_sessions")
    .select("sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();

  if (!session?.sl_username) {
    return json(401, { success: false, message: "Session not found or inactive." });
  }

  if (session.sl_username.toLowerCase() !== ADMIN_USERNAME.toLowerCase()) {
    return json(403, { success: false, message: "Admin access required." });
  }

  // Parse target username from body
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { body = {}; }
  const target_username = String(body.target_username || "").trim().toLowerCase();

  if (!target_username) {
    return json(400, { success: false, message: "target_username is required." });
  }

  // Verify target member exists
  const { data: member } = await supabase
    .from("cultivation_members")
    .select("sl_username")
    .ilike("sl_username", target_username)
    .maybeSingle();

  if (!member) {
    return json(404, { success: false, message: `No member found with username: ${target_username}` });
  }

  // Generate code: AP-ADMIN-XXXXXXXX
  const code = "AP-ADMIN-" + crypto.randomBytes(4).toString("hex").toUpperCase();
  const expires_at = new Date(Date.now() + 60 * 60 * 1000).toISOString(); // 1 hour

  const { error } = await supabase
    .from("admin_impersonation_codes")
    .insert({
      code,
      target_username: member.sl_username,
      generated_by: session.sl_username,
      expires_at,
    });

  if (error) {
    console.error("[generate-admin-code] insert error:", error);
    return json(500, { success: false, message: "Failed to generate code." });
  }

  return json(200, {
    success: true,
    code,
    target_username: member.sl_username,
    expires_at,
    message: `Use code "${code}" with username "${member.sl_username}" on the login page. Valid for 15 minutes.`,
  });
};
