// pause-cultivation.js
// Thin proxy — forwards to stop-meditation.
// Exists so any cached frontend responses that reference this endpoint continue to work.

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
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ignore */ }

  const cookies = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const sessionToken = cookies[COOKIE_NAME];

  // Resolve member identity
  let memberUuid = null;

  if (sessionToken) {
    const { data: session } = await supabase
      .from("ap_sessions")
      .select("member_uuid")
      .eq("token", sessionToken)
      .maybeSingle();
    if (session?.member_uuid) memberUuid = session.member_uuid;
  }

  if (!memberUuid && body.sl_avatar_key) {
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("id")
      .eq("sl_avatar_key", body.sl_avatar_key)
      .maybeSingle();
    if (member?.id) memberUuid = member.id;
  }

  if (!memberUuid && body.sl_username) {
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("id")
      .eq("sl_username", body.sl_username)
      .maybeSingle();
    if (member?.id) memberUuid = member.id;
  }

  if (!memberUuid) {
    return json(401, { success: false, message: "Unauthorized." });
  }

  // Delegate to v2_pause_cultivation RPC
  const { data: rpcData, error: rpcError } = await supabase.rpc("v2_pause_cultivation", {
    p_member_uuid: memberUuid,
    p_volume_number: body.volume_number || null,
    p_section_key: body.section_key || null,
  });

  if (rpcError) {
    return json(500, { success: false, message: rpcError.message || "Failed to pause cultivation." });
  }

  const result = rpcData?.[0] ?? rpcData ?? {};
  if (result.success === false) {
    return json(400, { success: false, message: result.message || "Could not pause cultivation." });
  }

  return json(200, { success: true, message: result.message || "Cultivation paused." });
};
