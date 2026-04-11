// review-clan-join-request.js
// Approves or denies a pending clan join request.
// Actor must be Elder, Clan Head, or Ancestor in the clan.
// SQL function owns all permission checks, seat assignment, and state transitions.

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

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  // --- Auth ---
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  const sessionToken = cookies[COOKIE_NAME] || "";

  if (!sessionToken) {
    return json(401, { error: "Not authenticated" });
  }

  const { data: sessionRow, error: sessionError } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (sessionError || !sessionRow) {
    return json(401, { error: "Invalid or expired session" });
  }

  const actorAvatarKey = sessionRow.sl_avatar_key;
  const actorUsername  = sessionRow.sl_username;

  // --- Input ---
  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const { request_id, approve, note } = body;

  if (!request_id) {
    return json(400, { error: "request_id is required" });
  }

  if (typeof approve !== "boolean") {
    return json(400, { error: "approve must be a boolean (true to approve, false to deny)" });
  }

  // --- Delegate entirely to SQL ---
  // SQL owns: pending check, rank gate (Elder+), seat availability,
  // membership creation on approval, denial update
  const { data: result, error: rpcError } = await supabase
    .schema("clan")
    .rpc("review_clan_join_request", {
      p_request_id:       request_id,
      p_approve:          approve,
      p_actor_avatar_key: actorAvatarKey,
      p_actor_username:   actorUsername,
      p_is_admin:         false,
      p_note:             note || null
    });

  if (rpcError) {
    console.error("review_clan_join_request error:", rpcError);

    // Surface meaningful SQL errors to the caller
    const msg = rpcError.message || "";
    if (msg.includes("not pending"))       return json(409, { error: "Request is no longer pending" });
    if (msg.includes("not an eligible"))   return json(403, { error: "You are not an active member of this clan" });
    if (msg.includes("only Elder"))        return json(403, { error: "Only Elders, Clan Heads, or Ancestors can review join requests" });
    if (msg.includes("no available seat")) return json(409, { error: "No seats available for this rank" });

    return json(500, { error: "Failed to review join request", detail: msg });
  }

  const row = Array.isArray(result) ? result[0] : result;

  return json(200, {
    success:       true,
    request_id:    row.request_id,
    request_status: row.request_status,
    clan_id:       row.clan_id,
    membership_id: row.membership_id || null,
    seat_id:       row.seat_id       || null
  });
};
