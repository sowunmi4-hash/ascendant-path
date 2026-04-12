// list-clan-join-requests.js
// Lists clan join requests for a given clan.
// Actor must be active + leadership rank in the clan.
// Defaults to pending requests. Pass status=all for full history.

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
  if (event.httpMethod !== "GET") {
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

  // --- Input ---
  const { clan_id, status } = event.queryStringParameters || {};

  if (!clan_id) {
    return json(400, { error: "clan_id is required" });
  }

  // --- Permission: active + leadership in this clan ---
  const { data: actorState, error: actorError } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select("membership_id, membership_status, is_leadership")
    .eq("clan_id", clan_id)
    .eq("sl_avatar_key", actorAvatarKey)
    .eq("membership_status", "active")
    .maybeSingle();

  if (actorError || !actorState) {
    return json(403, { error: "You are not an active member of this clan" });
  }

  if (!actorState.is_leadership) {
    return json(403, { error: "Only Elders, Clan Heads, or Ancestors can view join requests" });
  }

  // --- Query ---
  const requestedStatus = status || "pending";

  let query = supabase
    .schema("clan")
    .from("clan_join_requests")
    .select(
      "id, clan_id, sl_avatar_key, sl_username, display_name, request_status, " +
      "request_message, reviewed_by_username, reviewed_at, decision_note, created_at, updated_at"
    )
    .eq("clan_id", clan_id)
    .order("created_at", { ascending: false });

  if (requestedStatus !== "all") {
    query = query.eq("request_status", requestedStatus);
  }

  const { data: requests, error: listError } = await query;

  if (listError) {
    console.error("list-clan-join-requests error:", listError);
    return json(500, { error: "Failed to load join requests", detail: listError.message });
  }

  return json(200, {
    clan_id,
    status_filter: requestedStatus,
    requests: requests || []
  });
};
