// submit-clan-promotion-request.js
// Submits a promotion request for a clan member.
// Member can self-submit, or an Elder+ can submit on their behalf.
// SQL owns: rank order check, realm gate, actor permission, seat availability check.

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

  const { membership_id, target_rank_key, request_note } = body;

  if (!membership_id) {
    return json(400, { error: "membership_id is required" });
  }

  if (!target_rank_key) {
    return json(400, { error: "target_rank_key is required" });
  }

  // --- Delegate entirely to SQL ---
  // SQL owns: membership existence, status check, rank order gate,
  // realm gate, actor permission (self or Elder+), seat availability, request insert
  const { data: result, error: rpcError } = await supabase
    .schema("clan")
    .rpc("submit_clan_promotion_request", {
      p_membership_id:    membership_id,
      p_target_rank_key:  target_rank_key,
      p_actor_avatar_key: actorAvatarKey,
      p_actor_username:   actorUsername,
      p_is_admin:         false,
      p_request_note:     request_note || null
    });

  if (rpcError) {
    console.error("submit_clan_promotion_request error:", rpcError);

    const msg = rpcError.message || "";
    if (msg.includes("membership not found"))          return json(404, { error: "Membership not found" });
    if (msg.includes("not in an eligible state"))      return json(409, { error: "Membership is not in an eligible state for promotion" });
    if (msg.includes("target rank not found"))         return json(400, { error: "Invalid target rank" });
    if (msg.includes("must be higher than current"))   return json(400, { error: "Target rank must be higher than current rank" });
    if (msg.includes("does not meet realm"))           return json(403, { error: "Member does not meet the realm requirement for this rank" });
    if (msg.includes("not allowed to submit"))         return json(403, { error: "You do not have permission to submit this promotion request" });
    if (msg.includes("ancestor"))                      return json(400, { error: "Ancestor rank is not submitted through the normal promotion flow" });

    return json(500, { error: "Failed to submit promotion request", detail: msg });
  }

  const row = Array.isArray(result) ? result[0] : result;

  return json(200, {
    success:             true,
    request_id:          row.request_id,
    clan_id:             row.clan_id,
    membership_id:       row.membership_id,
    current_rank_key:    row.current_rank_key,
    target_rank_key:     row.target_rank_key,
    request_status:      row.request_status,
    seat_available_now:  row.seat_available_now
  });
};
