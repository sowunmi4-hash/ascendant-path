// verify-clan-recruit-referral.js
// Verifies a pending clan recruit referral.
// Actor must be active + leadership rank in the clan.
// Delegates all business logic to clan.verify_clan_recruit_referral() SQL helper.

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

  const { referral_id, accepted_membership_id, note } = body;

  if (!referral_id) {
    return json(400, { error: "referral_id is required" });
  }

  // --- Load referral to determine clan scope ---
  const { data: referral, error: referralError } = await supabase
    .schema("clan")
    .from("clan_recruit_referrals")
    .select("id, clan_id, referral_status, recruit_username")
    .eq("id", referral_id)
    .maybeSingle();

  if (referralError || !referral) {
    return json(404, { error: "Referral not found" });
  }

  if (referral.referral_status !== "pending") {
    return json(409, { error: "Referral is not pending", referral_status: referral.referral_status });
  }

  // --- Permission: actor must be active + leadership rank in this clan ---
  const { data: actorState, error: actorError } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select("membership_id, membership_status, is_leadership")
    .eq("clan_id", referral.clan_id)
    .eq("sl_avatar_key", actorAvatarKey)
    .eq("membership_status", "active")
    .maybeSingle();

  if (actorError || !actorState) {
    return json(403, { error: "You are not an active member of this clan" });
  }

  if (!actorState.is_leadership) {
    return json(403, { error: "Only Elders, Clan Heads, or Ancestors can verify referrals" });
  }

  // --- Delegate to SQL helper ---
  const { data: result, error: rpcError } = await supabase
    .schema("clan")
    .rpc("verify_clan_recruit_referral", {
      p_referral_id:            referral_id,
      p_accepted_membership_id: accepted_membership_id || null,
      p_actor_avatar_key:       actorAvatarKey,
      p_actor_username:         actorUsername,
      p_note:                   note || null
    });

  if (rpcError) {
    console.error("verify_clan_recruit_referral error:", rpcError);
    return json(500, { error: "Failed to verify referral", detail: rpcError.message });
  }

  const verified = Array.isArray(result) ? result[0] : result;

  return json(200, {
    success: true,
    referral_id:            verified.referral_id,
    referrer_membership_id: verified.referrer_membership_id,
    recruit_username:       verified.recruit_username,
    referral_status:        verified.referral_status
  });
};
