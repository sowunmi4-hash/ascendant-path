// load-clan-governance-summary.js
// Returns governance overview for the caller's clan: member counts by rank/status,
// pending join requests, pending promotion requests, recent discipline log.
// Requires is_leadership to access full detail. Lower ranks get limited view.
// Website session auth only.

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
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return json(401, { error: "Not authenticated" });

  const { data: session } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Invalid or expired session" });

  // Load caller's clan membership
  const { data: caller } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select(`
      membership_id, clan_id, clan_name, clan_key,
      current_rank_key, current_rank_order, current_rank_name,
      is_leadership, can_manage_discipline, can_review_promotions,
      membership_status
    `)
    .eq("sl_avatar_key", session.sl_avatar_key)
    .eq("membership_status", "active")
    .maybeSingle();

  if (!caller) return json(403, { error: "You are not an active clan member" });

  const clanId = caller.clan_id;

  // Member counts by rank and status
  const { data: memberRows } = await supabase
    .schema("clan")
    .from("clan_memberships")
    .select("current_rank_key, membership_status")
    .eq("clan_id", clanId)
    .in("membership_status", ["active", "probation", "suspended"]);

  const membersByRank = {};
  let totalActive = 0;
  for (const row of (memberRows || [])) {
    if (!membersByRank[row.current_rank_key]) membersByRank[row.current_rank_key] = 0;
    membersByRank[row.current_rank_key]++;
    if (row.membership_status === "active") totalActive++;
  }

  // Pending join requests (leadership only)
  let pendingJoinRequests = null;
  if (caller.is_leadership) {
    const { data: joinReqs } = await supabase
      .schema("clan")
      .from("clan_join_requests")
      .select("id, sl_username, display_name, created_at, referral_code_used")
      .eq("clan_id", clanId)
      .eq("status", "pending")
      .order("created_at", { ascending: true });
    pendingJoinRequests = joinReqs || [];
  }

  // Pending promotion requests (can_review_promotions)
  let pendingPromotionRequests = null;
  if (caller.can_review_promotions) {
    const { data: promoReqs } = await supabase
      .schema("clan")
      .from("clan_promotion_requests")
      .select("id, membership_id, requested_rank_key, sl_username, justification, created_at")
      .eq("clan_id", clanId)
      .eq("status", "pending")
      .order("created_at", { ascending: true });
    pendingPromotionRequests = promoReqs || [];
  }

  // Recent discipline log (can_manage_discipline — last 20 entries)
  let recentDiscipline = null;
  if (caller.can_manage_discipline) {
    const { data: discLog } = await supabase
      .schema("clan")
      .from("clan_discipline_log")
      .select("id, action_type, action_status, from_rank_key, to_rank_key, acted_by_username, note, created_at")
      .eq("clan_id", clanId)
      .order("created_at", { ascending: false })
      .limit(20);
    recentDiscipline = discLog || [];
  }

  // Seat summary
  const { data: seats } = await supabase
    .schema("clan")
    .from("clan_seats")
    .select("rank_key, seat_number, seat_status, holder_membership_id")
    .eq("clan_id", clanId)
    .order("rank_key")
    .order("seat_number");

  return json(200, {
    success:    true,
    clan_id:    clanId,
    clan_name:  caller.clan_name,
    clan_key:   caller.clan_key,
    caller_rank: caller.current_rank_key,
    caller_rank_name: caller.current_rank_name,
    total_active_members: totalActive,
    members_by_rank:      membersByRank,
    seats:                seats || [],
    pending_join_requests:      pendingJoinRequests,
    pending_promotion_requests: pendingPromotionRequests,
    recent_discipline:          recentDiscipline,
  });
};
