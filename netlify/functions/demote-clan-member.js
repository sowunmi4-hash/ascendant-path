// demote-clan-member.js
// Demotes a clan member by one rank.
// Requires can_manage_discipline. Actor must outrank target.
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
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return json(401, { error: "Not authenticated" });

  const { data: session } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Invalid or expired session" });

  const actorKey = session.sl_avatar_key;
  const { target_membership_id, note } = body;
  if (!target_membership_id) return json(400, { error: "target_membership_id is required" });

  // Load actor
  const { data: actor } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select("membership_id, clan_id, current_rank_key, current_rank_order, can_manage_discipline, membership_status")
    .eq("sl_avatar_key", actorKey)
    .eq("membership_status", "active")
    .maybeSingle();

  if (!actor) return json(403, { error: "You are not an active clan member" });
  if (!actor.can_manage_discipline) return json(403, { error: "You do not have permission to demote members" });

  // Load target
  const { data: target } = await supabase
    .schema("clan")
    .from("clan_memberships")
    .select("id, clan_id, sl_avatar_key, sl_username, current_rank_key, membership_status")
    .eq("id", target_membership_id)
    .maybeSingle();

  if (!target) return json(404, { error: "Target member not found" });
  if (target.clan_id !== actor.clan_id) return json(403, { error: "Target is not in your clan" });
  if (target.sl_avatar_key === actorKey) return json(409, { error: "You cannot demote yourself" });
  if (!["active", "probation"].includes(target.membership_status)) {
    return json(409, { error: `Cannot demote a member with status: ${target.membership_status}` });
  }

  // Get all ranks ordered
  const { data: allRanks } = await supabase
    .schema("clan")
    .from("clan_rank_catalog")
    .select("rank_key, rank_order")
    .order("rank_order", { ascending: true });

  const targetRankRow = allRanks?.find(r => r.rank_key === target.current_rank_key);
  if (!targetRankRow) return json(500, { error: "Could not resolve target rank" });

  if (targetRankRow.rank_order >= actor.current_rank_order) {
    return json(403, { error: "You can only demote members of lower rank than yourself" });
  }

  // Find the rank one step below target's current rank
  const lowerRanks = allRanks.filter(r => r.rank_order < targetRankRow.rank_order);
  if (lowerRanks.length === 0) {
    return json(409, { error: "Member is already at the lowest rank" });
  }
  const newRank = lowerRanks[lowerRanks.length - 1]; // highest of the lower ranks

  const now = new Date().toISOString();

  await supabase
    .schema("clan")
    .from("clan_memberships")
    .update({ current_rank_key: newRank.rank_key, rank_assigned_at: now, updated_at: now })
    .eq("id", target_membership_id);

  // Vacate seat if held
  await supabase
    .schema("clan")
    .from("clan_seats")
    .update({ seat_status: "vacant", holder_membership_id: null, vacated_at: now, updated_at: now })
    .eq("holder_membership_id", target_membership_id);

  // Log rank change
  await supabase.schema("clan").from("clan_rank_change_log").insert({
    clan_id:             actor.clan_id,
    membership_id:       target_membership_id,
    action_type:         "demoted",
    from_rank_key:       target.current_rank_key,
    to_rank_key:         newRank.rank_key,
    acted_by_avatar_key: actorKey,
    acted_by_username:   session.sl_username || null,
    note:                note || null,
    created_at:          now,
  });

  return json(200, {
    success:         true,
    target_username: target.sl_username,
    from_rank_key:   target.current_rank_key,
    to_rank_key:     newRank.rank_key,
  });
};
