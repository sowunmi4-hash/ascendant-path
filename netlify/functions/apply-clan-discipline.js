// apply-clan-discipline.js
// Applies a discipline action (warn, probation, suspend) to a clan member.
// Requires can_manage_discipline permission. Actor must outrank target.
// Website session auth only (leadership action).

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

const VALID_ACTIONS = ["warn", "probation", "suspend"];

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  // Auth
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

  const actorKey = session.sl_avatar_key;

  const { target_membership_id, action_type, note } = body;
  if (!target_membership_id) return json(400, { error: "target_membership_id is required" });
  if (!VALID_ACTIONS.includes(action_type)) {
    return json(400, { error: `action_type must be one of: ${VALID_ACTIONS.join(", ")}` });
  }

  // Load actor's clan state
  const { data: actor } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select("membership_id, clan_id, current_rank_key, current_rank_order, can_manage_discipline, membership_status")
    .eq("sl_avatar_key", actorKey)
    .eq("membership_status", "active")
    .maybeSingle();

  if (!actor) return json(403, { error: "You are not an active clan member" });
  if (!actor.can_manage_discipline) return json(403, { error: "You do not have permission to apply discipline" });

  // Load target membership
  const { data: target } = await supabase
    .schema("clan")
    .from("clan_memberships")
    .select("id, clan_id, sl_avatar_key, sl_username, current_rank_key, membership_status")
    .eq("id", target_membership_id)
    .maybeSingle();

  if (!target) return json(404, { error: "Target member not found" });
  if (target.clan_id !== actor.clan_id) return json(403, { error: "Target is not in your clan" });
  if (!["active", "probation"].includes(target.membership_status)) {
    return json(409, { error: `Cannot discipline a member with status: ${target.membership_status}` });
  }
  if (target.sl_avatar_key === actorKey) return json(409, { error: "You cannot discipline yourself" });

  // Verify actor outranks target
  const { data: targetRank } = await supabase
    .schema("clan")
    .from("clan_rank_catalog")
    .select("rank_order")
    .eq("rank_key", target.current_rank_key)
    .maybeSingle();

  if (targetRank && targetRank.rank_order >= actor.current_rank_order) {
    return json(403, { error: "You can only discipline members of lower rank than yourself" });
  }

  const now = new Date().toISOString();

  // Determine new membership_status
  const newStatus = action_type === "warn" ? target.membership_status
    : action_type === "probation" ? "probation"
    : "suspended";

  const updateFields = { membership_status: newStatus, updated_at: now };
  if (action_type === "probation" && target.membership_status !== "probation") {
    updateFields.probation_started_at = now;
  }
  if (action_type === "suspend") {
    updateFields.suspended_at = now;
  }

  await supabase
    .schema("clan")
    .from("clan_memberships")
    .update(updateFields)
    .eq("id", target_membership_id);

  // Log the discipline action
  const { data: actorSession } = await supabase
    .from("website_sessions")
    .select("sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();

  await supabase.schema("clan").from("clan_discipline_log").insert({
    clan_id:              actor.clan_id,
    membership_id:        target_membership_id,
    action_type:          action_type,
    action_status:        "applied",
    loyalty_delta:        0,
    contribution_delta:   0,
    note:                 note || null,
    acted_by_avatar_key:  actorKey,
    acted_by_username:    actorSession?.sl_username || null,
    resolved_at:          action_type === "warn" ? now : null,
    created_at:           now,
    updated_at:           now,
  });

  return json(200, {
    success:         true,
    action_type,
    target_username: target.sl_username,
    new_status:      newStatus,
  });
};
