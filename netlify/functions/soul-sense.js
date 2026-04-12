// soul-sense.js
// Returns active cultivators filtered by the viewer's realm perception tier.
// Perception tiers (realm_index of VIEWER):
//   1 Mortal           — name + gender
//   2 Auric Gathering  — + realm
//   3 Foundation       — + path
//   4 Core Formation   — + stage
//   5 Nascent Soul     — + bond status
//   6 Soul Transform.  — + breakthrough history
//   7 Void Refinement  — + stat tier (weak/moderate/strong)
//   8 Body Integration — + alignment drift direction
//   9 Tribulation      — + awakening lineage
//  10 Immortal         — full perception
//
// "Active" = last_presence_at within 10 minutes
// Auth: ap_session cookie OR sl_avatar_key in body

const { createClient } = require("@supabase/supabase-js");
const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();
const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

function parseCookies(h) {
  const c = {}; if (!h) return c;
  h.split(";").forEach(p => {
    const e = p.trim().indexOf("="); if (e < 0) return;
    const k = p.trim().slice(0,e).trim(); const v = p.trim().slice(e+1).trim();
    try { c[k] = decodeURIComponent(v); } catch { c[k] = v; }
  });
  return c;
}

function json(s, b) {
  return { statusCode: s, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }, body: JSON.stringify(b) };
}

function statTier(total) {
  if (!total || total < 50)  return "weak";
  if (total < 150)           return "moderate";
  if (total < 300)           return "strong";
  return "transcendent";
}

function applyPerceptionFilter(member, viewerRealm, stats, recentBreakthroughs) {
  const tier = Math.min(Math.max(viewerRealm || 1, 1), 10);

  // Tier 1 — always visible
  const result = {
    sl_username:    member.character_name || member.sl_username || "Unknown Cultivator",
    gender:         member.gender || "male",
    is_self:        member._is_self || false,
    realm_index:    member.realm_index
  };

  // Tier 2+ realm
  if (tier >= 2) {
    result.realm_display_name = member.realm_display_name || member.realm_key || "Mortal";
  }

  // Tier 3+ path
  if (tier >= 3) {
    result.alignment_path = member.alignment_path || "Unaligned";
  }

  // Tier 4+ stage
  if (tier >= 4) {
    const stageKey = member.v2_active_stage_key || "";
    const parts    = stageKey.split(":");
    result.stage = parts[1]
      ? parts[1].charAt(0).toUpperCase() + parts[1].slice(1)
      : "Base";
    result.cultivation_status = member.v2_cultivation_status || "idle";
  }

  // Tier 5+ bond status
  if (tier >= 5) {
    result.has_bond = member.has_bond || false;
    result.bond_status = member.bond_status || "none";
  }

  // Tier 6+ breakthrough history
  if (tier >= 6) {
    result.total_breakthroughs = recentBreakthroughs[member.sl_avatar_key] || 0;
  }

  // Tier 7+ stat tier
  if (tier >= 7 && stats[member.sl_avatar_key]) {
    const s     = stats[member.sl_avatar_key];
    const total = (s.vitality || 0) + (s.will || 0) + (s.resonance || 0) + (s.insight || 0);
    result.stat_tier = statTier(total);
    result.stat_total = total;
  }

  // Tier 8+ drift direction
  if (tier >= 8) {
    result.drift_direction = member.drift_direction || null;
  }

  // Tier 9+ awakening lineage
  if (tier >= 9) {
    result.awakened_by = member.awakened_by || null;
  }

  // Tier 10 — full
  if (tier >= 10) {
    result.region = member.current_region_name || null;
    result.auric_current = member.auric_current || 0;
    result.auric_maximum = member.auric_maximum || 0;
    result.vestiges       = member.vestiges      || 0;
  }

  return result;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  // Resolve viewer
  const cookies      = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const sessionToken = cookies[COOKIE_NAME] || "";
  let viewerAvatarKey = "";

  if (sessionToken) {
    const { data: sess } = await supabase
      .from("website_sessions")
      .select("sl_avatar_key")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();
    if (sess?.sl_avatar_key) viewerAvatarKey = sess.sl_avatar_key;
  }

  let body = {}; try { body = JSON.parse(event.body || "{}"); } catch {}
  if (!viewerAvatarKey) viewerAvatarKey = (body.sl_avatar_key || "").trim();
  if (!viewerAvatarKey) return json(401, { error: "Not authenticated" });

  // Load viewer's realm
  const { data: viewer } = await supabase
    .from("cultivation_members")
    .select("realm_index, sl_avatar_key")
    .eq("sl_avatar_key", viewerAvatarKey)
    .maybeSingle();

  const viewerRealm = viewer?.realm_index || 1;

  // Load all active cultivators (present within 10 min)
  const { data: members, error } = await supabase
    .from("cultivation_members")
    .select(`
      sl_avatar_key, sl_username, character_name, gender,
      realm_index, realm_key, realm_display_name,
      v2_active_stage_key, v2_cultivation_status,
      current_region_name, auric_current, auric_maximum, vestiges,
      last_presence_at, awakened_by
    `)
    .gte("last_presence_at", new Date(Date.now() - 10 * 60 * 1000).toISOString())
    .order("realm_index", { ascending: false });

  if (error) return json(500, { error: "Failed to load cultivators", detail: error.message });

  const activeMemberKeys = (members || []).map(m => m.sl_avatar_key);

  // Load stats if viewer can see them (tier 7+)
  let statsMap = {};
  if (viewerRealm >= 7 && activeMemberKeys.length) {
    const { data: statsRows } = await supabase
      .from("cultivator_stats")
      .select("sl_avatar_key, vitality, will, resonance, insight")
      .in("sl_avatar_key", activeMemberKeys);
    (statsRows || []).forEach(s => { statsMap[s.sl_avatar_key] = s; });
  }

  // Load breakthrough counts if viewer can see them (tier 6+)
  let breakthroughMap = {};
  if (viewerRealm >= 6 && activeMemberKeys.length) {
    const { data: btRows } = await supabase
      .schema("breakthrough")
      .from("v2_member_breakthrough_state")
      .select("sl_avatar_key")
      .in("sl_avatar_key", activeMemberKeys)
      .eq("lifecycle_status", "success");
    (btRows || []).forEach(b => {
      breakthroughMap[b.sl_avatar_key] = (breakthroughMap[b.sl_avatar_key] || 0) + 1;
    });
  }

  // Load alignment path if viewer can see it (tier 3+)
  let pathMap = {};
  if (viewerRealm >= 3 && activeMemberKeys.length) {
    const { data: pathRows } = await supabase
      .schema("alignment")
      .from("member_path_state")
      .select("member_avatar_key, current_path, first_revealed_at")
      .in("member_avatar_key", activeMemberKeys);
    (pathRows || []).forEach(p => {
      if (p.first_revealed_at) {
        pathMap[p.member_avatar_key] = p.current_path
          ? p.current_path.charAt(0).toUpperCase() + p.current_path.slice(1)
          : "Unaligned";
      } else {
        pathMap[p.member_avatar_key] = "Unaligned";
      }
    });
  }

  // Build filtered response
  const cultivators = (members || []).map(m => {
    m.alignment_path = pathMap[m.sl_avatar_key] || "Unaligned";
    m._is_self       = m.sl_avatar_key === viewerAvatarKey;
    return applyPerceptionFilter(m, viewerRealm, statsMap, breakthroughMap);
  });

  return json(200, {
    success:       true,
    viewer_realm:  viewerRealm,
    active_count:  cultivators.length,
    cultivators,
    perception_tier: viewerRealm,
    perception_label: [
      "", "Name & Gender", "Realm", "Path", "Stage",
      "Bond Status", "Breakthrough History", "Stat Tier",
      "Drift Direction", "Awakening Lineage", "Full Perception"
    ][viewerRealm] || "Name & Gender"
  });
};
