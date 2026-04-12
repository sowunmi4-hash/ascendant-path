// soul-sense-activate.js
// HUD-facing Soul Sense — deducts auric cost then returns cultivator list.
// Blocks if auric = 0 or insufficient.
// Auth: sl_avatar_key in body (HUD call — no cookie)

const { createClient } = require("@supabase/supabase-js");
const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

const SOUL_SENSE_COSTS = {
  1:5, 2:8, 3:12, 4:18, 5:25, 6:35, 7:48, 8:65, 9:85, 10:110
};

function json(s, b) {
  return { statusCode: s, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }, body: JSON.stringify(b) };
}

function statTier(total) {
  if (!total || total < 50)  return "weak";
  if (total < 150)           return "moderate";
  if (total < 300)           return "strong";
  return "transcendent";
}

function applyPerceptionFilter(member, viewerRealm, statsMap, breakthroughMap, pathMap) {
  const tier = Math.min(Math.max(viewerRealm || 1, 1), 10);
  const result = {
    sl_username: member.character_name || member.sl_username || "Unknown Cultivator",
    gender:      member.gender || "male",
    is_self:     member._is_self || false,
    realm_index: member.realm_index
  };
  if (tier >= 2)  result.realm_display_name   = member.realm_display_name || member.realm_key || "Mortal";
  if (tier >= 3)  result.alignment_path        = pathMap[member.sl_avatar_key] || "Unaligned";
  if (tier >= 4) {
    const parts  = (member.v2_active_stage_key || "").split(":");
    result.stage = parts[1] ? parts[1].charAt(0).toUpperCase() + parts[1].slice(1) : "Base";
    result.cultivation_status = member.v2_cultivation_status || "idle";
  }
  if (tier >= 5) { result.has_bond = member.has_bond || false; result.bond_status = member.bond_status || "none"; }
  if (tier >= 6)  result.total_breakthroughs   = breakthroughMap[member.sl_avatar_key] || 0;
  if (tier >= 7 && statsMap[member.sl_avatar_key]) {
    const s = statsMap[member.sl_avatar_key];
    const total = (s.vitality||0)+(s.will||0)+(s.resonance||0)+(s.insight||0);
    result.stat_tier = statTier(total); result.stat_total = total;
  }
  if (tier >= 8)  result.drift_direction       = member.drift_direction || null;
  if (tier >= 9)  result.awakened_by           = member.awakened_by || null;
  if (tier >= 10) {
    result.region       = member.current_region_name || null;
    result.auric_current = member.auric_current || 0;
    result.auric_maximum = member.auric_maximum || 0;
    result.vestiges      = member.vestiges      || 0;
  }
  return result;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body = {}; try { body = JSON.parse(event.body || "{}"); } catch {}
  const avatarKey = (body.sl_avatar_key || "").trim();
  if (!avatarKey) return json(401, { error: "sl_avatar_key required" });

  // Load viewer
  const { data: viewer } = await supabase
    .from("cultivation_members")
    .select("sl_avatar_key, sl_username, realm_index, auric_current, auric_maximum")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (!viewer) return json(404, { error: "Member not found" });

  const realmIndex  = Math.min(Math.max(viewer.realm_index || 1, 1), 10);
  const auricCost   = SOUL_SENSE_COSTS[realmIndex] || 5;
  const auricCurrent = viewer.auric_current || 0;

  // Block if no auric
  if (auricCurrent <= 0) {
    return json(402, {
      success:    false,
      error_code: "no_auric",
      message:    "Your auric is depleted. Meditate to restore it before using Soul Sense.",
      auric_current: auricCurrent,
      auric_cost:    auricCost
    });
  }

  // Block if insufficient auric
  if (auricCurrent < auricCost) {
    return json(402, {
      success:    false,
      error_code: "insufficient_auric",
      message:    `Soul Sense requires ${auricCost} auric. You have ${auricCurrent}. Meditate to restore.`,
      auric_current: auricCurrent,
      auric_cost:    auricCost
    });
  }

  // Deduct auric
  const auricAfter = auricCurrent - auricCost;
  await supabase
    .from("cultivation_members")
    .update({ auric_current: auricAfter })
    .eq("sl_avatar_key", avatarKey);

  // Load active cultivators (present within 10 min)
  const { data: members } = await supabase
    .from("cultivation_members")
    .select(`sl_avatar_key, sl_username, character_name, gender, realm_index, realm_key,
             realm_display_name, v2_active_stage_key, v2_cultivation_status,
             current_region_name, auric_current, auric_maximum, vestiges,
             last_presence_at, awakened_by`)
    .gte("last_presence_at", new Date(Date.now() - 10 * 60 * 1000).toISOString())
    .order("realm_index", { ascending: false });

  const activeMemberKeys = (members || []).map(m => m.sl_avatar_key);

  // Load supporting data based on tier
  let statsMap = {}, breakthroughMap = {}, pathMap = {};

  if (realmIndex >= 7 && activeMemberKeys.length) {
    const { data: statsRows } = await supabase
      .from("cultivator_stats").select("sl_avatar_key,vitality,will,resonance,insight")
      .in("sl_avatar_key", activeMemberKeys);
    (statsRows || []).forEach(s => { statsMap[s.sl_avatar_key] = s; });
  }
  if (realmIndex >= 6 && activeMemberKeys.length) {
    const { data: btRows } = await supabase.schema("breakthrough")
      .from("v2_member_breakthrough_state").select("sl_avatar_key")
      .in("sl_avatar_key", activeMemberKeys).eq("lifecycle_status", "success");
    (btRows || []).forEach(b => { breakthroughMap[b.sl_avatar_key] = (breakthroughMap[b.sl_avatar_key]||0)+1; });
  }
  if (realmIndex >= 3 && activeMemberKeys.length) {
    const { data: pathRows } = await supabase.schema("alignment")
      .from("member_path_state").select("member_avatar_key,current_path,first_revealed_at")
      .in("member_avatar_key", activeMemberKeys);
    (pathRows || []).forEach(p => {
      pathMap[p.member_avatar_key] = p.first_revealed_at
        ? (p.current_path ? p.current_path.charAt(0).toUpperCase()+p.current_path.slice(1) : "Unaligned")
        : "Unaligned";
    });
  }

  const cultivators = (members || []).map(m => {
    m._is_self = m.sl_avatar_key === avatarKey;
    return applyPerceptionFilter(m, realmIndex, statsMap, breakthroughMap, pathMap);
  });

  // Log usage
  await supabase.from("soul_sense_log").insert({
    sl_avatar_key:    avatarKey,
    sl_username:      viewer.sl_username || "",
    realm_index:      realmIndex,
    auric_cost:       auricCost,
    auric_before:     auricCurrent,
    auric_after:      auricAfter,
    cultivators_seen: cultivators.length
  });

  return json(200, {
    success:          true,
    auric_cost:       auricCost,
    auric_before:     auricCurrent,
    auric_after:      auricAfter,
    viewer_realm:     realmIndex,
    perception_tier:  realmIndex,
    perception_label: ["","Name & Gender","Realm","Path","Stage","Bond Status",
      "Breakthrough History","Stat Tier","Drift Direction","Awakening Lineage","Full Perception"][realmIndex],
    active_count:     cultivators.length,
    cultivators
  });
};
