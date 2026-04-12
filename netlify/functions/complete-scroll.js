// complete-scroll.js
// Awards cultivation stats when a scroll section is fully cultivated.
// Called automatically by sync-meditation-progress when stage_complete = true.
// Idempotent — safe to call multiple times, will not double-award same scroll.

const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

// Per-realm stat awards from design doc
const REALM_STATS = {
  1:  { vitality: 5, will: 1, resonance: 1,  insight: 2,  primary: "vitality"  },
  2:  { vitality: 4, will: 2, resonance: 2,  insight: 3,  primary: "insight"   },
  3:  { vitality: 3, will: 3, resonance: 3,  insight: 3,  primary: "will"      },
  4:  { vitality: 3, will: 4, resonance: 3,  insight: 4,  primary: "will"      },
  5:  { vitality: 2, will: 5, resonance: 4,  insight: 4,  primary: "will"      },
  6:  { vitality: 2, will: 5, resonance: 5,  insight: 5,  primary: "resonance" },
  7:  { vitality: 1, will: 4, resonance: 6,  insight: 6,  primary: "resonance" },
  8:  { vitality: 1, will: 4, resonance: 6,  insight: 6,  primary: "resonance" },
  9:  { vitality: 1, will: 3, resonance: 8,  insight: 8,  primary: "resonance" },
  10: { vitality: 1, will: 2, resonance: 10, insight: 10, primary: "resonance" }
};

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
    body: JSON.stringify(body)
  };
}

async function resolveAvatarKey(event, body) {
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  const sessionToken = cookies[COOKIE_NAME] || "";

  if (sessionToken) {
    const { data: sessionRow } = await supabase
      .from("website_sessions")
      .select("sl_avatar_key")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();
    if (sessionRow?.sl_avatar_key) return sessionRow.sl_avatar_key;
  }

  const avatarKey = (body.sl_avatar_key || "").trim();
  if (avatarKey) {
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("sl_avatar_key")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();
    if (member?.sl_avatar_key) return member.sl_avatar_key;
  }

  return null;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const avatarKey = await resolveAvatarKey(event, body);
  if (!avatarKey) return json(401, { error: "Not authenticated" });

  const volumeNumber = parseInt(body.volume_number, 10) || null;
  const sectionKey   = (body.section_key || "").trim().toLowerCase() || null;

  if (!volumeNumber || !sectionKey) {
    return json(400, { error: "volume_number and section_key are required" });
  }

  const { data: member } = await supabase
    .from("cultivation_members")
    .select("sl_avatar_key, sl_username, realm_index")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (!member) return json(404, { error: "Member not found" });

  // Verify scroll is complete
  const { data: stageRow } = await supabase
    .schema("library")
    .from("v2_member_stage_state")
    .select("id, stage_status")
    .eq("sl_avatar_key", avatarKey)
    .eq("volume_number", volumeNumber)
    .eq("section_key", sectionKey)
    .in("stage_status", ["complete", "comprehended"])
    .maybeSingle();

  if (!stageRow) {
    return json(409, { error: "Scroll section is not complete", error_code: "stage_not_complete" });
  }

  const realmIndex = Math.min(Math.max(member.realm_index || 1, 1), 10);
  const scrollKey  = `v${volumeNumber}_${sectionKey}`;
  const baseStats  = REALM_STATS[realmIndex];

  // Idempotency — already awarded?
  const { data: existingLog } = await supabase
    .from("stat_gain_log")
    .select("vitality_gained, will_gained, resonance_gained, insight_gained, variance_applied, variance_stat")
    .eq("sl_avatar_key", avatarKey)
    .eq("scroll_key", scrollKey)
    .maybeSingle();

  if (existingLog) {
    return json(200, {
      success: true,
      action: "already_awarded",
      scroll_key: scrollKey,
      message: "Stats were already awarded for this scroll.",
      stats_gained: {
        vitality:  existingLog.vitality_gained,
        will:      existingLog.will_gained,
        resonance: existingLog.resonance_gained,
        insight:   existingLog.insight_gained
      }
    });
  }

  // Roll variance (+1 to +3) on primary stat
  const variance    = Math.floor(Math.random() * 3) + 1;
  const primaryStat = baseStats.primary;

  const awards = {
    vitality:  baseStats.vitality  + (primaryStat === "vitality"  ? variance : 0),
    will:      baseStats.will      + (primaryStat === "will"       ? variance : 0),
    resonance: baseStats.resonance + (primaryStat === "resonance"  ? variance : 0),
    insight:   baseStats.insight   + (primaryStat === "insight"    ? variance : 0)
  };

  // Read then increment stats
  const { data: existingStats } = await supabase
    .from("cultivator_stats")
    .select("vitality, will, resonance, insight")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  let finalStats;
  if (existingStats) {
    const { data: updated, error: updateErr } = await supabase
      .from("cultivator_stats")
      .update({
        vitality:   (existingStats.vitality  || 0) + awards.vitality,
        will:       (existingStats.will      || 0) + awards.will,
        resonance:  (existingStats.resonance || 0) + awards.resonance,
        insight:    (existingStats.insight   || 0) + awards.insight,
        updated_at: new Date().toISOString()
      })
      .eq("sl_avatar_key", avatarKey)
      .select()
      .maybeSingle();
    if (updateErr) return json(500, { error: "Failed to update stats", detail: updateErr.message });
    finalStats = updated;
  } else {
    const { data: inserted, error: insertErr } = await supabase
      .from("cultivator_stats")
      .insert({ sl_avatar_key: avatarKey, sl_username: member.sl_username || "", ...awards })
      .select()
      .maybeSingle();
    if (insertErr) return json(500, { error: "Failed to create stats", detail: insertErr.message });
    finalStats = inserted;
  }

  // Log — unique constraint prevents double-awarding
  await supabase.from("stat_gain_log").insert({
    sl_avatar_key:    avatarKey,
    sl_username:      member.sl_username || "",
    scroll_key:       scrollKey,
    volume_number:    volumeNumber,
    section_key:      sectionKey,
    realm_index:      realmIndex,
    vitality_gained:  awards.vitality,
    will_gained:      awards.will,
    resonance_gained: awards.resonance,
    insight_gained:   awards.insight,
    variance_applied: variance,
    variance_stat:    primaryStat
  });

  // Notification
  const sectionLabel = { base: "Base", early: "Early", middle: "Middle", late: "Late" }[sectionKey] || sectionKey;
  await supabase.from("member_notifications").insert({
    sl_avatar_key: avatarKey,
    sl_username:   member.sl_username || "",
    type:          "scroll_complete",
    title:         "Scroll Comprehended",
    message:       `The ${sectionLabel} Scroll of Tome ${volumeNumber} has been comprehended. Breakthrough awaits.`,
    is_read:       false,
    metadata: { volume_number: volumeNumber, section_key: sectionKey, scroll_key: scrollKey, realm_index: realmIndex, stats_gained: awards, primary_stat: primaryStat, variance }
  });

  return json(200, {
    success: true,
    action:           "stats_awarded",
    scroll_key:       scrollKey,
    volume_number:    volumeNumber,
    section_key:      sectionKey,
    realm_index:      realmIndex,
    stats_gained:     awards,
    variance_stat:    primaryStat,
    variance_applied: variance,
    total_stats: {
      vitality:  finalStats?.vitality  || 0,
      will:      finalStats?.will      || 0,
      resonance: finalStats?.resonance || 0,
      insight:   finalStats?.insight   || 0
    },
    message: `${sectionLabel} Scroll comprehended. Stats awarded.`
  });
};
