// cultivation/complete-scroll.js
// Awards cultivation stats when a scroll section (stage) is completed.
// Called by the UI after sync returns stage_complete = true.
//
// Awards:
//   - Base stat (all four: vitality, will, resonance, insight):
//       ceil(realm_index / 2) per stat  →  realm 1-2 = 1, 3-4 = 2, 5-6 = 3, 7-8 = 4, 9-10 = 5
//   - Section bonus: 'late' section adds +1 to all stats
//   - Variance: one random primary stat gets +1 to +3 extra
//
// Writes:
//   - UPSERT public.cultivator_stats (increment totals)
//   - INSERT public.stat_gain_log
//   - INSERT public.member_notifications
//
// Dual auth: ap_session cookie OR sl_avatar_key in body.
// Body params: { volume_number, section_key }  (from sync response)

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

const STAT_NAMES = ["vitality", "will", "resonance", "insight"];

function calculateStatAwards(realmIndex, sectionKey) {
  const baseStat     = Math.ceil(realmIndex / 2);
  const sectionBonus = sectionKey === "late" ? 1 : 0;

  // One random stat gets variance (+1 to +3)
  const primaryStatIdx = Math.floor(Math.random() * 4);
  const primaryStat    = STAT_NAMES[primaryStatIdx];
  const variance       = Math.floor(Math.random() * 3) + 1;   // 1, 2, or 3

  const awards = {};
  for (const stat of STAT_NAMES) {
    awards[stat] = baseStat + sectionBonus + (stat === primaryStat ? variance : 0);
  }

  return { awards, primaryStat, variance };
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
  const sectionKey   = (body.section_key || "").trim() || null;

  if (!volumeNumber || !sectionKey) {
    return json(400, { error: "volume_number and section_key are required" });
  }

  // Load member
  const { data: member } = await supabase
    .from("cultivation_members")
    .select("sl_avatar_key, sl_username, realm_index, v2_cultivation_status")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (!member) return json(404, { error: "Member not found" });

  // Verify scroll is actually complete (status must be breakthrough_ready or cultivating with complete stage)
  const { data: stageRow } = await supabase
    .schema("library")
    .from("v2_member_stage_state")
    .select("id, stage_status, accumulated_seconds, required_seconds")
    .eq("sl_avatar_key", avatarKey)
    .eq("volume_number", volumeNumber)
    .eq("section_key", sectionKey)
    .in("stage_status", ["complete"])
    .maybeSingle();

  if (!stageRow) {
    return json(409, {
      error: "Scroll section is not complete",
      error_code: "stage_not_complete",
      message: "Stats can only be awarded when the scroll section has been fully cultivated."
    });
  }

  const realmIndex = member.realm_index || 1;
  const { awards, primaryStat, variance } = calculateStatAwards(realmIndex, sectionKey);

  const scrollKey = `v${volumeNumber}_${sectionKey}`;

  // 1. Upsert cultivator_stats — increment each stat
  const { data: upsertResult, error: upsertErr } = await supabase
    .from("cultivator_stats")
    .upsert(
      {
        sl_avatar_key: avatarKey,
        sl_username:   member.sl_username || "",
        vitality:      awards.vitality,
        will:          awards.will,
        resonance:     awards.resonance,
        insight:       awards.insight,
        updated_at:    new Date().toISOString()
      },
      {
        onConflict: "sl_avatar_key",
        ignoreDuplicates: false
      }
    )
    .select()
    .maybeSingle();

  // If upsert doesn't merge (Supabase upsert replaces, not increments), do it manually
  if (upsertErr) {
    console.error("cultivator_stats upsert error:", upsertErr);
  }

  // Use raw increment via rpc or manual read-then-write approach
  const { data: existingStats } = await supabase
    .from("cultivator_stats")
    .select("vitality, will, resonance, insight")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  let finalStats;
  if (existingStats) {
    // Increment
    const updated = {
      vitality:  (existingStats.vitality  || 0) + awards.vitality,
      will:      (existingStats.will      || 0) + awards.will,
      resonance: (existingStats.resonance || 0) + awards.resonance,
      insight:   (existingStats.insight   || 0) + awards.insight,
      updated_at: new Date().toISOString()
    };
    const { data: incrementResult, error: incErr } = await supabase
      .from("cultivator_stats")
      .update(updated)
      .eq("sl_avatar_key", avatarKey)
      .select()
      .maybeSingle();
    if (incErr) {
      console.error("cultivator_stats increment error:", incErr);
      return json(500, { error: "Failed to update stats", detail: incErr.message });
    }
    finalStats = incrementResult;
  } else {
    // First time — insert
    const { data: insertResult, error: insErr } = await supabase
      .from("cultivator_stats")
      .insert({
        sl_avatar_key: avatarKey,
        sl_username:   member.sl_username || "",
        ...awards,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .select()
      .maybeSingle();
    if (insErr) {
      console.error("cultivator_stats insert error:", insErr);
      return json(500, { error: "Failed to create stats record", detail: insErr.message });
    }
    finalStats = insertResult;
  }

  // 2. Log the stat gain
  await supabase
    .from("stat_gain_log")
    .insert({
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

  // 3. Create notification
  const sectionLabels = { base: "Base", early: "Early", middle: "Middle", late: "Late" };
  const sectionLabel  = sectionLabels[sectionKey] || sectionKey;
  await supabase
    .from("member_notifications")
    .insert({
      sl_avatar_key: avatarKey,
      sl_username:   member.sl_username || "",
      type:          "stat_gain",
      title:         "Scroll Comprehended",
      message:       `You have comprehended the ${sectionLabel} stage of Scroll ${volumeNumber}. Your cultivation grows stronger.`,
      is_read:       false,
      metadata: {
        volume_number: volumeNumber,
        section_key:   sectionKey,
        scroll_key:    scrollKey,
        realm_index:   realmIndex,
        stats_gained:  awards,
        primary_stat:  primaryStat,
        variance:      variance
      }
    });

  return json(200, {
    success: true,
    action: "stats_awarded",
    scroll_key:       scrollKey,
    volume_number:    volumeNumber,
    section_key:      sectionKey,
    realm_index:      realmIndex,
    stats_gained: {
      vitality:  awards.vitality,
      will:      awards.will,
      resonance: awards.resonance,
      insight:   awards.insight
    },
    variance_stat:    primaryStat,
    variance_applied: variance,
    total_stats: {
      vitality:  finalStats?.vitality  || 0,
      will:      finalStats?.will      || 0,
      resonance: finalStats?.resonance || 0,
      insight:   finalStats?.insight   || 0
    },
    message: `Scroll ${sectionLabel} comprehended. Stats awarded and logged.`
  });
};
