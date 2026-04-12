const { createClient } = require("@supabase/supabase-js");

// ---------------------------------------------------------------------------
// Supabase clients
// ---------------------------------------------------------------------------

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "breakthrough" } }
);

const alignmentSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "alignment" } }
);

// ---------------------------------------------------------------------------
// Dice mechanic constants
// ---------------------------------------------------------------------------

const HEAVEN_POWER = {
  1: 20, 2: 40, 3: 70, 4: 110, 5: 160,
  6: 220, 7: 290, 8: 370, 9: 460, 10: 600
};

// Bond breakthroughs: heaven power is doubled (harder threshold, but combined stats)
const BOND_HEAVEN_POWER_MULTIPLIER = 2.0;

const ROLL_MODIFIERS = {
  1: 0.5, 2: 0.75, 3: 1.0, 4: 1.0, 5: 1.5, 6: 2.0
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function parseBody(event) {
  try { return event.body ? JSON.parse(event.body) : {}; } catch (e) { return {}; }
}

function safeText(value, fallback) {
  if (fallback === undefined) fallback = "";
  const text = String(value !== null && value !== undefined ? value : "").trim();
  return text || fallback;
}

function safeNumber(value, fallback) {
  if (fallback === undefined) fallback = 0;
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  cookieHeader.split(";").forEach(function(part) {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const k = trimmed.slice(0, eq).trim();
    const v = trimmed.slice(eq + 1).trim();
    try { cookies[k] = decodeURIComponent(v); } catch(e) { cookies[k] = v; }
  });
  return cookies;
}

// ---------------------------------------------------------------------------
// Alignment: determine isResonant from current_path vs current SLT hour_group
// Returns false silently if any lookup fails (non-fatal)
// ---------------------------------------------------------------------------

async function resolveIsResonant(slAvatarKey) {
  try {
    // 1) Member's current path
    const { data: pathState } = await alignmentSupabase
      .from("member_path_state")
      .select("current_path")
      .eq("member_avatar_key", slAvatarKey)
      .maybeSingle();

    const currentPath = (pathState && pathState.current_path)
      ? pathState.current_path.toLowerCase().trim()
      : "unaligned";

    if (currentPath === "unaligned") return false;

    // 2) Current SLT hour_group via RPC
    const { data: hourRows } = await alignmentSupabase.rpc("resolve_current_path_hour");
    const hourGroup = (hourRows && hourRows.length > 0 && hourRows[0].hour_group)
      ? hourRows[0].hour_group
      : null;

    if (!hourGroup) return false;

    // 3) Look up state_label in path_hour_reward_matrix
    const { data: matrix } = await alignmentSupabase
      .from("path_hour_reward_matrix")
      .select("state_label")
      .eq("path_key", currentPath)
      .eq("hour_group", hourGroup)
      .maybeSingle();

    const stateLabel = matrix && matrix.state_label ? matrix.state_label : "";
    return stateLabel === "Resonant" || stateLabel === "Harmonized";

  } catch (err) {
    console.warn("resolveIsResonant failed (non-fatal):", err.message);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return buildResponse(200, { ok: true });
  if (event.httpMethod !== "POST") {
    return buildResponse(405, { success: false, message: "Method not allowed. Use POST." });
  }

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, { success: false, message: "Missing Supabase environment variables." });
    }

    // -----------------------------------------------------------------------
    // AUTH: ap_session cookie → sl_avatar_key, fallback to body sl_avatar_key
    // -----------------------------------------------------------------------

    const cookieHeader = (event.headers && event.headers.cookie)
      ? event.headers.cookie
      : ((event.headers && event.headers.Cookie) ? event.headers.Cookie : "");
    const cookies = parseCookies(cookieHeader);
    const sessionToken = cookies.ap_session || "";
    let sessionAvatarKey = "";

    if (sessionToken) {
      const sessionResult = await publicSupabase
        .from("website_sessions")
        .select("sl_avatar_key")
        .eq("session_token", sessionToken)
        .eq("is_active", true)
        .maybeSingle();
      sessionAvatarKey = (sessionResult.data && sessionResult.data.sl_avatar_key)
        ? sessionResult.data.sl_avatar_key : "";
    }

    const body = parseBody(event);
    const slAvatarKey = sessionAvatarKey || safeText(body.sl_avatar_key);

    if (!slAvatarKey) {
      return buildResponse(400, {
        success: false,
        error_code: "avatar_key_required",
        message: "sl_avatar_key is required for battle resolution."
      });
    }

    // -----------------------------------------------------------------------
    // 1) Read cultivator_stats — default to 0 for new cultivators
    // -----------------------------------------------------------------------

    const { data: stats } = await publicSupabase
      .from("cultivator_stats")
      .select("vitality, will, resonance, insight")
      .eq("sl_avatar_key", slAvatarKey)
      .maybeSingle();

    const vitality  = safeNumber(stats && stats.vitality,  0);
    const will      = safeNumber(stats && stats.will,      0);
    const resonance = safeNumber(stats && stats.resonance, 0);
    const insight   = safeNumber(stats && stats.insight,   0);

    // -----------------------------------------------------------------------
    // 2) Read realm_index from cultivation_members
    // -----------------------------------------------------------------------

    const { data: member, error: memberErr } = await publicSupabase
      .from("cultivation_members")
      .select("member_id, sl_avatar_key, sl_username, realm_index")
      .eq("sl_avatar_key", slAvatarKey)
      .maybeSingle();

    if (memberErr || !member) {
      return buildResponse(404, {
        success: false,
        error_code: "member_not_found",
        message: "Cultivation member not found."
      });
    }

    const realmIndex = safeNumber(member.realm_index, 1);
    const clampedRealm = Math.min(Math.max(realmIndex, 1), 10);

    // -----------------------------------------------------------------------
    // 2b) Check if this is a bond breakthrough — if so, load partner stats
    // -----------------------------------------------------------------------

    // Load active breakthrough to check target_type and partnership_id
    const { data: activeBt } = await breakthroughSupabase
      .from("v2_member_breakthrough_state")
      .select("id, target_type, partnership_id, bond_volume_number")
      .eq("sl_avatar_key", slAvatarKey)
      .in("lifecycle_status", ["active", "timer_elapsed"])
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    const isBondBreakthrough = activeBt?.target_type === "bond";
    const partnershipId = activeBt?.partnership_id || null;

    let partnerVitality = 0, partnerWill = 0, partnerResonance = 0, partnerInsight = 0;
    let partnerAvatarKey = null;
    let partnerMember = null;

    if (isBondBreakthrough && partnershipId) {
      // Fetch partnership to find the partner
      const { data: partnership } = await publicSupabase
        .schema("partner")
        .from("cultivation_partnerships")
        .select("requester_avatar_key, recipient_avatar_key")
        .eq("id", partnershipId)
        .maybeSingle();

      if (partnership) {
        const reqKey = safeText(partnership.requester_avatar_key).toLowerCase();
        const recKey = safeText(partnership.recipient_avatar_key).toLowerCase();
        partnerAvatarKey = reqKey === slAvatarKey.toLowerCase() ? partnership.recipient_avatar_key : partnership.requester_avatar_key;
      }

      if (partnerAvatarKey) {
        const { data: partnerStats } = await publicSupabase
          .from("cultivator_stats")
          .select("vitality, will, resonance, insight")
          .eq("sl_avatar_key", partnerAvatarKey)
          .maybeSingle();

        partnerVitality  = safeNumber(partnerStats?.vitality,  0);
        partnerWill      = safeNumber(partnerStats?.will,      0);
        partnerResonance = safeNumber(partnerStats?.resonance, 0);
        partnerInsight   = safeNumber(partnerStats?.insight,   0);

        const { data: pm } = await publicSupabase
          .from("cultivation_members")
          .select("member_id, sl_avatar_key, sl_username, realm_index")
          .eq("sl_avatar_key", partnerAvatarKey)
          .maybeSingle();
        partnerMember = pm || null;
      }
    }

    // Combined stats: self + partner (partner is 0 for solo breakthroughs)
    const combinedVitality  = vitality  + partnerVitality;
    const combinedWill      = will      + partnerWill;
    const combinedResonance = resonance + partnerResonance;
    const combinedInsight   = insight   + partnerInsight;

    // Heaven power: doubled for bond breakthroughs
    const heavenPow = HEAVEN_POWER[clampedRealm] * (isBondBreakthrough ? BOND_HEAVEN_POWER_MULTIPLIER : 1.0);

    // -----------------------------------------------------------------------
    // 3) Determine isResonant from alignment state
    // -----------------------------------------------------------------------

    const isResonant = await resolveIsResonant(slAvatarKey);

    // -----------------------------------------------------------------------
    // 4) Stat-based dice mechanic (combined stats for bond breakthroughs)
    // -----------------------------------------------------------------------

    const basePower    = (combinedVitality * 0.20) + (combinedWill * 0.30) + (combinedResonance * 0.25) + (combinedInsight * 0.25);
    const roll         = Math.floor(Math.random() * 6) + 1;
    const rollModifier = ROLL_MODIFIERS[roll];
    const finalPower   = basePower * rollModifier;
    const alignmentBonus = isResonant ? 1.10 : 1.0;
    const adjustedPower  = finalPower * alignmentBonus;

    let outcome  = "success";
    let severity = null;

    if (adjustedPower >= heavenPow) {
      outcome  = "success";
      severity = null;
    } else {
      outcome        = "failure";
      const gap      = heavenPow - adjustedPower;
      if      (gap <= heavenPow * 0.20) severity = "stable";
      else if (gap <= heavenPow * 0.50) severity = "minor";
      else                              severity = "severe";
    }

    // -----------------------------------------------------------------------
    // 5) Persist via breakthrough.v2_resolve_breakthrough_battle_stat RPC
    // -----------------------------------------------------------------------

    const { data: rpcResult, error: rpcErr } = await breakthroughSupabase.rpc(
      "v2_resolve_breakthrough_battle_stat",
      {
        p_sl_avatar_key: slAvatarKey,
        p_outcome:       outcome,
        p_severity:      severity,
        p_battle_roll:   roll,
        p_battle_power:  Math.round(adjustedPower),
        p_heaven_power:  heavenPow
      }
    );

    if (rpcErr) {
      console.error("v2_resolve_breakthrough_battle_stat RPC error:", rpcErr);
      return buildResponse(500, {
        success: false,
        error_code: "rpc_error",
        message: "Battle resolution RPC failed: " + rpcErr.message
      });
    }

    const result = Array.isArray(rpcResult) ? rpcResult[0] : rpcResult;

    if (!result || !result.success) {
      const errorCode = safeText(result && result.error_code);
      const message   = safeText(result && result.message, "Battle resolution failed.");
      const statusMap = {
        member_not_found:           404,
        no_resolvable_breakthrough: 409,
        timer_not_elapsed:          409,
        unexpected_error:           500
      };
      return buildResponse(statusMap[errorCode] || 400, {
        success:    false,
        error_code: errorCode,
        message:    message,
        ...(result && result.ends_at          ? { ends_at:           result.ends_at }          : {}),
        ...(result && result.seconds_remaining !== undefined ? { seconds_remaining: result.seconds_remaining } : {})
      });
    }

    // -----------------------------------------------------------------------
    // 6) Return success — outcome stays hidden until v2_reveal_verdict
    // -----------------------------------------------------------------------

    return buildResponse(200, {
      success:               true,
      message:               safeText(result.message) || "Battle resolved. Awaiting verdict.",
      breakthrough_state_id: safeText(result.breakthrough_state_id) || null,
      lifecycle_status:      safeText(result.lifecycle_status, "battle_resolved"),
      previous_lifecycle:    safeText(result.previous_lifecycle) || null,
      battle_status:         "resolved",
      battle_resolved_at:    result.battle_resolved_at || null,
      tribulation_power:     safeNumber(result.tribulation_power, heavenPow),
      battle_roll:           safeNumber(result.battle_roll, roll),
      battle_power:          safeNumber(result.battle_power, Math.round(adjustedPower)),
      heavens_forgiveness:   !!(result.heavens_forgiveness),
      outcome_hidden:        true,
      is_bond_breakthrough:  isBondBreakthrough,
      dice: {
        roll:             roll,
        roll_modifier:    rollModifier,
        base_power:       Math.round(basePower * 100) / 100,
        adjusted_power:   Math.round(adjustedPower * 100) / 100,
        heaven_power:     heavenPow,
        realm_index:      clampedRealm,
        is_resonant:      isResonant,
        alignment_bonus:  alignmentBonus,
        is_bond:          isBondBreakthrough,
        bond_power_multiplier: isBondBreakthrough ? BOND_HEAVEN_POWER_MULTIPLIER : 1.0
      },
      self_stats: {
        vitality, will, resonance, insight
      },
      ...(isBondBreakthrough ? {
        partner_stats: {
          vitality: partnerVitality, will: partnerWill,
          resonance: partnerResonance, insight: partnerInsight
        },
        combined_stats: {
          vitality: combinedVitality, will: combinedWill,
          resonance: combinedResonance, insight: combinedInsight
        },
        partner_avatar_key: partnerAvatarKey
      } : {}),
      member: {
        member_id:     safeText(member.member_id) || null,
        sl_avatar_key: safeText(member.sl_avatar_key),
        sl_username:   safeText(member.sl_username)
      }
    });

  } catch (error) {
    console.error("resolve-breakthrough-battle error:", error);
    return buildResponse(500, {
      success: false,
      message: "Failed to resolve breakthrough battle.",
      error:   error.message || "Unknown error."
    });
  }
};
