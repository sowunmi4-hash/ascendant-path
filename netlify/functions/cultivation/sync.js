// cultivation/sync.js
// HUD heartbeat sync — Phase 2 rewrite.
// Handles three cultivation states:
//   'meditating'        → fill auric + vestiges from progression table
//   'cultivating'       → call v2_sync_realm_cultivation (scroll advance + drift debt)
//   'breakthrough_ready'→ call v2_sync_realm_cultivation (stage stays 'complete', gate open)
//   'in_breakthrough'   → suspended, return notice
//   anything else       → not active
// Dual auth: ap_session cookie OR sl_avatar_key in body.

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

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const avatarKey = await resolveAvatarKey(event, body);
  if (!avatarKey) return json(401, { error: "Not authenticated" });

  // Load member state
  const { data: member } = await supabase
    .from("cultivation_members")
    .select(
      "v2_cultivation_status, auric_current, auric_maximum, vestiges, " +
      "realm_index, v2_active_stage_key, last_hud_sync_at, drift_debt, " +
      "personal_cultivation_preference, cultivation_mode"
    )
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (!member) return json(404, { error: "Member not found" });

  const status = member.v2_cultivation_status || "idle";

  // ── In breakthrough: all gains suspended ────────────────────────────────
  if (status === "in_breakthrough") {
    return json(200, {
      success: true,
      synced: false,
      reason: "in_breakthrough",
      v2_cultivation_status: status,
      message: "Auric and CP gains are suspended during breakthrough. The cultivator faces the tribulation."
    });
  }

  // ── Not active ───────────────────────────────────────────────────────────
  if (!["meditating", "cultivating", "breakthrough_ready"].includes(status)) {
    return json(200, {
      success: true,
      synced: false,
      reason: "not_active",
      v2_cultivation_status: status,
      auric_current: member.auric_current
    });
  }

  // ── MEDITATING: fill auric + vestiges, no scroll advance ─────────────────
  if (status === "meditating") {
    const realmOrder = member.realm_index || 1;
    const stageKey   = member.v2_active_stage_key || "base";

    const { data: prog } = await supabase
      .from("cultivation_realm_stage_progression")
      .select("normal_gain_per_minute, normal_vestiges_gain_per_minute, auric_maximum, vestiges_maximum")
      .eq("realm_order", realmOrder)
      .eq("realm_stage_key", stageKey)
      .maybeSingle();

    const fillRate         = prog?.normal_gain_per_minute          || 2;
    const vestigesFillRate = prog?.normal_vestiges_gain_per_minute || 1;
    const auricMax         = prog?.auric_maximum    || member.auric_maximum || 100;
    const vestigesMax      = prog?.vestiges_maximum || 1000;

    const now      = new Date();
    const lastSync = member.last_hud_sync_at ? new Date(member.last_hud_sync_at) : null;

    // If no anchor yet, stamp now and return — next call will have elapsed time
    if (!lastSync) {
      await supabase
        .from("cultivation_members")
        .update({ last_hud_sync_at: now.toISOString() })
        .eq("sl_avatar_key", avatarKey);
      return json(200, {
        success: true,
        synced: false,
        reason: "anchor_set",
        v2_cultivation_status: status,
        auric_current: member.auric_current,
        vestiges: member.vestiges || 0,
        message: "Sync anchor set. Gains will accumulate from next heartbeat."
      });
    }

    const elapsedSecs = (now - lastSync) / 1000;
    const fullMinutes = Math.floor(elapsedSecs / 60);
    const leftoverSecs = Math.floor(elapsedSecs % 60);

    if (fullMinutes <= 0) {
      return json(200, {
        success: true,
        synced: false,
        reason: "less_than_one_full_minute",
        v2_cultivation_status: status,
        auric_current: member.auric_current,
        vestiges: member.vestiges || 0,
        elapsed_seconds: Math.floor(elapsedSecs),
        leftover_seconds: leftoverSecs
      });
    }

    const auricGain    = fullMinutes * fillRate;
    const vestigesGain = fullMinutes * vestigesFillRate;
    const newAuric     = Math.min((member.auric_current || 0) + auricGain, auricMax);
    const newVestiges  = Math.min((member.vestiges || 0) + vestigesGain, vestigesMax);
    // Advance anchor by full minutes only — leftover carries to next sync
    const newAnchor = new Date(lastSync.getTime() + fullMinutes * 60 * 1000);

    const { error: updateErr } = await supabase
      .from("cultivation_members")
      .update({
        auric_current:    newAuric,
        vestiges:         newVestiges,
        last_hud_sync_at: newAnchor.toISOString(),
        updated_at:       now.toISOString()
      })
      .eq("sl_avatar_key", avatarKey);

    if (updateErr) {
      console.error("Meditation sync update error:", updateErr);
      return json(500, { error: "Failed to sync meditation", detail: updateErr.message });
    }

    return json(200, {
      success: true,
      synced: true,
      action: "meditation_sync",
      v2_cultivation_status: status,
      minutes_processed:            fullMinutes,
      auric_before:                 member.auric_current,
      auric_gained:                 newAuric - (member.auric_current || 0),
      auric_after:                  newAuric,
      auric_maximum:                auricMax,
      vestiges_before:              member.vestiges || 0,
      vestiges_gained:              newVestiges - (member.vestiges || 0),
      vestiges_after:               newVestiges,
      vestiges_maximum:             vestigesMax,
      fill_rate_per_minute:         fillRate,
      vestiges_fill_rate_per_minute: vestigesFillRate,
      leftover_seconds:             leftoverSecs
    });
  }

  // ── CULTIVATING / BREAKTHROUGH_READY: scroll advance via RPC ─────────────
  const { data: syncResult, error: syncError } = await supabase
    .schema("library")
    .rpc("v2_sync_realm_cultivation", { p_sl_avatar_key: avatarKey });

  if (syncError) {
    console.error("v2_sync_realm_cultivation error:", syncError);
    return json(500, { error: "Failed to sync cultivation", detail: syncError.message });
  }

  const { data: freshMember } = await supabase
    .from("cultivation_members")
    .select("auric_current, vestiges, v2_cultivation_status")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  // Bond state if partnership context provided
  const partnershipUuid = body.partnership_uuid || body.selected_partnership_uuid || null;
  let bondState = null;
  if (partnershipUuid) {
    const { data: mRow } = await supabase
      .from("cultivation_members")
      .select("member_id")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();
    if (mRow?.member_id) {
      const { data: bondBook } = await supabase
        .schema("partner")
        .from("partner_bond_member_book_states")
        .select("bond_volume_number, bond_book_number, status, paused_at, started_at, completed_at")
        .eq("partnership_uuid", partnershipUuid)
        .eq("member_id", mRow.member_id)
        .is("completed_at", null)
        .not("started_at", "is", null)
        .order("bond_volume_number", { ascending: false })
        .order("bond_book_number", { ascending: false })
        .limit(1)
        .maybeSingle();
      if (bondBook) {
        const sessionStatus = bondBook.paused_at ? "paused" :
          bondBook.status === "available" ? "active" : "idle";
        bondState = {
          bond_runtime_active: ["active", "paused", "waiting_for_partner_start"].includes(sessionStatus),
          bond_session_status: sessionStatus,
          bond_volume_number:  bondBook.bond_volume_number,
          bond_book_number:    bondBook.bond_book_number
        };
      }
    }
  }

  return json(200, {
    success: true,
    cultivation_sync:            syncResult,
    personal_cultivation_status: freshMember?.v2_cultivation_status || syncResult?.stage_status || "idle",
    v2_cultivation_status:       freshMember?.v2_cultivation_status || syncResult?.stage_status || "idle",
    accumulated_seconds:         syncResult?.accumulated_seconds || 0,
    required_seconds:            syncResult?.required_seconds   || 0,
    breakthrough_gate_open:      syncResult?.breakthrough_gate_open || false,
    auric_current:               freshMember?.auric_current ?? null,
    ...(bondState || { bond_runtime_active: false, bond_session_status: "idle" }),
    focused_partnership_uuid: partnershipUuid || null
  });
};
