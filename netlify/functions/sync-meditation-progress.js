// sync-meditation-progress.js
// Heartbeat sync for active personal cultivation using v2 system.
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).

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
  if (!avatarKey) {
    return json(401, { error: "Not authenticated" });
  }

  const partnershipUuid = body.partnership_uuid || body.selected_partnership_uuid || null;

  // Load member to check status before syncing
  const { data: member } = await supabase
    .from("cultivation_members")
    .select("v2_cultivation_status, personal_cultivation_preference")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (member && member.v2_cultivation_status === "in_breakthrough") {
    return json(200, {
      success: true,
      synced: false,
      reason: "in_breakthrough",
      message: "Auric and CP gains are suspended during breakthrough. The cultivator faces the tribulation."
    });
  }

  const preference = (member?.personal_cultivation_preference || "manual").toLowerCase();
  const cultivationStatus = member?.v2_cultivation_status || "idle";

  // 'meditating' = base meditation (auric fills freely, scroll is idle).
  // Applies in Manual mode when the player has NOT yet pressed "Resume Cultivation".
  // When the player presses "Resume Cultivation", status becomes 'cultivating'
  // and the sync falls through to run v2_sync_realm_cultivation normally.
  if (cultivationStatus === "meditating") {
    return json(200, {
      success: true,
      synced: false,
      reason: "base_meditation_no_scroll",
      personal_cultivation_status: cultivationStatus,
      v2_cultivation_status: cultivationStatus,
      auric_current: member?.auric_current ?? null,
      cultivation_preference: preference,
      message: "Base meditation active. Auric fills freely. Use Resume Cultivation on the website to advance scroll."
    });
  }

  // v2 cultivation sync (status = 'cultivating' in any mode)
  const { data: syncResult, error: syncError } = await supabase
    .schema("library")
    .rpc("v2_sync_realm_cultivation", { p_sl_avatar_key: avatarKey });

  if (syncError) {
    console.error("v2_sync_realm_cultivation error:", syncError);
    return json(500, { error: "Failed to sync cultivation", detail: syncError.message });
  }

  // Bond state if partnership context provided
  let bondState = null;
  if (partnershipUuid) {
    const { data: memberRow } = await supabase
      .from("cultivation_members")
      .select("member_id")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();

    if (memberRow?.member_id) {
      const { data: bondBook } = await supabase
        .schema("partner")
        .from("partner_bond_member_book_states")
        .select("bond_volume_number, bond_book_number, status, paused_at, started_at, completed_at")
        .eq("partnership_uuid", partnershipUuid)
        .eq("member_id", memberRow.member_id)
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
    personal_cultivation_status: syncResult?.stage_status  || "idle",
    v2_cultivation_status:       syncResult?.stage_status  || "idle",
    accumulated_seconds:         syncResult?.accumulated_seconds || 0,
    required_seconds:            syncResult?.required_seconds   || 0,
    breakthrough_gate_open:      syncResult?.breakthrough_gate_open || false,
    auric_current:               syncResult?.auric_after ?? null,
    ...(bondState || {
      bond_runtime_active: false,
      bond_session_status: "idle"
    }),
    focused_partnership_uuid: partnershipUuid || null
  });
};
