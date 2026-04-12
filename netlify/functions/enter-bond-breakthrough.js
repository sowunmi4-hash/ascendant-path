const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const partnerSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "partner" } }
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "breakthrough" } }
);

const COOKIE_NAME = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

function parseCookies(header = "") {
  const cookies = {};
  header.split(";").forEach((part) => {
    const eq = part.trim().indexOf("=");
    if (eq === -1) return;
    const k = part.trim().slice(0, eq).trim();
    const v = part.trim().slice(eq + 1).trim();
    try { cookies[k] = decodeURIComponent(v); } catch { cookies[k] = v; }
  });
  return cookies;
}

function json(statusCode, body) {
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
  try { return event.body ? JSON.parse(event.body) : {}; } catch { return {}; }
}

function safeText(v, fb = "") {
  const t = String(v ?? "").trim();
  return t || fb;
}

function safeNumber(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}

async function resolveAvatarKey(event, body) {
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (token) {
    const { data } = await supabase
      .from("website_sessions")
      .select("sl_avatar_key")
      .eq("session_token", token)
      .eq("is_active", true)
      .maybeSingle();
    if (data?.sl_avatar_key) return data.sl_avatar_key;
  }
  const key = safeText(body.sl_avatar_key);
  if (key) {
    const { data } = await supabase
      .from("cultivation_members")
      .select("sl_avatar_key")
      .eq("sl_avatar_key", key)
      .maybeSingle();
    if (data?.sl_avatar_key) return data.sl_avatar_key;
  }
  return null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { success: false, message: "Method not allowed. Use POST." });

  try {
    const body = parseBody(event);
    const avatarKey = await resolveAvatarKey(event, body);
    if (!avatarKey) return json(401, { success: false, message: "Not authenticated." });

    const partnershipId = safeText(body.partnership_id || body.partnership_uuid);
    const bondVolumeNumber = safeNumber(body.bond_volume_number || body.volume_number, 0);

    if (!partnershipId) return json(400, { success: false, message: "partnership_id is required." });
    if (!bondVolumeNumber) return json(400, { success: false, message: "bond_volume_number is required." });

    // Verify partnership Chronicle 4 of this volume is completed for this member
    const { data: member } = await supabase
      .from("cultivation_members")
      .select("member_id, sl_username")
      .eq("sl_avatar_key", avatarKey)
      .maybeSingle();

    if (!member) return json(404, { success: false, message: "Member not found." });

    const { data: bookState } = await partnerSupabase
      .from("partner_bond_member_book_states")
      .select("status, bond_book_number, completed_at")
      .eq("partnership_uuid", partnershipId)
      .eq("member_id", member.member_id)
      .eq("bond_volume_number", bondVolumeNumber)
      .eq("bond_book_number", 4) // Chronicle 4 = final book
      .maybeSingle();

    if (!bookState || bookState.status !== "completed") {
      return json(409, {
        success: false,
        error_code: "chronicle_4_not_complete",
        message: "Chronicle 4 of this Relic must be completed before entering the bond breakthrough.",
        bond_volume_number: bondVolumeNumber,
        chronicle_4_status: bookState?.status || "not_found"
      });
    }

    // Call the DB function
    const { data: result, error } = await breakthroughSupabase.rpc(
      "v2_enter_bond_breakthrough",
      {
        p_sl_avatar_key: avatarKey,
        p_partnership_id: partnershipId,
        p_bond_volume_number: bondVolumeNumber
      }
    );

    if (error) {
      console.error("v2_enter_bond_breakthrough error:", error);
      return json(500, { success: false, message: "Bond breakthrough entry failed: " + error.message });
    }

    const payload = Array.isArray(result) ? result[0] : result;

    if (!payload?.success) {
      const statusMap = {
        member_not_found: 404,
        partnership_not_found: 404,
        not_in_partnership: 403,
        breakthrough_active: 409,
        already_active: 409,
        already_entered: 409
      };
      return json(statusMap[payload?.error_code] || 400, {
        success: false,
        error_code: safeText(payload?.error_code),
        message: safeText(payload?.message, "Bond breakthrough entry failed.")
      });
    }

    // Fire notification
    try {
      const isFirst = payload.is_first_to_enter;
      await supabase.from("member_notifications").insert({
        sl_avatar_key: avatarKey,
        sl_username: member.sl_username || "",
        type: "bond_breakthrough_entered",
        title: "Bond Tribulation — The Heavens Stir",
        message: isFirst
          ? "You have entered the Bond Tribulation. Await your partner — both must face this trial together."
          : "Both cultivators have entered the Bond Tribulation. The countdown begins.",
        is_read: false,
        metadata: {
          partnership_id: partnershipId,
          bond_volume_number: bondVolumeNumber,
          breakthrough_state_id: payload.breakthrough_state_id
        }
      });
    } catch (notifErr) {
      console.error("Bond breakthrough notification error:", notifErr);
    }

    return json(200, {
      success: true,
      message: safeText(payload.message),
      breakthrough_state_id: payload.breakthrough_state_id,
      bond_breakthrough_id: payload.bond_breakthrough_id,
      lifecycle_status: "countdown",
      countdown_ends_at: payload.countdown_ends_at,
      target_type: "bond",
      role: payload.role,
      is_first_to_enter: payload.is_first_to_enter,
      bond_volume_number: bondVolumeNumber,
      partnership_id: partnershipId,
      partner_avatar_key: payload.partner_avatar_key
    });

  } catch (err) {
    console.error("enter-bond-breakthrough error:", err);
    return json(500, { success: false, message: "Unexpected error.", error: err.message });
  }
};
