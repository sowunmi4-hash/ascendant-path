// sync-clan-location.js
// Upserts the clan's primary location from an in-world object (HUD/LSL caller).
// Dual auth: ap_session cookie (website) OR sl_avatar_key in body (HUD/LSL).
// Caller must be an active clan member with can_manage_location permission.

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

async function resolveAvatarKey(event, body) {
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (token) {
    const { data: sessionRow } = await supabase
      .from("website_sessions")
      .select("sl_avatar_key")
      .eq("session_token", token)
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
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ok */ }

  const avatarKey = await resolveAvatarKey(event, body);
  if (!avatarKey) return json(401, { error: "Not authenticated" });

  const {
    region_name, parcel_name,
    position_x, position_y, position_z,
    sl_object_key, is_primary = true,
  } = body;

  if (!region_name) return json(400, { error: "region_name is required" });
  if (position_x == null || position_y == null || position_z == null) {
    return json(400, { error: "position_x, position_y, position_z are required" });
  }

  // Load caller's clan membership
  const { data: caller } = await supabase
    .schema("clan")
    .from("clan_membership_state_view")
    .select("clan_id, can_manage_location, membership_status, sl_username")
    .eq("sl_avatar_key", avatarKey)
    .eq("membership_status", "active")
    .maybeSingle();

  if (!caller) return json(403, { error: "You are not an active clan member" });
  if (!caller.can_manage_location) return json(403, { error: "You do not have permission to manage clan location" });

  const clanId = caller.clan_id;
  const now = new Date().toISOString();

  // If setting as primary, clear existing primary first
  if (is_primary) {
    await supabase
      .schema("clan")
      .from("clan_locations")
      .update({ is_primary: false, updated_at: now })
      .eq("clan_id", clanId)
      .eq("is_primary", true);
  }

  // Upsert by sl_object_key if provided, otherwise insert new
  if (sl_object_key) {
    const { data: existing } = await supabase
      .schema("clan")
      .from("clan_locations")
      .select("id")
      .eq("clan_id", clanId)
      .eq("sl_object_key", sl_object_key)
      .maybeSingle();

    if (existing) {
      await supabase
        .schema("clan")
        .from("clan_locations")
        .update({
          region_name, parcel_name: parcel_name || null,
          position_x, position_y, position_z,
          is_primary: !!is_primary, is_active: true,
          placed_by_avatar_key: avatarKey,
          placed_by_username:   caller.sl_username || null,
          updated_at: now,
        })
        .eq("id", existing.id);
    } else {
      await supabase
        .schema("clan")
        .from("clan_locations")
        .insert({
          clan_id: clanId,
          region_name, parcel_name: parcel_name || null,
          position_x, position_y, position_z,
          sl_object_key,
          is_primary: !!is_primary, is_active: true,
          placed_by_avatar_key: avatarKey,
          placed_by_username:   caller.sl_username || null,
          created_at: now, updated_at: now,
        });
    }
  } else {
    await supabase
      .schema("clan")
      .from("clan_locations")
      .insert({
        clan_id: clanId,
        region_name, parcel_name: parcel_name || null,
        position_x, position_y, position_z,
        sl_object_key: null,
        is_primary: !!is_primary, is_active: true,
        placed_by_avatar_key: avatarKey,
        placed_by_username:   caller.sl_username || null,
        created_at: now, updated_at: now,
      });
  }

  return json(200, {
    success:     true,
    clan_id:     clanId,
    region_name,
    is_primary:  !!is_primary,
  });
};
