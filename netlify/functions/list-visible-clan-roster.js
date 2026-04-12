const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function cleanText(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function cleanLower(value) {
  return cleanText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function firstRow(data) {
  return Array.isArray(data) && data.length > 0 ? data[0] : null;
}

function mapRoster(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    membership_id: row.membership_id,
    clan_id: row.clan_id,
    clan_key: cleanText(row.clan_key),
    clan_name: cleanText(row.clan_name),
    sl_avatar_key: cleanText(row.sl_avatar_key),
    sl_username: cleanText(row.sl_username),
    display_name: cleanText(row.display_name) || cleanText(row.sl_username),
    current_rank_key: cleanText(row.current_rank_key),
    current_rank_name: cleanText(row.current_rank_name),
    current_rank_order: safeNumber(row.current_rank_order, 0),
    membership_status: cleanText(row.membership_status),
    loyalty_percent: safeNumber(row.loyalty_percent, 0),
    clan_contribution: safeNumber(row.clan_contribution, 0),
    promotion_credit: safeNumber(row.promotion_credit, 0),
    realm_index: row.realm_index ?? null,
    realm_display_name: cleanText(row.realm_display_name) || null,
    current_seat_id: row.current_seat_id || null,
    current_seat_number: row.current_seat_number ?? null,
    current_seat_label: cleanText(row.current_seat_label) || null
  }));
}

async function loadMyClanState(sl_avatar_key, sl_username) {
  const { data, error } = await supabase.rpc("get_my_clan_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load my clan state: ${error.message}`);
  }

  return firstRow(data);
}

async function loadVisibleRoster(membership_id) {
  const { data, error } = await supabase.rpc("get_visible_clan_roster_for_membership", {
    p_viewer_membership_id: membership_id
  });

  if (error) {
    throw new Error(`Failed to load visible clan roster: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use GET or POST."
    });
  }

  try {
    const body = parseBody(event);
    const query = event.queryStringParameters || {};

    const sl_avatar_key = cleanText(query.sl_avatar_key || body.sl_avatar_key);
    const sl_username = cleanLower(query.sl_username || body.sl_username);

    if (!sl_avatar_key && !sl_username) {
      return buildResponse(400, {
        success: false,
        message: "Missing required member identity. Provide sl_avatar_key or sl_username."
      });
    }

    const membership = await loadMyClanState(sl_avatar_key, sl_username);

    if (!membership) {
      return buildResponse(404, {
        success: false,
        message: "No active clan membership found for this user."
      });
    }

    const rosterRows = await loadVisibleRoster(membership.membership_id);

    return buildResponse(200, {
      success: true,
      message: "Visible clan roster loaded successfully.",
      viewer: {
        membership_id: membership.membership_id,
        clan_id: membership.clan_id,
        clan_key: cleanText(membership.clan_key),
        clan_name: cleanText(membership.clan_name),
        sl_avatar_key: cleanText(membership.sl_avatar_key),
        sl_username: cleanText(membership.sl_username),
        display_name: cleanText(membership.display_name) || cleanText(membership.sl_username),
        current_rank_key: cleanText(membership.current_rank_key),
        current_rank_name: cleanText(membership.current_rank_name),
        current_rank_order: safeNumber(membership.current_rank_order, 0),
        visibility_level: safeNumber(membership.visibility_level, 0)
      },
      roster_count: rosterRows.length,
      roster: mapRoster(rosterRows)
    });
  } catch (error) {
    console.error("list-visible-clan-roster error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to load visible clan roster.",
      error: error.message || "Unknown error."
    });
  }
};