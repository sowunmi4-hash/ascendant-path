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

function safeBoolean(value) {
  return value === true;
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

function mapClanRow(row, userContext) {
  const clanKey = cleanText(row.clan_key);
  const myClanKey = cleanText(userContext?.membership?.clan_key);
  const petitionClanKey = cleanText(userContext?.petition?.clan_key);
  const petitionStatus = cleanLower(userContext?.petition?.request_status);

  let join_state = "joinable";

  if (myClanKey && myClanKey === clanKey) {
    join_state = "member";
  } else if (petitionStatus === "pending" && petitionClanKey === clanKey) {
    join_state = "petition_pending_here";
  } else if (petitionStatus === "pending" && petitionClanKey && petitionClanKey !== clanKey) {
    join_state = "petition_pending_elsewhere";
  } else if (myClanKey && myClanKey !== clanKey) {
    join_state = "already_in_other_clan";
  } else if (!safeBoolean(row.is_joinable)) {
    join_state = "not_joinable";
  }

  const newMemberCap = safeNumber(row.new_member_cap, 0);
  const newMemberOccupied = safeNumber(row.new_member_occupied, 0);
  const newMemberOpenSeats = Math.max(0, newMemberCap - newMemberOccupied);

  return {
    clan_id: row.clan_id,
    clan_key: clanKey,
    clan_name: cleanText(row.clan_name),
    clan_type: cleanText(row.clan_type),
    is_great_clan: safeBoolean(row.is_great_clan),
    is_public: safeBoolean(row.is_public),
    is_joinable: safeBoolean(row.is_joinable),
    join_policy: cleanText(row.join_policy),
    founder_name: cleanText(row.founder_name),
    motto: cleanText(row.motto),
    summary: cleanText(row.summary),
    full_lore: cleanText(row.full_lore),
    crest_image_url: cleanText(row.crest_image_url),
    primary_theme_color: cleanText(row.primary_theme_color),
    secondary_theme_color: cleanText(row.secondary_theme_color),
    clan_status: cleanText(row.clan_status),

    current_clan_head_membership_id: row.current_clan_head_membership_id || null,
    current_clan_head_avatar_key: cleanText(row.current_clan_head_avatar_key) || null,
    current_clan_head_username: cleanText(row.current_clan_head_username) || null,
    current_clan_head_display_name: cleanText(row.current_clan_head_display_name) || null,

    primary_region_name: cleanText(row.primary_region_name) || null,
    primary_parcel_name: cleanText(row.primary_parcel_name) || null,
    primary_position_x: row.primary_position_x ?? null,
    primary_position_y: row.primary_position_y ?? null,
    primary_position_z: row.primary_position_z ?? null,

    counts: {
      new_member_count: safeNumber(row.new_member_count, 0),
      outer_court_count: safeNumber(row.outer_court_count, 0),
      inner_court_count: safeNumber(row.inner_court_count, 0),
      elder_count: safeNumber(row.elder_count, 0),
      clan_head_count: safeNumber(row.clan_head_count, 0),
      total_active_count: safeNumber(row.total_active_count, 0)
    },

    seats: {
      new_member_cap: newMemberCap,
      new_member_occupied: newMemberOccupied,
      new_member_open: newMemberOpenSeats,

      outer_court_cap: safeNumber(row.outer_court_cap, 0),
      outer_court_occupied: safeNumber(row.outer_court_occupied, 0),

      inner_court_cap: safeNumber(row.inner_court_cap, 0),
      inner_court_occupied: safeNumber(row.inner_court_occupied, 0),

      elder_cap: safeNumber(row.elder_cap, 0),
      elder_occupied: safeNumber(row.elder_occupied, 0),

      clan_head_cap: safeNumber(row.clan_head_cap, 0),
      clan_head_occupied: safeNumber(row.clan_head_occupied, 0)
    },

    clan_head_under_admin_oversight: safeBoolean(row.clan_head_under_admin_oversight),

    join_state,
    can_petition_now:
      join_state === "joinable" && safeBoolean(row.is_joinable)
  };
}

async function loadPublicClans() {
  const { data, error } = await supabase.rpc("get_public_clan_directory");

  if (error) {
    throw new Error(`Failed to load public clans: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

async function loadMyClanState(sl_avatar_key, sl_username) {
  if (!sl_avatar_key && !sl_username) return null;

  const { data, error } = await supabase.rpc("get_my_clan_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load my clan state: ${error.message}`);
  }

  return firstRow(data);
}

async function loadMyPetitionState(sl_avatar_key, sl_username) {
  if (!sl_avatar_key && !sl_username) return null;

  const { data, error } = await supabase.rpc("get_my_current_clan_petition_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load clan petition state: ${error.message}`);
  }

  return firstRow(data);
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

    const warnings = [];

    const clans = await loadPublicClans();

    let membership = null;
    let petition = null;

    if (sl_avatar_key || sl_username) {
      try {
        const [membershipResult, petitionResult] = await Promise.all([
          loadMyClanState(sl_avatar_key, sl_username),
          loadMyPetitionState(sl_avatar_key, sl_username)
        ]);

        membership = membershipResult;
        petition = petitionResult;
      } catch (contextError) {
        console.error("list-public-clans user context error:", contextError);
        warnings.push(contextError.message);
      }
    }

    const userContext = {
      membership,
      petition
    };

    const mappedClans = clans.map((row) => mapClanRow(row, userContext));

    return buildResponse(200, {
      success: true,
      message: "Public clan directory loaded successfully.",
      warnings,
      user_context: {
        has_clan_membership: !!membership,
        membership: membership
          ? {
              membership_id: membership.membership_id,
              clan_id: membership.clan_id,
              clan_key: cleanText(membership.clan_key),
              clan_name: cleanText(membership.clan_name),
              current_rank_key: cleanText(membership.current_rank_key),
              current_rank_name: cleanText(membership.current_rank_name),
              membership_status: cleanText(membership.membership_status),
              loyalty_percent: safeNumber(membership.loyalty_percent, 0),
              clan_contribution: safeNumber(membership.clan_contribution, 0),
              promotion_credit: safeNumber(membership.promotion_credit, 0)
            }
          : null,
        petition: petition
          ? {
              request_id: petition.request_id,
              clan_id: petition.clan_id,
              clan_key: cleanText(petition.clan_key),
              clan_name: cleanText(petition.clan_name),
              request_status: cleanText(petition.request_status),
              request_message: cleanText(petition.request_message),
              reviewed_by_username: cleanText(petition.reviewed_by_username) || null,
              reviewed_at: petition.reviewed_at || null,
              decision_note: cleanText(petition.decision_note) || null,
              created_at: petition.created_at || null,
              updated_at: petition.updated_at || null
            }
          : null
      },
      clan_count: mappedClans.length,
      clans: mappedClans
    });
  } catch (error) {
    console.error("list-public-clans error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to load public clan directory.",
      error: error.message || "Unknown error."
    });
  }
};