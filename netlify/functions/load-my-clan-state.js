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

function mapMembership(row) {
  if (!row) return null;

  return {
    membership_id: row.membership_id,
    clan_id: row.clan_id,
    clan_key: cleanText(row.clan_key),
    clan_name: cleanText(row.clan_name),
    clan_type: cleanText(row.clan_type),
    is_great_clan: safeBoolean(row.is_great_clan),

    founder_name: cleanText(row.founder_name),
    motto: cleanText(row.motto),
    summary: cleanText(row.summary),
    full_lore: cleanText(row.full_lore),
    crest_image_url: cleanText(row.crest_image_url),
    primary_theme_color: cleanText(row.primary_theme_color),
    secondary_theme_color: cleanText(row.secondary_theme_color),

    member: {
      sl_avatar_key: cleanText(row.sl_avatar_key),
      sl_username: cleanText(row.sl_username),
      display_name: cleanText(row.display_name) || cleanText(row.sl_username)
    },

    rank: {
      current_rank_key: cleanText(row.current_rank_key),
      current_rank_name: cleanText(row.current_rank_name),
      current_rank_order: safeNumber(row.current_rank_order, 0),
      visibility_level: safeNumber(row.visibility_level, 0),
      rank_min_realm_index: row.rank_min_realm_index ?? null,
      rank_max_realm_index: row.rank_max_realm_index ?? null,
      is_seated: safeBoolean(row.is_seated),
      is_leadership: safeBoolean(row.is_leadership),
      can_issue_tasks: safeBoolean(row.can_issue_tasks),
      can_review_promotions: safeBoolean(row.can_review_promotions),
      can_manage_location: safeBoolean(row.can_manage_location),
      can_manage_discipline: safeBoolean(row.can_manage_discipline)
    },

    progression: {
      membership_status: cleanText(row.membership_status),
      loyalty_percent: safeNumber(row.loyalty_percent, 0),
      clan_contribution: safeNumber(row.clan_contribution, 0),
      promotion_credit: safeNumber(row.promotion_credit, 0),
      joined_at: row.joined_at || null,
      rank_assigned_at: row.rank_assigned_at || null,
      probation_started_at: row.probation_started_at || null,
      suspended_at: row.suspended_at || null,
      expelled_at: row.expelled_at || null,
      left_at: row.left_at || null
    },

    seat: {
      current_seat_id: row.current_seat_id || null,
      current_seat_number: row.current_seat_number ?? null,
      current_seat_label: cleanText(row.current_seat_label) || null,
      current_seat_status: cleanText(row.current_seat_status) || null
    },

    cultivation: {
      member_id: row.member_id || null,
      realm_index: row.realm_index ?? null,
      realm_key: cleanText(row.realm_key) || null,
      realm_name: cleanText(row.realm_name) || null,
      realm_display_name: cleanText(row.realm_display_name) || null
    },

    location: {
      primary_region_name: cleanText(row.primary_region_name) || null,
      primary_parcel_name: cleanText(row.primary_parcel_name) || null,
      primary_position_x: row.primary_position_x ?? null,
      primary_position_y: row.primary_position_y ?? null,
      primary_position_z: row.primary_position_z ?? null
    },

    visibility: {
      can_view_new_member: safeBoolean(row.can_view_new_member),
      can_view_outer_court: safeBoolean(row.can_view_outer_court),
      can_view_inner_court: safeBoolean(row.can_view_inner_court),
      can_view_elder: safeBoolean(row.can_view_elder),
      can_view_clan_head: safeBoolean(row.can_view_clan_head)
    },

    clan_head_under_admin_oversight: safeBoolean(row.clan_head_under_admin_oversight),

    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
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

function mapTasks(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    task_instance_id: row.task_instance_id,
    task_id: row.task_id,
    task_key: cleanText(row.task_key),
    owner_rank_key: cleanText(row.owner_rank_key),
    task_name: cleanText(row.task_name),
    task_frequency: cleanText(row.task_frequency),
    proof_type: cleanText(row.proof_type),
    requires_approval: safeBoolean(row.requires_approval),
    auto_progress_rule: cleanText(row.auto_progress_rule) || null,
    display_order: safeNumber(row.display_order, 0),
    description: cleanText(row.description),
    cycle_key: cleanText(row.cycle_key),
    cycle_started_at: row.cycle_started_at || null,
    cycle_ends_at: row.cycle_ends_at || null,
    progress_current: safeNumber(row.progress_current, 0),
    progress_target: safeNumber(row.progress_target, 1),
    task_status: cleanText(row.task_status),
    proof_status: cleanText(row.proof_status),
    proof_note: cleanText(row.proof_note) || null,
    submitted_at: row.submitted_at || null,
    approved_at: row.approved_at || null,
    claimed_at: row.claimed_at || null,
    reward_tokens: safeNumber(row.reward_tokens, 0),
    reward_loyalty_percent: safeNumber(row.reward_loyalty_percent, 0),
    reward_clan_contribution: safeNumber(row.reward_clan_contribution, 0),
    reward_promotion_credit: safeNumber(row.reward_promotion_credit, 0)
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

async function loadPetitionState(sl_avatar_key, sl_username) {
  const { data, error } = await supabase.rpc("get_my_current_clan_petition_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load clan petition state: ${error.message}`);
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

async function loadCurrentTasks(membership_id) {
  const { data, error } = await supabase.rpc("get_current_clan_tasks_for_membership", {
    p_membership_id: membership_id
  });

  if (error) {
    throw new Error(`Failed to load current clan tasks: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

async function loadGovernanceSummary(clan_id, actor_avatar_key) {
  const { data, error } = await supabase.rpc("get_clan_governance_summary", {
    p_clan_id: clan_id,
    p_actor_avatar_key: actor_avatar_key || null,
    p_is_admin: false
  });

  if (error) {
    throw new Error(`Failed to load clan governance summary: ${error.message}`);
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

    if (!sl_avatar_key && !sl_username) {
      return buildResponse(400, {
        success: false,
        message: "Missing required member identity. Provide sl_avatar_key or sl_username."
      });
    }

    const warnings = [];

    let petition = null;
    try {
      petition = await loadPetitionState(sl_avatar_key, sl_username);
    } catch (petitionError) {
      console.error("load-my-clan-state petition error:", petitionError);
      warnings.push(petitionError.message);
    }

    const membershipRow = await loadMyClanState(sl_avatar_key, sl_username);

    if (!membershipRow) {
      return buildResponse(200, {
        success: true,
        message: "No active clan membership found.",
        warnings,
        has_clan_membership: false,
        membership: null,
        roster: [],
        tasks: [],
        governance: null,
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
      });
    }

    const membership = mapMembership(membershipRow);

    const [rosterResult, tasksResult, governanceResult] = await Promise.all([
      loadVisibleRoster(membership.membership_id).catch((error) => {
        console.error("load-my-clan-state roster error:", error);
        warnings.push(error.message);
        return [];
      }),
      loadCurrentTasks(membership.membership_id).catch((error) => {
        console.error("load-my-clan-state tasks error:", error);
        warnings.push(error.message);
        return [];
      }),
      membership.rank.can_review_promotions ||
      membership.rank.can_manage_discipline ||
      membership.rank.can_issue_tasks
        ? loadGovernanceSummary(membership.clan_id, membership.member.sl_avatar_key).catch((error) => {
            console.error("load-my-clan-state governance error:", error);
            warnings.push(error.message);
            return null;
          })
        : Promise.resolve(null)
    ]);

    return buildResponse(200, {
      success: true,
      message: "My clan state loaded successfully.",
      warnings,
      has_clan_membership: true,
      membership,
      roster: mapRoster(rosterResult),
      tasks: mapTasks(tasksResult),
      governance: governanceResult
        ? {
            clan_id: governanceResult.clan_id,
            pending_join_requests: safeNumber(governanceResult.pending_join_requests, 0),
            pending_promotion_requests: safeNumber(governanceResult.pending_promotion_requests, 0),
            active_discipline_cases: safeNumber(governanceResult.active_discipline_cases, 0),
            active_probation_members: safeNumber(governanceResult.active_probation_members, 0),
            suspended_members: safeNumber(governanceResult.suspended_members, 0),
            new_member_open_seats: safeNumber(governanceResult.new_member_open_seats, 0),
            outer_court_open_seats: safeNumber(governanceResult.outer_court_open_seats, 0),
            inner_court_open_seats: safeNumber(governanceResult.inner_court_open_seats, 0),
            elder_open_seats: safeNumber(governanceResult.elder_open_seats, 0),
            clan_head_open_seats: safeNumber(governanceResult.clan_head_open_seats, 0),
            clan_head_under_admin_oversight: safeBoolean(governanceResult.clan_head_under_admin_oversight)
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
    });
  } catch (error) {
    console.error("load-my-clan-state error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to load my clan state.",
      error: error.message || "Unknown error."
    });
  }
};