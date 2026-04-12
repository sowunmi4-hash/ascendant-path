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
      "Access-Control-Allow-Methods": "POST, OPTIONS"
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

function inferErrorStatus(message) {
  const text = cleanLower(message);

  if (!text) return 500;
  if (text.includes("required")) return 400;
  if (text.includes("not found")) return 404;
  if (text.includes("only pending requests may be withdrawn")) return 409;
  if (text.includes("actor does not own this request")) return 403;

  return 500;
}

async function loadMember(sl_avatar_key, sl_username) {
  let query = supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      character_name
    `)
    .limit(1);

  if (sl_avatar_key) {
    query = query.eq("sl_avatar_key", sl_avatar_key);
  } else if (sl_username) {
    query = query.eq("sl_username", sl_username);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function loadLatestPetition(sl_avatar_key, sl_username) {
  const { data, error } = await supabase.rpc("get_my_current_clan_petition_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load current clan petition state: ${error.message}`);
  }

  return firstRow(data);
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use POST."
    });
  }

  try {
    const body = parseBody(event);

    const request_id = cleanText(body.request_id);
    const input_avatar_key = cleanText(body.sl_avatar_key);
    const input_username = cleanLower(body.sl_username);

    if (!request_id) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: request_id."
      });
    }

    if (!input_avatar_key && !input_username) {
      return buildResponse(400, {
        success: false,
        message: "Missing required member identity. Provide sl_avatar_key or sl_username."
      });
    }

    const member = await loadMember(input_avatar_key, input_username);

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    const sl_avatar_key = cleanText(member.sl_avatar_key);
    const sl_username = cleanLower(member.sl_username);

    const { data, error } = await supabase.rpc("withdraw_clan_join_request", {
      p_request_id: request_id,
      p_actor_avatar_key: sl_avatar_key
    });

    if (error) {
      console.error("withdraw-clan-join-request rpc error:", error);

      return buildResponse(inferErrorStatus(error.message), {
        success: false,
        message: error.message || "Failed to withdraw clan join request."
      });
    }

    const withdrawnRow = firstRow(data);

    let latestPetition = null;
    try {
      latestPetition = await loadLatestPetition(sl_avatar_key, sl_username);
    } catch (petitionError) {
      console.error("withdraw-clan-join-request petition reload error:", petitionError);
    }

    return buildResponse(200, {
      success: true,
      message: "Clan petition withdrawn successfully.",
      request: withdrawnRow
        ? {
            request_id: withdrawnRow.request_id,
            clan_id: withdrawnRow.clan_id,
            request_status: cleanText(withdrawnRow.request_status)
          }
        : null,
      petition_state: latestPetition
        ? {
            request_id: latestPetition.request_id,
            clan_id: latestPetition.clan_id,
            clan_key: cleanText(latestPetition.clan_key),
            clan_name: cleanText(latestPetition.clan_name),
            request_status: cleanText(latestPetition.request_status),
            request_message: cleanText(latestPetition.request_message),
            reviewed_by_username: cleanText(latestPetition.reviewed_by_username) || null,
            reviewed_at: latestPetition.reviewed_at || null,
            decision_note: cleanText(latestPetition.decision_note) || null,
            created_at: latestPetition.created_at || null,
            updated_at: latestPetition.updated_at || null
          }
        : null,
      member: {
        member_id: member.member_id || null,
        sl_avatar_key,
        sl_username,
        display_name:
          cleanText(member.display_name) ||
          cleanText(member.character_name) ||
          sl_username
      }
    });
  } catch (error) {
    console.error("withdraw-clan-join-request error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to withdraw clan join request.",
      error: error.message || "Unknown error."
    });
  }
};