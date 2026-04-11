const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY;

const baseHeaders = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
  "Access-Control-Allow-Origin": "*"
};

function response(statusCode, body, extraHeaders = {}) {
  return {
    statusCode,
    headers: {
      ...baseHeaders,
      ...extraHeaders
    },
    body: JSON.stringify(body)
  };
}

function normalizeWorldState(raw) {
  const state = raw && typeof raw === "object" ? raw : {};

  return {
    success: typeof state.success === "boolean" ? state.success : true,
    message: state.message || null,
    event_name: state.event_name || "Tribulation Lightning",
    tribulation_lightning_active: Boolean(state.tribulation_lightning_active),
    active_event_count: Number(state.active_event_count || 0),
    dominant_intensity: state.dominant_intensity || null,
    omen_text: state.omen_text || null,
    latest_target_id: state.latest_target_id || null,
    latest_started_at: state.latest_started_at || null,
    latest_sl_avatar_key: state.latest_sl_avatar_key || null,
    latest_sl_username: state.latest_sl_username || null,
    active_usernames: Array.isArray(state.active_usernames)
      ? state.active_usernames
      : []
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        ...baseHeaders,
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
      },
      body: ""
    };
  }

  if (event.httpMethod !== "GET") {
    return response(405, {
      success: false,
      message: "Method not allowed. Use GET."
    });
  }

  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
    return response(500, {
      success: false,
      message: "Missing Supabase environment variables."
    });
  }

  try {
    const breakthroughSupabase = createClient(
      SUPABASE_URL,
      SUPABASE_SECRET_KEY,
      {
        db: { schema: "breakthrough" }
      }
    );

    const { data, error } = await breakthroughSupabase.rpc(
      "get_tribulation_lightning_world_state"
    );

    if (error) {
      console.error("load-tribulation rpc error:", error);
      return response(500, {
        success: false,
        message: error.message || "Failed to load tribulation state."
      });
    }

    const worldState = normalizeWorldState(data);

    if (worldState.success === false) {
      return response(200, {
        success: false,
        message: worldState.message || "Tribulation state loaded with no active result.",
        tribulation: worldState
      });
    }

    return response(200, {
      success: true,
      tribulation: worldState,
      event_name: worldState.event_name,
      tribulation_lightning_active: worldState.tribulation_lightning_active,
      active_event_count: worldState.active_event_count,
      dominant_intensity: worldState.dominant_intensity,
      omen_text: worldState.omen_text,
      latest_target_id: worldState.latest_target_id,
      latest_started_at: worldState.latest_started_at,
      latest_sl_avatar_key: worldState.latest_sl_avatar_key,
      latest_sl_username: worldState.latest_sl_username,
      active_usernames: worldState.active_usernames
    });
  } catch (error) {
    console.error("load-tribulation unexpected error:", error);

    return response(500, {
      success: false,
      message: error.message || "Unexpected error while loading tribulation state."
    });
  }
};