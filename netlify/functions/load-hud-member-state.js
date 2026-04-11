const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

// =========================================================
// UTILITIES
// =========================================================

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

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function unwrapRpcPayload(data) {
  if (Array.isArray(data)) return data[0] || null;
  return data || null;
}

// =========================================================
// HANDLER
// =========================================================

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
    const requestSource = { ...query, ...body };

    const slAvatarKey = safeText(requestSource.sl_avatar_key);
    const slUsername = safeText(requestSource.sl_username) || null;
    const currentRegionName = safeText(requestSource.current_region_name) || null;
    const currentPositionX = toNumberOrNull(requestSource.current_position_x);
    const currentPositionY = toNumberOrNull(requestSource.current_position_y);
    const currentPositionZ = toNumberOrNull(requestSource.current_position_z);

    if (!slAvatarKey) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: sl_avatar_key"
      });
    }

    const rpcPayload = {
      p_sl_avatar_key: slAvatarKey,
      p_sl_username: slUsername,
      p_current_region_name: currentRegionName,
      p_current_position_x: currentPositionX,
      p_current_position_y: currentPositionY,
      p_current_position_z: currentPositionZ
    };

    const { data, error } = await supabase.rpc("load_hud_member_state", rpcPayload);

    if (error) {
      console.error("load-hud-member-state rpc error:", error);

      return buildResponse(500, {
        success: false,
        message: "Failed to load HUD member state.",
        error: error.message
      });
    }

    const rpcResult = unwrapRpcPayload(data);

    if (!rpcResult || typeof rpcResult !== "object") {
      return buildResponse(500, {
        success: false,
        message: "HUD member state RPC returned no usable payload."
      });
    }

    return buildResponse(200, rpcResult);
  } catch (error) {
    console.error("load-hud-member-state server error:", error);

    return buildResponse(500, {
      success: false,
      message: "Server error",
      error: error.message
    });
  }
};