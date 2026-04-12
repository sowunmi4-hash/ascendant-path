const { createClient } = require("@supabase/supabase-js");

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const librarySupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "library" }
  }
);

// =========================================================
// WHAT THIS FILE DOES (V2)
//
// Calls library.v2_repair_stage(p_sl_avatar_key).
//
// This is the REPAIR step only.
// DB owns:
// - whether repair is needed
// - whether repair is allowed
// - CP cost
// - retained comprehension handling
// - damaged stage state updates
// - refreshed stage/breakthrough state after repair
//
// JS stays thin:
// - accept request
// - resolve avatar key if needed
// - call the V2 DB function
// - return DB payload
// =========================================================

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function parseEventBody(event) {
  if (!event || !event.body) return {};
  const parsed = safeJsonParse(event.body);
  return parsed && typeof parsed === "object" ? parsed : {};
}

function firstNonEmptyString(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value
    : null;
}

async function resolveAvatarKey({ avatarKey, username }) {
  if (avatarKey) {
    return avatarKey;
  }

  if (!username) {
    return null;
  }

  const { data, error } = await publicSupabase
    .from("cultivation_members")
    .select("sl_avatar_key, sl_username")
    .ilike("sl_username", username)
    .maybeSingle();

  if (error) {
    throw new Error(`Member lookup failed: ${error.message}`);
  }

  return data?.sl_avatar_key || null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "POST" && event.httpMethod !== "GET") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use POST."
    });
  }

  try {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, {
        success: false,
        message: "Missing Supabase environment variables."
      });
    }

    const body = parseEventBody(event);
    const query = event.queryStringParameters || {};

    const avatarKey = firstNonEmptyString(
      body.sl_avatar_key,
      body.avatar_key,
      body.avatarKey,
      query.sl_avatar_key,
      query.avatar_key,
      query.avatarKey
    );

    const username = firstNonEmptyString(
      body.sl_username,
      body.username,
      body.userName,
      query.sl_username,
      query.username,
      query.userName
    );

    const resolvedAvatarKey = await resolveAvatarKey({
      avatarKey,
      username
    });

    if (!resolvedAvatarKey) {
      return buildResponse(400, {
        success: false,
        message: "Missing SL avatar key. Provide sl_avatar_key or a resolvable sl_username."
      });
    }

    const { data, error } = await librarySupabase.rpc(
      "v2_repair_stage",
      {
        p_sl_avatar_key: resolvedAvatarKey
      }
    );

    if (error) {
      return buildResponse(400, {
        success: false,
        message: error.message || "Failed to repair cultivation stage."
      });
    }

    if (data == null) {
      return buildResponse(500, {
        success: false,
        message: "Repair completed with no payload returned from DB."
      });
    }

    const payload = objectValue(data);

    if (payload?.success === false) {
      return buildResponse(400, {
        success: false,
        message:
          payload.message ||
          payload.detail ||
          payload.status_message ||
          "Cultivation stage repair failed.",
        ...payload
      });
    }

    return buildResponse(200, {
      success: true,
      message:
        payload?.message ||
        payload?.detail ||
        payload?.status_message ||
        "Cultivation stage repaired successfully.",
      ...(payload ? payload : { data })
    });
  } catch (error) {
    console.error("repair-cultivation-book error:", error);

    return buildResponse(500, {
      success: false,
      message: error.message || "Unexpected server error."
    });
  }
};