const { createClient } = require("@supabase/supabase-js");

const publicSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "breakthrough" }
  }
);

// =========================================================
// WHAT THIS FILE DOES (V2)
//
// Calls breakthrough.v2_reveal_breakthrough_verdict(p_sl_avatar_key).
//
// This is the REVEAL step only.
// It does not enter, begin, or resolve the breakthrough.
// It only asks the DB for the verdict that is ready to be shown.
//
// DB owns the truth for:
// - whether a breakthrough exists
// - whether it has been resolved already
// - whether the verdict is revealable
// - what result should be shown
// - any persisted state/details tied to the verdict
//
// JS stays thin:
// - accept request
// - resolve avatar key if needed
// - call the V2 DB function
// - return DB payload
// =========================================================

// =========================================================
// RESPONSE HELPERS
// =========================================================

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

// =========================================================
// REQUEST HELPERS
// =========================================================

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

// =========================================================
// MEMBER RESOLUTION
// =========================================================

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

// =========================================================
// MAIN
// =========================================================

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

    const { data, error } = await breakthroughSupabase.rpc(
      "v2_reveal_breakthrough_verdict",
      {
        p_sl_avatar_key: resolvedAvatarKey
      }
    );

    if (error) {
      return buildResponse(400, {
        success: false,
        message: error.message || "Failed to reveal breakthrough verdict."
      });
    }

    if (data == null) {
      return buildResponse(500, {
        success: false,
        message: "Reveal completed with no payload returned from DB."
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
          "Breakthrough verdict reveal failed.",
        ...payload
      });
    }

    // Fire notification for breakthrough outcome
    try {
      const outcome   = payload?.outcome || payload?.verdict_key || 'unknown';
      const isSuccess = outcome === 'success' || (payload?.lifecycle_status === 'success');
      const verdictText = payload?.verdict_text || payload?.message || 'Heaven has rendered its judgment.';
      await publicSupabase.from('member_notifications').insert({
        sl_avatar_key: resolvedAvatarKey,
        sl_username:   '',
        type:          isSuccess ? 'breakthrough_success' : 'breakthrough_failure',
        title:         isSuccess ? 'Breakthrough — Heaven Yields' : 'Breakthrough — Heaven Holds Firm',
        message:       verdictText,
        is_read:       false,
        metadata:      { outcome, lifecycle_status: payload?.lifecycle_status, verdict_key: payload?.verdict_key }
      });
    } catch (notifErr) {
      console.error('Notification insert error:', notifErr);
    }

    return buildResponse(200, {
      success: true,
      message:
        payload?.message ||
        payload?.detail ||
        payload?.status_message ||
        "Breakthrough verdict revealed successfully.",
      ...(payload ? payload : { data })
    });
  } catch (error) {
    console.error("reveal-breakthrough-verdict error:", error);

    return buildResponse(500, {
      success: false,
      message: error.message || "Unexpected server error."
    });
  }
};