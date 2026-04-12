const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
};

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
}

function getRequestOrigin(event) {
  const headers = event.headers || {};

  const forwardedProto =
    headers["x-forwarded-proto"] ||
    headers["X-Forwarded-Proto"] ||
    "https";

  const host =
    headers.host ||
    headers.Host ||
    process.env.URL?.replace(/^https?:\/\//, "") ||
    process.env.DEPLOY_PRIME_URL?.replace(/^https?:\/\//, "") ||
    process.env.SITE_URL?.replace(/^https?:\/\//, "") ||
    "";

  if (!host) return "";
  return `${forwardedProto}://${host}`;
}

function getForwardHeaders(event) {
  const headers = event.headers || {};
  const forward = {
    Accept: "application/json"
  };

  const cookie = headers.cookie || headers.Cookie;
  const authorization = headers.authorization || headers.Authorization;

  if (cookie) {
    forward.cookie = cookie;
  }

  if (authorization) {
    forward.Authorization = authorization;
  }

  return forward;
}

function extractMember(payload) {
  if (!payload || typeof payload !== "object") return null;

  return (
    payload.member ||
    payload.data?.member ||
    payload.result?.member ||
    payload.state?.member ||
    null
  );
}

exports.handler = async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders,
      body: ""
    };
  }

  if (!["GET", "POST"].includes(event.httpMethod)) {
    return json(405, {
      success: false,
      error: "Method not allowed."
    });
  }

  const origin = getRequestOrigin(event);

  if (!origin) {
    return json(500, {
      success: false,
      error: "Could not determine request origin."
    });
  }

  try {
    const response = await fetch(
      `${origin}/.netlify/functions/load-dashboard-state`,
      {
        method: "GET",
        headers: getForwardHeaders(event)
      }
    );

    const text = await response.text();
    let payload = null;

    try {
      payload = text ? JSON.parse(text) : null;
    } catch (err) {
      payload = null;
    }

    if (!response.ok) {
      return json(response.status, {
        success: false,
        error:
          payload?.error ||
          payload?.message ||
          "Failed to load dashboard state for Oracle context."
      });
    }

    const member = extractMember(payload);

    const slAvatarKey = String(
      member?.sl_avatar_key ||
      member?.avatar_key ||
      payload?.sl_avatar_key ||
      payload?.avatar_key ||
      ""
    ).trim();

    const slUsername = String(
      member?.sl_username ||
      member?.username ||
      payload?.sl_username ||
      payload?.username ||
      ""
    ).trim();

    const characterName = String(
      member?.character_name ||
      payload?.character_name ||
      ""
    ).trim();

    if (!slAvatarKey) {
      return json(500, {
        success: false,
        error: "Oracle member context loaded, but no avatar key was found.",
        debug: {
          hasMember: Boolean(member),
          hasUsername: Boolean(slUsername),
          source: "load-dashboard-state"
        }
      });
    }

    return json(200, {
      success: true,
      member: {
        sl_avatar_key: slAvatarKey,
        sl_username: slUsername,
        character_name: characterName
      }
    });
  } catch (err) {
    console.error("Unhandled load-oracle-member-context error:", err);

    return json(500, {
      success: false,
      error: err.message || "Unexpected server error."
    });
  }
};