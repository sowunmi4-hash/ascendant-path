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
      "Access-Control-Allow-Methods": "POST, OPTIONS"
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

function safeLower(value) {
  return safeText(value).toLowerCase();
}

// =========================================================
// MEMBER LOADER
// =========================================================

async function loadMember(slAvatarKey, slUsername) {
  let query = supabase
    .from("cultivation_members")
    .select("member_id, sl_avatar_key, sl_username, personal_cultivation_preference")
    .limit(1);

  if (slAvatarKey) {
    query = query.eq("sl_avatar_key", slAvatarKey);
  } else if (slUsername) {
    query = query.eq("sl_username", slUsername);
  }

  const { data, error } = await query.maybeSingle();
  if (error) throw new Error(`Failed to load cultivation member: ${error.message}`);
  return data || null;
}

// =========================================================
// MAIN HANDLER
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
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, {
        success: false,
        message: "Missing Supabase environment variables."
      });
    }

    const body = parseBody(event);
    const slAvatarKey = safeText(body.sl_avatar_key);
    const slUsername = safeText(body.sl_username);
    const preference = safeLower(body.personal_cultivation_preference);

    if (!slAvatarKey && !slUsername) {
      return buildResponse(400, {
        success: false,
        message: "sl_avatar_key or sl_username is required."
      });
    }

    if (!["manual", "auto"].includes(preference)) {
      return buildResponse(400, {
        success: false,
        message: "personal_cultivation_preference must be 'manual' or 'auto'."
      });
    }

    // -------------------------------------------------------
    // LOAD MEMBER
    // -------------------------------------------------------
    const member = await loadMember(slAvatarKey, slUsername);

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    // -------------------------------------------------------
    // UPDATE PREFERENCE
    // -------------------------------------------------------
    const { error: updateError } = await supabase
      .from("cultivation_members")
      .update({ personal_cultivation_preference: preference })
      .eq("member_id", member.member_id);

    if (updateError) {
      console.error("save-personal-cultivation-preference update error:", updateError);
      return buildResponse(500, {
        success: false,
        message: "Failed to save preference.",
        error: updateError.message
      });
    }

    return buildResponse(200, {
      success: true,
      message: `Personal cultivation preference set to '${preference}'.`,
      sl_avatar_key: safeText(member.sl_avatar_key),
      sl_username: safeText(member.sl_username),
      personal_cultivation_preference: preference
    });
  } catch (error) {
    console.error("save-personal-cultivation-preference error:", error);
    return buildResponse(500, {
      success: false,
      message: "Failed to save personal cultivation preference.",
      error: error.message || "Unknown error."
    });
  }
};
