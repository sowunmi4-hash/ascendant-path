const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const COOKIE_NAME = "ap_session";
const SESSION_TABLE = "website_sessions";
const MEMBER_TABLE = "cultivation_members";
const PARTNERSHIP_TABLE = "cultivation_partnerships";

function json(statusCode, payload) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    },
    body: JSON.stringify(payload)
  };
}

function safeText(value, fallback = "") {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function pickFirst(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return undefined;
}

function normalizeLower(value) {
  return safeText(value, "").toLowerCase();
}

function sameValue(a, b) {
  return normalizeLower(a) === normalizeLower(b);
}

function parseCookies(cookieHeader) {
  const cookies = {};

  if (!cookieHeader) {
    return cookies;
  }

  const parts = String(cookieHeader).split(";");

  for (const part of parts) {
    const index = part.indexOf("=");
    if (index === -1) continue;

    const key = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();

    if (!key) continue;

    cookies[key] = decodeURIComponent(value || "");
  }

  return cookies;
}

function parseBody(body) {
  if (!body) return {};
  try {
    return JSON.parse(body);
  } catch {
    return {};
  }
}

function isNoRowsError(error) {
  if (!error) return false;

  const code = safeText(error.code, "");
  const message = safeText(error.message, "").toLowerCase();

  return code === "PGRST116" || message.includes("0 rows");
}

function isUuid(value) {
  const text = safeText(value, "");
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text);
}

function safeUuid(value) {
  const text = safeText(value, "");
  return isUuid(text) ? text : "";
}

function isSessionExpired(sessionRow) {
  const expiresAt = safeText(sessionRow?.expires_at, "");
  if (!expiresAt) return false;

  const expiresMs = Date.parse(expiresAt);
  if (!Number.isFinite(expiresMs)) return false;

  return expiresMs <= Date.now();
}

function isSessionInactive(sessionRow) {
  if (sessionRow?.is_active === false) return true;
  if (sessionRow?.revoked_at) return true;
  return false;
}

function extractPartnershipRefs(source) {
  const rawUuidCandidate = pickFirst(
    source?.partnership_uuid,
    source?.partnershipUuid,
    source?.selected_partnership_id,
    source?.selectedPartnershipId,
    source?.id
  );

  const rawMixedCandidate = pickFirst(
    source?.partnership_id,
    source?.partnershipId
  );

  const partnershipUuid =
    safeUuid(rawUuidCandidate) ||
    safeUuid(rawMixedCandidate);

  const partnershipId = partnershipUuid
    ? 0
    : safeNumber(rawMixedCandidate, 0);

  return {
    partnership_uuid: partnershipUuid,
    partnership_id: partnershipId
  };
}

async function loadSessionByToken(sessionToken) {
  const { data, error } = await supabase
    .from(SESSION_TABLE)
    .select("*")
    .eq("session_token", sessionToken)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

async function touchWebsiteSession(sessionToken) {
  if (!safeText(sessionToken, "")) return;

  const { error } = await supabase
    .from(SESSION_TABLE)
    .update({ updated_at: new Date().toISOString() })
    .eq("session_token", sessionToken);

  if (error) {
    throw new Error(`Failed to update website session timestamp: ${error.message}`);
  }
}

async function loadMemberByAvatarKey(avatarKey) {
  const validAvatarKey = safeUuid(avatarKey);
  if (!validAvatarKey) return null;

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .eq("sl_avatar_key", validAvatarKey)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

async function touchPresence(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return;

  const { error } = await supabase
    .from(MEMBER_TABLE)
    .update({ last_presence_at: new Date().toISOString() })
    .eq("sl_avatar_key", cleanKey);

  if (error) {
    throw new Error(`Failed to update member presence: ${error.message}`);
  }
}

async function loadPartnershipByUuid(partnershipUuid) {
  const cleanUuid = safeUuid(partnershipUuid);
  if (!cleanUuid) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select("*")
    .eq("id", cleanUuid)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

async function loadPartnershipByLegacyId(partnershipId) {
  const cleanId = safeNumber(partnershipId, 0);
  if (!cleanId) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select("*")
    .eq("partnership_id", cleanId)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

function memberCanAccessPartnership(partnershipRow, memberAvatarKey) {
  const requesterAvatarKey = safeText(partnershipRow?.requester_avatar_key, "");
  const recipientAvatarKey = safeText(partnershipRow?.recipient_avatar_key, "");
  const cleanMemberAvatarKey = safeText(memberAvatarKey, "");

  return (
    sameValue(cleanMemberAvatarKey, requesterAvatarKey) ||
    sameValue(cleanMemberAvatarKey, recipientAvatarKey)
  );
}

async function persistSelectedPartnership(memberId, partnershipUuid) {
  const cleanMemberId = safeText(memberId, "");
  const cleanUuid = safeUuid(partnershipUuid);

  if (!cleanMemberId || !cleanUuid) {
    throw new Error("Missing member_id or partnership_uuid.");
  }

  const { data, error } = await supabase.schema("partner").rpc("set_member_selected_partnership", {
    p_member_id: cleanMemberId,
    p_selected_partnership_id: cleanUuid
  });

  if (error) {
    throw new Error(`Failed to save selected partnership: ${error.message}`);
  }

  return data || null;
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "POST") {
    return json(405, {
      success: false,
      message: "Method not allowed"
    });
  }

  try {
    const cookieHeader =
      event.headers?.cookie ||
      event.headers?.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionToken = safeText(cookies[COOKIE_NAME], "");

    if (!sessionToken) {
      return json(401, {
        success: false,
        message: "Missing session token."
      });
    }

    const sessionRow = await loadSessionByToken(sessionToken);

    if (!sessionRow) {
      return json(401, {
        success: false,
        message: "Invalid session."
      });
    }

    if (isSessionInactive(sessionRow) || isSessionExpired(sessionRow)) {
      return json(401, {
        success: false,
        message: "Session expired."
      });
    }

    await touchWebsiteSession(sessionToken);

    const sessionAvatarKey = safeUuid(sessionRow.sl_avatar_key);

    if (!sessionAvatarKey) {
      return json(403, {
        success: false,
        message: "Session is missing a valid member avatar key."
      });
    }

    const memberRow = await loadMemberByAvatarKey(sessionAvatarKey);

    if (!memberRow) {
      return json(403, {
        success: false,
        message: "Member record not found."
      });
    }

    await touchPresence(sessionAvatarKey);

    const body = parseBody(event.body);
    const refs = extractPartnershipRefs(body);

    let partnershipRow = null;
    let source = "none";

    if (refs.partnership_uuid) {
      partnershipRow = await loadPartnershipByUuid(refs.partnership_uuid);
      source = "uuid";
    } else if (refs.partnership_id) {
      partnershipRow = await loadPartnershipByLegacyId(refs.partnership_id);
      source = "legacy_id";
    }

    if (!partnershipRow) {
      return json(404, {
        success: false,
        message: "Partnership not found."
      });
    }

    if (!memberCanAccessPartnership(partnershipRow, sessionAvatarKey)) {
      return json(403, {
        success: false,
        message: "You do not have permission to select this partnership."
      });
    }

    const partnershipUuid = safeUuid(partnershipRow.id);

    if (!partnershipUuid) {
      return json(500, {
        success: false,
        message: "Partnership is missing a valid UUID."
      });
    }

    await persistSelectedPartnership(memberRow.member_id, partnershipUuid);

    return json(200, {
      success: true,
      message: "Selected partnership saved successfully.",
      source,
      selected_partnership: {
        member_id: memberRow.member_id ?? null,
        partnership_id: partnershipRow.partnership_id ?? null,
        partnership_uuid: partnershipUuid,
        status: safeText(partnershipRow.status, "") || null,
        requester_avatar_key: safeText(partnershipRow.requester_avatar_key, "") || null,
        requester_username: safeText(partnershipRow.requester_username, "") || null,
        recipient_avatar_key: safeText(partnershipRow.recipient_avatar_key, "") || null,
        recipient_username: safeText(partnershipRow.recipient_username, "") || null,
        updated_at: safeText(partnershipRow.updated_at, "") || null
      }
    });
  } catch (error) {
    console.error("[set-selected-partnership] fatal error:", error);

    return json(500, {
      success: false,
      message: safeText(error?.message, "Failed to set selected partnership.")
    });
  }
};