const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const SESSION_COOKIE_NAME = "ap_session";
const SESSION_TABLE = "website_sessions";
const SESSION_TOKEN_COLUMN = "session_token";
const OPEN_PARTNERSHIP_STATUSES = ["pending", "active", "accepted"];

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
}

function safeText(value, fallback = "") {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function normalizeLower(value) {
  return safeText(value, "").toLowerCase();
}

function sameValue(a, b) {
  return normalizeLower(a) === normalizeLower(b);
}

function safeUuid(value) {
  const text = safeText(value, "");
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text)
    ? text
    : "";
}

function parseCookies(cookieHeader = "") {
  return cookieHeader
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .reduce((acc, pair) => {
      const index = pair.indexOf("=");
      if (index === -1) return acc;
      const key = pair.slice(0, index).trim();
      const value = pair.slice(index + 1).trim();
      acc[key] = decodeURIComponent(value);
      return acc;
    }, {});
}

function getSessionToken(cookieHeader = "") {
  const cookies = parseCookies(cookieHeader);
  return safeText(cookies[SESSION_COOKIE_NAME], "");
}

function parseBody(body) {
  if (!body) return {};
  try {
    return JSON.parse(body);
  } catch {
    return {};
  }
}

function sessionIsValid(sessionRow = {}) {
  const activeValue =
    sessionRow.is_active ??
    sessionRow.active ??
    sessionRow.session_active ??
    true;

  if (activeValue === false) return false;

  const expiresAt =
    sessionRow.expires_at ||
    sessionRow.expires_on ||
    sessionRow.session_expires_at ||
    null;

  if (!expiresAt) return true;

  const expiresMs = new Date(expiresAt).getTime();
  if (!Number.isFinite(expiresMs)) return true;

  return expiresMs > Date.now();
}

async function loadSessionRecord(sessionToken) {
  const { data, error } = await supabase
    .from(SESSION_TABLE)
    .select("*")
    .eq(SESSION_TOKEN_COLUMN, sessionToken)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load website session: ${error.message}`);
  }

  return data || null;
}

async function touchWebsiteSession(sessionToken) {
  if (!safeText(sessionToken, "")) return;

  const { error } = await supabase
    .from(SESSION_TABLE)
    .update({ updated_at: new Date().toISOString() })
    .eq(SESSION_TOKEN_COLUMN, sessionToken);

  if (error) {
    throw new Error(`Failed to update website session timestamp: ${error.message}`);
  }
}

async function loadMemberFromSession(sessionRow) {
  const avatarKey = safeText(sessionRow.sl_avatar_key, "");

  if (!avatarKey) {
    return null;
  }

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load member by avatar key: ${error.message}`);
  }

  return data || null;
}

async function loadMemberByAvatarKey(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", cleanKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partner by avatar key: ${error.message}`);
  }

  return data || null;
}

async function loadMemberByUsername(username) {
  const cleanUsername = safeText(username, "");
  if (!cleanUsername) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .ilike("sl_username", cleanUsername)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partner by username: ${error.message}`);
  }

  return data || null;
}

async function touchPresence(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return;

  const { error } = await supabase
    .from("cultivation_members")
    .update({ last_presence_at: new Date().toISOString() })
    .eq("sl_avatar_key", cleanKey);

  if (error) {
    throw new Error(`Failed to update member presence: ${error.message}`);
  }
}

async function loadOpenPartnershipsForMember(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return [];

  const { data, error } = await supabase
    .schema("partner")
    .from("cultivation_partnerships")
    .select("*")
    .or(`requester_avatar_key.eq.${cleanKey},recipient_avatar_key.eq.${cleanKey}`)
    .in("status", OPEN_PARTNERSHIP_STATUSES)
    .order("created_at", { ascending: false });

  if (error) {
    throw new Error(`Failed to load open partnerships: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

function findPairConflict(rows, currentAvatarKey, partnerAvatarKey) {
  return (
    rows.find((row) => {
      const requester = safeText(row.requester_avatar_key, "");
      const recipient = safeText(row.recipient_avatar_key, "");

      const samePair =
        (sameValue(requester, currentAvatarKey) && sameValue(recipient, partnerAvatarKey)) ||
        (sameValue(requester, partnerAvatarKey) && sameValue(recipient, currentAvatarKey));

      return samePair;
    }) || null
  );
}

function buildConflictMessage(conflict, currentAvatarKey) {
  if (!conflict) {
    return "A pending or active partnership already exists.";
  }

  const status = safeText(conflict.status, "").toLowerCase();

  if (status === "active" || status === "accepted") {
    return "You already have an active partnership with this member.";
  }

  const requestedByYou = sameValue(conflict.requester_avatar_key, currentAvatarKey);

  if (requestedByYou) {
    return "You already sent a pending partner request to this member.";
  }

  return "This member already sent you a pending partner request. Accept that request instead.";
}

function buildPartnerSummary(targetMember = {}) {
  return {
    member_id: targetMember.member_id ?? null,
    sl_avatar_key: safeText(targetMember.sl_avatar_key, "") || null,
    sl_username: safeText(targetMember.sl_username, "") || null,
    display_name: safeText(targetMember.display_name, "") || null,
    character_name: safeText(targetMember.character_name, "Unnamed Cultivator"),
    realm_display_name:
      safeText(targetMember.realm_display_name, "") ||
      safeText(targetMember.realm_name, "") ||
      "Mortal",
    path_type: safeText(targetMember.path_type, "") || null
  };
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "POST") {
    return json(405, {
      success: false,
      message: "Method not allowed"
    });
  }

  try {
    const cookieHeader = event.headers.cookie || event.headers.Cookie || "";
    const sessionToken = getSessionToken(cookieHeader);

    if (!sessionToken) {
      return json(401, {
        success: false,
        message: "Missing session"
      });
    }

    const sessionRow = await loadSessionRecord(sessionToken);

    if (!sessionRow || !sessionIsValid(sessionRow)) {
      return json(401, {
        success: false,
        message: "Invalid or expired session"
      });
    }

    await touchWebsiteSession(sessionToken);

    const currentMember = await loadMemberFromSession(sessionRow);

    if (!currentMember) {
      return json(404, {
        success: false,
        message: "Member not found"
      });
    }

    const body = parseBody(event.body);

    const partnerUsername =
      safeText(body.partner_username, "") ||
      safeText(body.partnerUsername, "") ||
      safeText(body.sl_username, "");

    const partnerAvatarKey =
      safeText(body.partner_avatar_key, "") ||
      safeText(body.partnerAvatarKey, "") ||
      safeText(body.avatar_key, "");

    if (!partnerUsername && !partnerAvatarKey) {
      return json(400, {
        success: false,
        message: "Provide either partner_username or partner_avatar_key"
      });
    }

    await touchPresence(currentMember.sl_avatar_key);

    let targetMember = null;

    if (partnerAvatarKey) {
      targetMember = await loadMemberByAvatarKey(partnerAvatarKey);
    }

    if (!targetMember && partnerUsername) {
      targetMember = await loadMemberByUsername(partnerUsername);
    }

    if (!targetMember) {
      return json(404, {
        success: false,
        message: "Partner not found in cultivation members"
      });
    }

    const currentAvatarKey = safeText(currentMember.sl_avatar_key, "");
    const currentUsername = safeText(currentMember.sl_username, "");
    const targetAvatarKey = safeText(targetMember.sl_avatar_key, "");
    const targetUsername = safeText(targetMember.sl_username, "");

    if (!currentAvatarKey || !targetAvatarKey) {
      return json(400, {
        success: false,
        message: "Both members must have a valid avatar key"
      });
    }

    if (sameValue(currentAvatarKey, targetAvatarKey)) {
      return json(400, {
        success: false,
        message: "You cannot send a partner request to yourself"
      });
    }

    const currentOpenRows = await loadOpenPartnershipsForMember(currentAvatarKey);
    const pairConflict = findPairConflict(
      currentOpenRows,
      currentAvatarKey,
      targetAvatarKey
    );

    if (pairConflict) {
      return json(409, {
        success: false,
        message: buildConflictMessage(pairConflict, currentAvatarKey),
        conflict: {
          partnership_id: pairConflict.partnership_id ?? null,
          partnership_uuid: safeUuid(pairConflict.id) || null,
          status: pairConflict.status,
          requester_avatar_key: pairConflict.requester_avatar_key,
          requester_username: pairConflict.requester_username,
          recipient_avatar_key: pairConflict.recipient_avatar_key,
          recipient_username: pairConflict.recipient_username
        }
      });
    }

    const insertPayload = {
      requester_avatar_key: currentAvatarKey,
      requester_username: currentUsername,
      recipient_avatar_key: targetAvatarKey,
      recipient_username: targetUsername,
      status: "pending"
    };

    const { data: insertedRow, error: insertError } = await supabase
      .schema("partner")
      .from("cultivation_partnerships")
      .insert(insertPayload)
      .select("*")
      .single();

    if (insertError) {
      return json(500, {
        success: false,
        message: "Failed to create partner request",
        error: insertError.message
      });
    }

    return json(200, {
      success: true,
      message: "Partner request sent successfully",
      partnership: {
        partnership_id: insertedRow.partnership_id ?? null,
        partnership_uuid: safeUuid(insertedRow.id) || null,
        status: insertedRow.status,
        requester_avatar_key: insertedRow.requester_avatar_key,
        requester_username: insertedRow.requester_username,
        recipient_avatar_key: insertedRow.recipient_avatar_key,
        recipient_username: insertedRow.recipient_username,
        created_at: insertedRow.created_at || null,
        updated_at: insertedRow.updated_at || null
      },
      partner: buildPartnerSummary(targetMember)
    });
  } catch (error) {
    console.error("send-partner-request error:", error);

    return json(500, {
      success: false,
      message: "Failed to send partner request",
      error: error.message
    });
  }
};