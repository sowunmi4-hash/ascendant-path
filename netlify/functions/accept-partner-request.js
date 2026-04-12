const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const SESSION_COOKIE_NAME = "ap_session";
const SESSION_TABLE = "website_sessions";
const SESSION_TOKEN_COLUMN = "session_token";
const MEMBER_TABLE = "cultivation_members";
const PARTNERSHIP_TABLE = "cultivation_partnerships";
const WALLET_TABLE = "member_wallets";

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

function extractPartnershipRefs(source) {
  const rawUuidCandidate = pickFirst(
    source?.partnership_uuid,
    source?.partnershipUuid,
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

function pickRealmName(member = {}) {
  return (
    safeText(member.realm_display_name, "") ||
    safeText(member.realm_name, "") ||
    "Mortal"
  );
}

function getWalletBalance(walletRow = {}, memberRow = {}) {
  const direct =
    walletRow?.ascension_tokens_balance ??
    walletRow?.ascension_tokens ??
    walletRow?.token_balance ??
    memberRow?.ascension_tokens_balance ??
    memberRow?.ascension_tokens ??
    memberRow?.token_balance ??
    0;

  return safeNumber(direct, 0);
}

function buildMemberSummary(member = null, wallet = null) {
  if (!member) return null;

  return {
    member_id: member.member_id ?? null,
    sl_avatar_key: safeText(member.sl_avatar_key, "") || null,
    sl_username: safeText(member.sl_username, "") || null,
    character_name: safeText(member.character_name, "Unnamed Cultivator"),
    display_name: safeText(member.display_name, "") || null,
    realm_display_name: pickRealmName(member),
    path_type: safeText(member.path_type, "") || null,
    mortal_energy: safeNumber(member.mortal_energy, 0),
    vestiges: safeNumber(member.vestiges, 0),
    ascension_tokens_balance: getWalletBalance(wallet, member),
    auric_current: safeNumber(member.auric_current, 0),
    auric_maximum: safeNumber(member.auric_maximum, 0)
  };
}

async function loadSessionRecord(sessionToken) {
  const { data, error } = await supabase
    .from(SESSION_TABLE)
    .select("*")
    .eq(SESSION_TOKEN_COLUMN, sessionToken)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
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
  if (!avatarKey) return null;

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw new Error(`Failed to load member by avatar key: ${error.message}`);
  }

  return data || null;
}

async function loadMemberByAvatarKey(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return null;

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .eq("sl_avatar_key", cleanKey)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw new Error(`Failed to load member by avatar key: ${error.message}`);
  }

  return data || null;
}

async function loadWalletByAvatarKey(avatarKey) {
  const cleanKey = safeText(avatarKey, "");
  if (!cleanKey) return null;

  const { data, error } = await supabase
    .from(WALLET_TABLE)
    .select("*")
    .eq("sl_avatar_key", cleanKey)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    return null;
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
    throw new Error(`Failed to load partnership by uuid: ${error.message}`);
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
    throw new Error(`Failed to load partnership by id: ${error.message}`);
  }

  return data || null;
}

async function persistSelectedPartnership(memberId, partnershipUuid) {
  const cleanMemberId = safeText(memberId, "");
  const cleanUuid = safeUuid(partnershipUuid);

  if (!cleanMemberId || !cleanUuid) return null;

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

    await touchPresence(currentMember.sl_avatar_key);

    const body = parseBody(event.body);
    const refs = extractPartnershipRefs(body);

    let partnership = null;
    let lookup_source = "none";

    if (refs.partnership_uuid) {
      partnership = await loadPartnershipByUuid(refs.partnership_uuid);
      lookup_source = "uuid";
    } else if (refs.partnership_id) {
      partnership = await loadPartnershipByLegacyId(refs.partnership_id);
      lookup_source = "legacy_id";
    }

    if (!partnership) {
      return json(404, {
        success: false,
        message: "Partnership request not found"
      });
    }

    const partnershipUuid = safeUuid(partnership.id);
    const partnershipId = partnership.partnership_id ?? null;
    const currentStatus = safeText(partnership.status, "").toLowerCase();

    if (currentStatus !== "pending") {
      return json(409, {
        success: false,
        message: "Only pending partner requests can be accepted",
        partnership: {
          partnership_id: partnershipId,
          partnership_uuid: partnershipUuid || null,
          status: partnership.status
        }
      });
    }

    const currentAvatarKey = safeText(currentMember.sl_avatar_key, "");
    const recipientAvatarKey = safeText(partnership.recipient_avatar_key, "");
    const requesterAvatarKey = safeText(partnership.requester_avatar_key, "");

    if (!sameValue(currentAvatarKey, recipientAvatarKey)) {
      return json(403, {
        success: false,
        message: "Only the recipient can accept this partner request"
      });
    }

    if (sameValue(currentAvatarKey, requesterAvatarKey)) {
      return json(400, {
        success: false,
        message: "Requester cannot accept their own partner request"
      });
    }

    const acceptedAt = new Date().toISOString();

    let updateQuery = supabase
      .schema("partner")
      .from(PARTNERSHIP_TABLE)
      .update({
        status: "active",
        accepted_at: acceptedAt
      })
      .eq("status", "pending");

    if (partnershipUuid) {
      updateQuery = updateQuery.eq("id", partnershipUuid);
    } else if (partnershipId) {
      updateQuery = updateQuery.eq("partnership_id", partnershipId);
    } else {
      return json(500, {
        success: false,
        message: "Partnership is missing both legacy and uuid identifiers"
      });
    }

    const { data: updatedRow, error: updateError } = await updateQuery
      .select("*")
      .single();

    if (updateError) {
      return json(500, {
        success: false,
        message: "Failed to accept partner request",
        error: updateError.message
      });
    }

    const updatedPartnershipUuid = safeUuid(updatedRow.id);

    if (updatedPartnershipUuid) {
      try {
        await persistSelectedPartnership(currentMember.member_id, updatedPartnershipUuid);
      } catch (selectionError) {
        console.error("accept-partner-request selection save warning:", selectionError.message);
      }
    }

    const requesterMember = await loadMemberByAvatarKey(updatedRow.requester_avatar_key);
    const recipientMember = await loadMemberByAvatarKey(updatedRow.recipient_avatar_key);

    const requesterWallet = requesterMember
      ? await loadWalletByAvatarKey(requesterMember.sl_avatar_key)
      : null;
    const recipientWallet = recipientMember
      ? await loadWalletByAvatarKey(recipientMember.sl_avatar_key)
      : null;

    const partner1 = buildMemberSummary(requesterMember, requesterWallet);
    const partner2 = buildMemberSummary(recipientMember, recipientWallet);

    const requesterCp = safeNumber(partner1?.vestiges, 0);
    const recipientCp = safeNumber(partner2?.vestiges, 0);

    const requesterAt = safeNumber(partner1?.ascension_tokens_balance, 0);
    const recipientAt = safeNumber(partner2?.ascension_tokens_balance, 0);

    const requesterQiCurrent = safeNumber(partner1?.auric_current, 0);
    const recipientQiCurrent = safeNumber(partner2?.auric_current, 0);
    const requesterQiMaximum = safeNumber(partner1?.auric_maximum, 0);
    const recipientQiMaximum = safeNumber(partner2?.auric_maximum, 0);

    return json(200, {
      success: true,
      message: "Partner request accepted successfully",
      lookup_source,
      selected_partnership_uuid: updatedPartnershipUuid || null,

      partnership: {
        partnership_id: updatedRow.partnership_id ?? null,
        partnership_uuid: updatedPartnershipUuid || null,
        status: updatedRow.status,
        requester_avatar_key: updatedRow.requester_avatar_key,
        requester_username: updatedRow.requester_username,
        recipient_avatar_key: updatedRow.recipient_avatar_key,
        recipient_username: updatedRow.recipient_username,
        created_at: updatedRow.created_at || null,
        accepted_at: updatedRow.accepted_at || null,
        updated_at: updatedRow.updated_at || null
      },

      partner_1: partner1,
      partner_2: partner2,

      computed: {
        partner_1_realm: safeText(partner1?.realm_display_name, "Mortal"),
        partner_2_realm: safeText(partner2?.realm_display_name, "Mortal"),
        realm_line: `Realms: ${safeText(partner1?.realm_display_name, "Mortal")} • ${safeText(partner2?.realm_display_name, "Mortal")}`,
        shared_vestiges: requesterCp + recipientCp,
        shared_ascension_tokens: requesterAt + recipientAt,
        shared_auric_current: requesterQiCurrent + recipientQiCurrent,
        shared_auric_maximum: requesterQiMaximum + recipientQiMaximum,
        partner_1_mortal_energy: safeNumber(partner1?.mortal_energy, 0),
        partner_2_mortal_energy: safeNumber(partner2?.mortal_energy, 0)
      }
    });
  } catch (error) {
    console.error("accept-partner-request error:", error);

    return json(500, {
      success: false,
      message: "Failed to accept partner request",
      error: error.message
    });
  }
};