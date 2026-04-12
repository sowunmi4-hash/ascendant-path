const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const PARTNERSHIP_TABLE = "cultivation_partnerships";
const MEMBER_SELECTED_PARTNERSHIPS_TABLE = "member_selected_partnerships";
const HUD_RECENT_WINDOW_MS = 60 * 1000;
const PARTNER_RANGE_METERS = 20;
const SESSION_COOKIE_NAME = process.env.SESSION_COOKIE_NAME || "ap_session";

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  cookieHeader.split(";").forEach((pair) => {
    const [name, ...rest] = pair.trim().split("=");
    if (name) cookies[name.trim()] = decodeURIComponent(rest.join("=").trim());
  });
  return cookies;
}

async function loadSession(sessionToken) {
  if (!sessionToken) return null;

  const { data, error } = await supabase
    .from("website_sessions")
    .select("*")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    console.error("Failed to load session:", error.message);
    return null;
  }

  return data || null;
}

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

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalize(value) {
  return safeText(value).toLowerCase().replace(/\s+/g, "");
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function pickFirstText(source, keys) {
  for (const key of keys) {
    const value = safeText(source?.[key]);
    if (value) return value;
  }
  return "";
}

function pickFirstNumber(source, keys) {
  for (const key of keys) {
    const raw = source?.[key];
    if (raw === null || raw === undefined || raw === "") continue;
    const number = Number(raw);
    if (Number.isFinite(number)) return number;
  }
  return null;
}

function getMemberPrimaryId(member) {
  return member?.member_id || member?.id || null;
}

function getBondTitle(member) {
  return pickFirstText(member, [
    "bond_title",
    "bond_profile_title",
    "current_bond_title",
    "bond_stage_title"
  ]);
}

function deriveCultivationMode(member) {
  const status = safeLower(member?.v2_cultivation_status);
  if (status === "cultivating") return "personal";
  if (status === "in_breakthrough") return "breakthrough";
  return "idle";
}

function buildLiveHudState(requestSource, member, nowIso) {
  const incomingRegionName = pickFirstText(requestSource, [
    "current_region_name",
    "region_name",
    "region"
  ]);

  const incomingX = pickFirstNumber(requestSource, [
    "current_position_x",
    "position_x",
    "x"
  ]);

  const incomingY = pickFirstNumber(requestSource, [
    "current_position_y",
    "position_y",
    "y"
  ]);

  const incomingZ = pickFirstNumber(requestSource, [
    "current_position_z",
    "position_z",
    "z"
  ]);

  const hasIncomingPosition =
    incomingRegionName !== "" &&
    incomingX !== null &&
    incomingY !== null &&
    incomingZ !== null;

  const updates = {
    last_hud_sync_at: nowIso,
    last_presence_at: nowIso,
    updated_at: nowIso
  };

  if (hasIncomingPosition) {
    updates.current_region_name = incomingRegionName;
    updates.current_position_x = incomingX;
    updates.current_position_y = incomingY;
    updates.current_position_z = incomingZ;
  }

  return {
    hasIncomingPosition,
    updates,
    mergedRegionName: hasIncomingPosition
      ? incomingRegionName
      : safeText(member?.current_region_name, ""),
    mergedX: hasIncomingPosition
      ? incomingX
      : toNumberOrNull(member?.current_position_x),
    mergedY: hasIncomingPosition
      ? incomingY
      : toNumberOrNull(member?.current_position_y),
    mergedZ: hasIncomingPosition
      ? incomingZ
      : toNumberOrNull(member?.current_position_z)
  };
}

function detectMeditationActive(member) {
  return safeLower(member?.v2_cultivation_status) === "cultivating";
}

function normalizePartnershipRow(row) {
  if (!row) return null;

  return {
    ...row,
    id: safeText(row.id)
  };
}

function isMemberOfPartnership(partnershipRow, member) {
  if (!partnershipRow || !member) return false;

  const selfAvatar = normalize(member?.sl_avatar_key);
  const selfUsername = normalize(member?.sl_username);

  const requesterAvatar = normalize(partnershipRow?.requester_avatar_key);
  const requesterUsername = normalize(partnershipRow?.requester_username);
  const recipientAvatar = normalize(partnershipRow?.recipient_avatar_key);
  const recipientUsername = normalize(partnershipRow?.recipient_username);

  return (
    (selfAvatar && (selfAvatar === requesterAvatar || selfAvatar === recipientAvatar)) ||
    (selfUsername && (selfUsername === requesterUsername || selfUsername === recipientUsername))
  );
}

function getPartnerIdentityFromPartnership(partnershipRow, member) {
  if (!partnershipRow) {
    return {
      partner_avatar_key: null,
      partner_sl_username: null
    };
  }

  const selfAvatar = safeText(member?.sl_avatar_key);
  const selfUsername = safeLower(member?.sl_username);

  const requesterAvatar = safeText(partnershipRow.requester_avatar_key);
  const requesterUsername = safeText(partnershipRow.requester_username);
  const recipientAvatar = safeText(partnershipRow.recipient_avatar_key);
  const recipientUsername = safeText(partnershipRow.recipient_username);

  const selfIsRequester =
    (selfAvatar && normalize(selfAvatar) === normalize(requesterAvatar)) ||
    (selfUsername && normalize(selfUsername) === normalize(requesterUsername));

  return selfIsRequester
    ? {
        partner_avatar_key: recipientAvatar || null,
        partner_sl_username: recipientUsername || null
      }
    : {
        partner_avatar_key: requesterAvatar || null,
        partner_sl_username: requesterUsername || null
      };
}

function computeDistanceMeters(selfMember, partnerMember) {
  const selfRegion = safeText(selfMember?.current_region_name);
  const partnerRegion = safeText(partnerMember?.current_region_name);

  if (!selfRegion || !partnerRegion) return null;
  if (normalize(selfRegion) !== normalize(partnerRegion)) return null;

  const selfX = toNumberOrNull(selfMember?.current_position_x);
  const selfY = toNumberOrNull(selfMember?.current_position_y);
  const selfZ = toNumberOrNull(selfMember?.current_position_z);

  const partnerX = toNumberOrNull(partnerMember?.current_position_x);
  const partnerY = toNumberOrNull(partnerMember?.current_position_y);
  const partnerZ = toNumberOrNull(partnerMember?.current_position_z);

  if (
    selfX === null || selfY === null || selfZ === null ||
    partnerX === null || partnerY === null || partnerZ === null
  ) {
    return null;
  }

  const dx = selfX - partnerX;
  const dy = selfY - partnerY;
  const dz = selfZ - partnerZ;

  return Math.sqrt(dx * dx + dy * dy + dz * dz);
}

function isRecentIso(iso, nowMs) {
  const clean = safeText(iso);
  if (!clean) return false;

  const time = new Date(clean).getTime();
  if (!Number.isFinite(time)) return false;

  return nowMs - time <= HUD_RECENT_WINDOW_MS;
}

function doResolvedPartnershipsMatch(a, b) {
  const aUuid = safeText(a?.partnership_uuid);
  const bUuid = safeText(b?.partnership_uuid);

  if (aUuid && bUuid) {
    return aUuid === bUuid;
  }

  const aLegacyId = toNumberOrNull(a?.partnership_id);
  const bLegacyId = toNumberOrNull(b?.partnership_id);

  if (aLegacyId !== null && bLegacyId !== null) {
    return aLegacyId === bLegacyId;
  }

  return false;
}

function getPartnerFocusReason(selfResolvedPartnership, partnerResolvedPartnership) {
  if (!partnerResolvedPartnership?.partnership) {
    if (partnerResolvedPartnership?.selected_partnership_required) {
      return "partner_selected_partnership_required";
    }
    if (partnerResolvedPartnership?.selected_partnership_missing) {
      return "partner_selected_partnership_missing";
    }
    if (partnerResolvedPartnership?.selected_partnership_inactive) {
      return "partner_selected_partnership_inactive";
    }
    if (partnerResolvedPartnership?.selected_partnership_invalid) {
      return "partner_selected_partnership_invalid";
    }
    return "partner_no_active_partnership";
  }

  if (!doResolvedPartnershipsMatch(selfResolvedPartnership, partnerResolvedPartnership)) {
    return "partner_focus_mismatch";
  }

  return "mutual_focus_ready";
}

function getResonanceReason({
  hasPartnership,
  selectedPartnershipRequired,
  selectedPartnershipMissing,
  selectedPartnershipInactive,
  selectedPartnershipInvalid,
  partnerFound,
  mutualFocusReady,
  mutualFocusReason,
  partnerHudRecent,
  partnerSameRegion,
  partnerWithinRange,
  selfMeditating,
  partnerMeditating,
  resonanceActive
}) {
  if (!hasPartnership) return "no_active_partnership";
  if (selectedPartnershipRequired) return "selected_partnership_required";
  if (selectedPartnershipMissing) return "selected_partnership_missing";
  if (selectedPartnershipInactive) return "selected_partnership_inactive";
  if (selectedPartnershipInvalid) return "selected_partnership_invalid";
  if (!partnerFound) return "partner_member_missing";
  if (!mutualFocusReady) return mutualFocusReason || "mutual_focus_required";
  if (!partnerHudRecent) return "partner_not_recent";
  if (!partnerSameRegion) return "partner_different_region";
  if (!partnerWithinRange) return "partner_out_of_range";
  if (!selfMeditating) return "self_not_meditating";
  if (!partnerMeditating) return "partner_not_meditating";
  if (resonanceActive) return "resonating";
  return "idle";
}

async function loadMemberByAvatarKey(slAvatarKey) {
  const cleanAvatarKey = safeText(slAvatarKey);
  if (!cleanAvatarKey) return null;

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", cleanAvatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function loadMemberByIdentity({ slAvatarKey, slUsername }) {
  const cleanAvatarKey = safeText(slAvatarKey);
  const cleanUsername = safeText(slUsername);

  if (!cleanAvatarKey && !cleanUsername) return null;

  let query = supabase
    .from("cultivation_members")
    .select("*")
    .limit(1);

  if (cleanAvatarKey) {
    query = query.eq("sl_avatar_key", cleanAvatarKey);
  } else {
    query = query.eq("sl_username", cleanUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function persistLiveHudState(slAvatarKey, updates) {
  const { data, error } = await supabase
    .from("cultivation_members")
    .update(updates)
    .eq("sl_avatar_key", slAvatarKey)
    .select("*")
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to persist HUD live state: ${error.message}`);
  }

  return data || null;
}

async function saveSelectedPartnership(memberId, partnershipUuid) {
  if (!memberId || !isUuid(partnershipUuid)) return false;

  const { error } = await supabase.schema("partner").rpc("set_member_selected_partnership", {
    p_member_id: memberId,
    p_selected_partnership_id: partnershipUuid
  });

  if (error) {
    throw new Error(`Failed to save selected partnership: ${error.message}`);
  }

  return true;
}

async function loadSelectedPartnershipReference(memberId) {
  if (!memberId) return "";

  const { data, error } = await supabase
    .schema("partner")
    .from(MEMBER_SELECTED_PARTNERSHIPS_TABLE)
    .select("*")
    .eq("member_id", memberId)
    .order("updated_at", { ascending: false })
    .limit(2);

  if (error) {
    throw new Error(`Failed to load selected partnership: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(`Multiple selected partnership rows found for member ${memberId}.`);
  }

  const row = rows[0] || null;
  if (!row) return "";

  return (
    safeText(row.selected_partnership_id) ||
    safeText(row.partnership_id) ||
    safeText(row.selected_partnership_uuid) ||
    safeText(row.partnership_uuid) ||
    ""
  );
}

async function loadPartnershipByUuid(partnershipUuid) {
  const cleanUuid = safeText(partnershipUuid);
  if (!isUuid(cleanUuid)) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      rejected_at,
      updated_at
    `)
    .eq("id", cleanUuid)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by UUID: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadPartnershipByLegacyId(legacyPartnershipId) {
  const cleanId = Number(legacyPartnershipId);
  if (!Number.isInteger(cleanId) || cleanId <= 0) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      rejected_at,
      updated_at
    `)
    .eq("partnership_id", cleanId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by legacy id: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadActivePartnershipRowsForMember(member) {
  const avatarKey = safeText(member?.sl_avatar_key);
  const username = safeText(member?.sl_username);

  if (!avatarKey && !username) return [];

  let query = supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      requester_username,
      recipient_avatar_key,
      recipient_username,
      status,
      created_at,
      accepted_at,
      rejected_at,
      updated_at
    `)
    .eq("status", "active")
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false });

  if (avatarKey) {
    query = query.or(
      `requester_avatar_key.eq.${avatarKey},recipient_avatar_key.eq.${avatarKey}`
    );
  } else if (username) {
    query = query.or(
      `requester_username.eq.${username},recipient_username.eq.${username}`
    );
  }

  const { data, error } = await query;

  if (error) {
    throw new Error(`Failed to load active partnerships: ${error.message}`);
  }

  return Array.isArray(data) ? data.map(normalizePartnershipRow) : [];
}

async function resolveFocusedPartnership(member, requestedPartnershipUuid) {
  const activeRows = await loadActivePartnershipRowsForMember(member);

  const result = {
    partnership: null,
    partnership_uuid: null,
    partnership_id: null,
    source: null,
    has_active_partnership: activeRows.length > 0,
    has_multiple_active_partnerships: activeRows.length > 1,
    selected_partnership_required: false,
    selected_partnership_found: false,
    selected_partnership_missing: false,
    selected_partnership_inactive: false,
    selected_partnership_invalid: false,
    selected_reference: null
  };

  if (requestedPartnershipUuid) {
    const row = await loadPartnershipByUuid(requestedPartnershipUuid);

    if (row && safeLower(row.status) === "active" && isMemberOfPartnership(row, member)) {
      result.partnership = row;
      result.partnership_uuid = row.id;
      result.partnership_id = toNumberOrNull(row.partnership_id);
      result.source = "explicit_partnership_uuid";
      return result;
    }

    result.selected_partnership_invalid = true;
    return result;
  }

  const memberId = getMemberPrimaryId(member);
  const selectedReference = await loadSelectedPartnershipReference(memberId);
  result.selected_reference = selectedReference || null;

  if (selectedReference) {
    result.selected_partnership_found = true;

    let selectedRow = null;
    if (isUuid(selectedReference)) {
      selectedRow = await loadPartnershipByUuid(selectedReference);
    } else {
      selectedRow = await loadPartnershipByLegacyId(selectedReference);
    }

    if (!selectedRow) {
      result.selected_partnership_missing = true;
      return result;
    }

    if (safeLower(selectedRow.status) !== "active") {
      result.selected_partnership_inactive = true;
      return result;
    }

    if (!isMemberOfPartnership(selectedRow, member)) {
      result.selected_partnership_invalid = true;
      return result;
    }

    result.partnership = selectedRow;
    result.partnership_uuid = selectedRow.id;
    result.partnership_id = toNumberOrNull(selectedRow.partnership_id);
    result.source = "selected_partnership";
    return result;
  }

  if (activeRows.length === 1) {
    result.partnership = activeRows[0];
    result.partnership_uuid = activeRows[0].id;
    result.partnership_id = toNumberOrNull(activeRows[0].partnership_id);
    result.source = "single_active_fallback";
    return result;
  }

  if (activeRows.length > 1) {
    result.selected_partnership_required = true;
  }

  return result;
}

function buildFallbackResponse({
  memberForResponse,
  requestedPartnershipUuid,
  resolvedPartnership,
  resonanceReason,
  mutualFocusReady = false,
  mutualFocusReason = null,
  partnerFocusedPartnershipUuid = null,
  partnerFocusedPartnershipId = null
}) {
  const memberBondTitle = getBondTitle(memberForResponse);
  const selfMeditating = safeLower(memberForResponse.v2_cultivation_status) === "cultivating";

  return buildResponse(200, {
    success: true,
    message: "Resonance state loaded successfully.",
    sl_avatar_key: safeText(memberForResponse.sl_avatar_key),
    sl_username: safeText(memberForResponse.sl_username),
    meditation_active: selfMeditating,
    cultivation_mode: deriveCultivationMode(memberForResponse),
    bond_title: memberBondTitle || null,
    bond_title_unlocked: memberBondTitle !== "",
    resonance_active: false,
    resonance_partner_username: "",
    resonance_reason: resonanceReason,
    mutual_focus_ready: mutualFocusReady,
    mutual_focus_reason: mutualFocusReason,
    partner_focused_partnership_uuid: partnerFocusedPartnershipUuid,
    partner_focused_partnership_id: partnerFocusedPartnershipId,
    partner_presence_active: false,
    partner_within_range: false,
    partner_same_region: false,
    partner_hud_recent: false,
    partner_meditating: false,
    partner_cultivation_mode: "idle",
    partner_bond_title: null,
    partner_bond_title_unlocked: false,
    partner_distance_meters: null,
    partner_range_limit_meters: PARTNER_RANGE_METERS,
    shared_presence_ready: false,
    shared_meditation_ready: false,
    meditation_mode: "normal",
    auric_rate_per_minute: 1,
    vestiges_rate_per_minute: (member && member.v2_cultivation_status === 'in_breakthrough') ? 0 : 1,
    auric_interval_seconds: 60,
    vestiges_interval_seconds: 60,
    bond_runtime_active: false,
    bond_session_status: null,
    bond_volume_number: null,
    bond_book_number: null,
    focused_partnership_uuid:
      safeText(resolvedPartnership?.partnership_uuid) ||
      requestedPartnershipUuid ||
      null,
    focused_partnership_id: toNumberOrNull(resolvedPartnership?.partnership_id),
    current_region_name: safeText(memberForResponse.current_region_name),
    current_position_x: toNumberOrNull(memberForResponse.current_position_x),
    current_position_y: toNumberOrNull(memberForResponse.current_position_y),
    current_position_z: toNumberOrNull(memberForResponse.current_position_z),
    last_hud_sync_at: memberForResponse.last_hud_sync_at || null
  });
}

const handler = async (event) => {
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

    // --- Session cookie auth ---
    const cookies = parseCookies(event.headers?.cookie || "");
    const sessionToken = cookies[SESSION_COOKIE_NAME] || "";
    const session = sessionToken ? await loadSession(sessionToken) : null;

    // Determine sl_avatar_key: prefer session, fall back to body/query (HUD sends it)
    let slAvatarKey = safeText(requestSource.sl_avatar_key);
    const bodyAvatarKey = slAvatarKey; // preserve original for verification

    if (session?.sl_avatar_key) {
      // If both session and body provide an avatar key, verify they match
      if (bodyAvatarKey && normalize(bodyAvatarKey) !== normalize(session.sl_avatar_key)) {
        return buildResponse(403, {
          success: false,
          message: "Session avatar key does not match request avatar key."
        });
      }
      slAvatarKey = safeText(session.sl_avatar_key);
    }

    const slUsername = safeText(requestSource.sl_username);
    const requestedPartnershipUuid = safeText(
      requestSource.partnership_uuid || requestSource.selected_partnership_uuid
    );

    if (!slAvatarKey) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: sl_avatar_key"
      });
    }

    if (requestedPartnershipUuid && !isUuid(requestedPartnershipUuid)) {
      return buildResponse(400, {
        success: false,
        message: "partnership_uuid is invalid."
      });
    }

    const member = await loadMemberByAvatarKey(slAvatarKey);

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "No cultivation profile found for this SL avatar."
      });
    }

    if (
      slUsername &&
      safeText(member.sl_username) &&
      normalize(slUsername) !== normalize(member.sl_username)
    ) {
      return buildResponse(403, {
        success: false,
        message: "Username does not match cultivation profile."
      });
    }

    const nowIso = new Date().toISOString();
    const nowMs = Date.now();

    const liveHudState = buildLiveHudState(requestSource, member, nowIso);

    const memberForResponse =
      (await persistLiveHudState(slAvatarKey, {
        ...liveHudState.updates,
        ...(liveHudState.hasIncomingPosition
          ? {
              current_region_name: liveHudState.updates.current_region_name,
              current_position_x: liveHudState.updates.current_position_x,
              current_position_y: liveHudState.updates.current_position_y,
              current_position_z: liveHudState.updates.current_position_z
            }
          : {})
      })) || member;

    if (requestedPartnershipUuid) {
      const memberPrimaryId = getMemberPrimaryId(memberForResponse);
      if (memberPrimaryId) {
        await saveSelectedPartnership(memberPrimaryId, requestedPartnershipUuid);
      }
    }

    const resolvedPartnership = await resolveFocusedPartnership(
      memberForResponse,
      requestedPartnershipUuid
    );

    if (!resolvedPartnership.partnership) {
      let reason = "no_active_partnership";

      if (resolvedPartnership.selected_partnership_required) {
        reason = "selected_partnership_required";
      } else if (resolvedPartnership.selected_partnership_missing) {
        reason = "selected_partnership_missing";
      } else if (resolvedPartnership.selected_partnership_inactive) {
        reason = "selected_partnership_inactive";
      } else if (resolvedPartnership.selected_partnership_invalid) {
        reason = "selected_partnership_invalid";
      }

      return buildFallbackResponse({
        memberForResponse,
        requestedPartnershipUuid,
        resolvedPartnership,
        resonanceReason: reason,
        mutualFocusReady: false,
        mutualFocusReason: reason
      });
    }

    const partnerIdentity = getPartnerIdentityFromPartnership(
      resolvedPartnership.partnership,
      memberForResponse
    );

    const partnerMember = await loadMemberByIdentity({
      slAvatarKey: partnerIdentity.partner_avatar_key,
      slUsername: partnerIdentity.partner_sl_username
    });

    if (!partnerMember) {
      return buildFallbackResponse({
        memberForResponse,
        requestedPartnershipUuid,
        resolvedPartnership,
        resonanceReason: "partner_member_missing",
        mutualFocusReady: false,
        mutualFocusReason: "partner_member_missing"
      });
    }

    const partnerResolvedPartnership = await resolveFocusedPartnership(
      partnerMember,
      ""
    );

    const mutualFocusReason = getPartnerFocusReason(
      resolvedPartnership,
      partnerResolvedPartnership
    );

    const mutualFocusReady = mutualFocusReason === "mutual_focus_ready";

    const selfMeditating = detectMeditationActive(memberForResponse);
    const partnerMeditating = detectMeditationActive(partnerMember);

    const selfRegion = safeText(memberForResponse.current_region_name);
    const partnerRegion = safeText(partnerMember.current_region_name);
    const partnerSameRegion =
      !!selfRegion &&
      !!partnerRegion &&
      normalize(selfRegion) === normalize(partnerRegion);

    const partnerDistanceRaw = computeDistanceMeters(memberForResponse, partnerMember);
    const partnerWithinRange =
      partnerDistanceRaw !== null && partnerDistanceRaw <= PARTNER_RANGE_METERS;

    const partnerHudRecent = isRecentIso(
      partnerMember.last_hud_sync_at || partnerMember.last_presence_at,
      nowMs
    );

    const partnerPresenceActive =
      partnerHudRecent &&
      partnerSameRegion &&
      partnerWithinRange;

    const resonanceActive =
      mutualFocusReady &&
      selfMeditating &&
      partnerPresenceActive &&
      partnerMeditating;

    const resonanceReason = getResonanceReason({
      hasPartnership: true,
      selectedPartnershipRequired: false,
      selectedPartnershipMissing: false,
      selectedPartnershipInactive: false,
      selectedPartnershipInvalid: false,
      partnerFound: true,
      mutualFocusReady,
      mutualFocusReason,
      partnerHudRecent,
      partnerSameRegion,
      partnerWithinRange,
      selfMeditating,
      partnerMeditating,
      resonanceActive
    });

    const memberBondTitle = getBondTitle(memberForResponse);
    const partnerBondTitle = getBondTitle(partnerMember);

    return buildResponse(200, {
      success: true,
      message: "Resonance state loaded successfully.",

      sl_avatar_key: safeText(memberForResponse.sl_avatar_key),
      sl_username: safeText(memberForResponse.sl_username),

      meditation_active: selfMeditating,
      cultivation_mode: deriveCultivationMode(memberForResponse),

      bond_title: memberBondTitle || null,
      bond_title_unlocked: memberBondTitle !== "",

      resonance_active: resonanceActive,
      resonance_partner_username: safeText(partnerMember.sl_username),
      resonance_reason: resonanceReason,

      mutual_focus_ready: mutualFocusReady,
      mutual_focus_reason: mutualFocusReason,
      partner_focused_partnership_uuid:
        safeText(partnerResolvedPartnership.partnership_uuid) || null,
      partner_focused_partnership_id:
        toNumberOrNull(partnerResolvedPartnership.partnership_id),

      partner_presence_active: partnerPresenceActive,
      partner_within_range: partnerWithinRange,
      partner_same_region: partnerSameRegion,
      partner_hud_recent: partnerHudRecent,
      partner_meditating: partnerMeditating,
      partner_cultivation_mode: deriveCultivationMode(partnerMember),
      partner_bond_title: partnerBondTitle || null,
      partner_bond_title_unlocked: partnerBondTitle !== "",
      partner_distance_meters:
        partnerDistanceRaw === null ? null : Number(partnerDistanceRaw.toFixed(2)),
      partner_range_limit_meters: PARTNER_RANGE_METERS,

      shared_presence_ready: partnerPresenceActive,
      shared_meditation_ready:
        mutualFocusReady &&
        selfMeditating &&
        partnerPresenceActive &&
        partnerMeditating,

      meditation_mode: resonanceActive ? "resonance" : "normal",

      auric_rate_per_minute: resonanceActive ? 2 : 1,
      vestiges_rate_per_minute: (member && member.v2_cultivation_status === 'in_breakthrough') ? 0 : (resonanceActive ? 2 : 1),

      auric_interval_seconds: resonanceActive ? 30 : 60,
      vestiges_interval_seconds: 60,

      bond_runtime_active: false,
      bond_session_status: null,
      bond_volume_number: null,
      bond_book_number: null,

      focused_partnership_uuid: safeText(resolvedPartnership.partnership_uuid) || null,
      focused_partnership_id: toNumberOrNull(resolvedPartnership.partnership_id),

      current_region_name: safeText(memberForResponse.current_region_name),
      current_position_x: toNumberOrNull(memberForResponse.current_position_x),
      current_position_y: toNumberOrNull(memberForResponse.current_position_y),
      current_position_z: toNumberOrNull(memberForResponse.current_position_z),
      last_hud_sync_at: memberForResponse.last_hud_sync_at || null
    });
  } catch (error) {
    console.error("load-resonance server error:", error);

    return buildResponse(500, {
      success: false,
      message: "Server error",
      error: error.message
    });
  }
};

module.exports = { handler };