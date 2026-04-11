const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const partnerSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "partner" }
  }
);

const COOKIE_NAME = "ap_session";
const SESSION_TABLE = "website_sessions";
const MEMBER_TABLE = "cultivation_members";
const PARTNERSHIP_TABLE = "cultivation_partnerships";
const MEMBER_SELECTED_PARTNERSHIPS_TABLE = "member_selected_partnerships";

const HUD_ACTIVE_WINDOW_MS = 60 * 1000;
const PARTNER_RANGE_METERS = 20;

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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
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

function isUuid(value) {
  const text = safeText(value, "");
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text);
}

function safeUuid(value) {
  const text = safeText(value, "");
  return isUuid(text) ? text : "";
}

function getMemberId(row) {
  return safeText(
    pickFirst(
      row?.member_id,
      row?.id
    ),
    ""
  );
}

function getAvatarKey(row) {
  return safeText(
    pickFirst(
      row?.sl_avatar_key,
      row?.avatar_key
    ),
    ""
  );
}

function getRealmDisplayName(row) {
  return safeText(
    pickFirst(
      row?.realm_display_name,
      row?.realm_name,
      row?.current_realm_name
    ),
    "Mortal Realm"
  );
}

function getCultivationPoints(row) {
  return safeNumber(
    pickFirst(
      row?.cultivation_points,
      row?.cultivation_points_balance,
      row?.cultivation_points_total,
      row?.cp,
      row?.cp_total
    ),
    0
  );
}

function getQiCurrent(row) {
  return safeNumber(
    pickFirst(
      row?.qi_current,
      row?.current_qi
    ),
    0
  );
}

function getQiMaximum(row) {
  return safeNumber(
    pickFirst(
      row?.qi_maximum,
      row?.max_qi,
      row?.qi_cap
    ),
    0
  );
}

function getMortalEnergy(row) {
  return safeNumber(
    pickFirst(
      row?.mortal_energy,
      row?.total_mortal_energy
    ),
    0
  );
}

function getBondStageName(percent) {
  const p = clamp(safeNumber(percent, 0), 0, 100);

  if (p >= 100) return "Eternal Bond";
  if (p >= 80) return "Bond Spirit";
  if (p >= 60) return "Bond Soul";
  if (p >= 40) return "Bond Core";
  if (p >= 20) return "Bond Root";
  return "Bond Seed";
}

function buildPublicMember(row) {
  if (!row) return null;

  const hasPosition =
    Number.isFinite(Number(row?.current_position_x)) &&
    Number.isFinite(Number(row?.current_position_y)) &&
    Number.isFinite(Number(row?.current_position_z));

  return {
    member_id: getMemberId(row) || null,
    sl_username: safeText(row?.sl_username, ""),
    sl_avatar_key: getAvatarKey(row) || null,
    character_name: safeText(row?.character_name, ""),
    display_name: safeText(row?.display_name, ""),
    path_type: safeText(row?.path_type, "single"),
    realm_name: safeText(row?.realm_name, ""),
    realm_display_name: getRealmDisplayName(row),
    cultivation_points: getCultivationPoints(row),
    qi_current: getQiCurrent(row),
    qi_maximum: getQiMaximum(row),
    mortal_energy: getMortalEnergy(row),
    last_hud_sync_at: safeText(row?.last_hud_sync_at, "") || null,
    current_region_name: safeText(row?.current_region_name, "") || null,
    current_position_x: hasPosition ? safeNumber(row?.current_position_x) : null,
    current_position_y: hasPosition ? safeNumber(row?.current_position_y) : null,
    current_position_z: hasPosition ? safeNumber(row?.current_position_z) : null
  };
}

function buildEmptyPartner(counterpartAvatarKey, counterpartUsername) {
  return {
    member_id: null,
    sl_username: safeText(counterpartUsername, ""),
    sl_avatar_key: safeText(counterpartAvatarKey, "") || null,
    character_name: "",
    display_name: "",
    path_type: "single",
    realm_name: "",
    realm_display_name: "",
    cultivation_points: 0,
    qi_current: 0,
    qi_maximum: 0,
    mortal_energy: 0,
    last_hud_sync_at: null,
    current_region_name: null,
    current_position_x: null,
    current_position_y: null,
    current_position_z: null
  };
}

function hasValidPosition(row) {
  return (
    Number.isFinite(Number(row?.current_position_x)) &&
    Number.isFinite(Number(row?.current_position_y)) &&
    Number.isFinite(Number(row?.current_position_z))
  );
}

function isHudRecent(row) {
  const raw = safeText(row?.last_hud_sync_at, "");
  if (!raw) return false;

  const time = Date.parse(raw);
  if (!Number.isFinite(time)) return false;

  return Date.now() - time <= HUD_ACTIVE_WINDOW_MS;
}

function sameRegion(a, b) {
  const regionA = safeText(a?.current_region_name, "").toLowerCase();
  const regionB = safeText(b?.current_region_name, "").toLowerCase();

  if (!regionA || !regionB) return false;
  return regionA === regionB;
}

function getDistanceMeters(a, b) {
  if (!hasValidPosition(a) || !hasValidPosition(b)) {
    return null;
  }

  const dx = safeNumber(a.current_position_x) - safeNumber(b.current_position_x);
  const dy = safeNumber(a.current_position_y) - safeNumber(b.current_position_y);
  const dz = safeNumber(a.current_position_z) - safeNumber(b.current_position_z);

  return Math.sqrt(dx * dx + dy * dy + dz * dz);
}

function computePartnerPresence(member, partner, hasActivePartnership) {
  if (!hasActivePartnership) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "no_active_partnership",
      partner_same_region: false,
      partner_hud_recent: false,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (!partner) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "no_partner_member",
      partner_same_region: false,
      partner_hud_recent: false,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  const partnerHudRecent = isHudRecent(partner);

  if (!partnerHudRecent) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "partner_hud_inactive",
      partner_same_region: false,
      partner_hud_recent: false,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (!safeText(member?.current_region_name, "")) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "missing_current_position",
      partner_same_region: false,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (!sameRegion(member, partner)) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "partner_other_region",
      partner_same_region: false,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (!hasValidPosition(member)) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "missing_current_position",
      partner_same_region: true,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (!hasValidPosition(partner)) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "partner_missing_position",
      partner_same_region: true,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  const distance = getDistanceMeters(member, partner);

  if (distance === null) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "partner_missing_position",
      partner_same_region: true,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: null
    };
  }

  if (distance > PARTNER_RANGE_METERS) {
    return {
      partner_presence_active: false,
      partner_presence_status: "inactive",
      partner_presence_reason: "partner_out_of_range",
      partner_same_region: true,
      partner_hud_recent: true,
      partner_within_range: false,
      partner_distance_meters: distance
    };
  }

  return {
    partner_presence_active: true,
    partner_presence_status: "active",
    partner_presence_reason: "partner_in_range",
    partner_same_region: true,
    partner_hud_recent: true,
    partner_within_range: true,
    partner_distance_meters: distance
  };
}

function computeVesselMode(pathType, hasActivePartnership, partnerPresenceActive) {
  const normalizedPathType = safeText(pathType, "single").toLowerCase();

  if (!hasActivePartnership) {
    return {
      vessel_mode: "solo",
      live_vessel_state: "solo"
    };
  }

  if (normalizedPathType === "hybrid") {
    if (partnerPresenceActive) {
      return {
        vessel_mode: "hybrid",
        live_vessel_state: "hybrid_linked"
      };
    }

    return {
      vessel_mode: "solo",
      live_vessel_state: "hybrid_self_only"
    };
  }

  if (normalizedPathType === "dual") {
    if (partnerPresenceActive) {
      return {
        vessel_mode: "dual",
        live_vessel_state: "dual_active"
      };
    }

    return {
      vessel_mode: "dual",
      live_vessel_state: "dual_inactive"
    };
  }

  return {
    vessel_mode: "solo",
    live_vessel_state: "solo"
  };
}

function getMemberRoleInPartnership(partnershipRow, currentAvatarKey) {
  const requesterAvatarKey = safeText(partnershipRow?.requester_avatar_key, "");
  const recipientAvatarKey = safeText(partnershipRow?.recipient_avatar_key, "");

  if (sameValue(currentAvatarKey, requesterAvatarKey)) return "requester";
  if (sameValue(currentAvatarKey, recipientAvatarKey)) return "recipient";
  return "unknown";
}

function getCounterpartIdentity(partnershipRow, currentAvatarKey) {
  const role = getMemberRoleInPartnership(partnershipRow, currentAvatarKey);

  if (role === "requester") {
    return {
      counterpart_avatar_key: safeText(partnershipRow?.recipient_avatar_key, ""),
      counterpart_username: safeText(partnershipRow?.recipient_username, ""),
      member_role: role
    };
  }

  return {
    counterpart_avatar_key: safeText(partnershipRow?.requester_avatar_key, ""),
    counterpart_username: safeText(partnershipRow?.requester_username, ""),
    member_role: role
  };
}

function buildRealmLine(member, partner) {
  const memberRealm = getRealmDisplayName(member);
  const partnerRealm = partner ? getRealmDisplayName(partner) : null;

  if (!partnerRealm) {
    return `Realm: ${memberRealm}`;
  }

  return `${memberRealm} • ${partnerRealm}`;
}

function buildBondSummary(bondRow, partnershipUuid) {
  const bondPercent = safeNumber(
    pickFirst(
      bondRow?.bond_percent,
      0
    ),
    0
  );

  if (!bondRow) {
    return {
      partnership_uuid: partnershipUuid || null,
      bond_percent: 0,
      current_stage_name: "Bond Seed",
      status: "idle",
      pause_reason: null,
      total_shared_minutes: 0,
      completed_books_count: 0,
      current_volume_id: null,
      current_book_id: null,
      updated_at: null
    };
  }

  return {
    partnership_uuid:
      safeUuid(
        pickFirst(
          bondRow.partnership_uuid,
          bondRow.partnership_id
        )
      ) || partnershipUuid || null,
    bond_percent: bondPercent,
    current_stage_name: safeText(
      pickFirst(
        bondRow.current_stage_name,
        bondRow.bond_stage_name,
        bondRow.stage_name
      ),
      getBondStageName(bondPercent)
    ),
    status: safeText(bondRow.status, "idle"),
    pause_reason: safeText(bondRow.pause_reason, "") || null,
    total_shared_minutes: safeNumber(bondRow.total_shared_minutes, 0),
    completed_books_count: safeNumber(bondRow.completed_books_count, 0),
    current_volume_id: safeText(bondRow.current_volume_id, "") || null,
    current_book_id: safeText(bondRow.current_book_id, "") || null,
    updated_at: safeText(bondRow.updated_at, "") || null
  };
}

function sortNewestFirst(rows) {
  return (rows || []).slice().sort((a, b) => {
    const aTime = Date.parse(
      safeText(
        pickFirst(a?.updated_at, a?.accepted_at, a?.created_at, a?.rejected_at, a?.removed_at),
        ""
      )
    );
    const bTime = Date.parse(
      safeText(
        pickFirst(b?.updated_at, b?.accepted_at, b?.created_at, b?.rejected_at, b?.removed_at),
        ""
      )
    );

    const aMs = Number.isFinite(aTime) ? aTime : 0;
    const bMs = Number.isFinite(bTime) ? bTime : 0;

    return bMs - aMs;
  });
}

function buildListItem({
  partnershipRow,
  memberRow,
  partnerRow,
  selectedPartnershipUuid,
  currentAvatarKey,
  bondRow
}) {
  const partnershipUuid = safeUuid(partnershipRow?.id) || null;
  const partnershipLegacyId = safeNumber(partnershipRow?.partnership_id, 0) || null;
  const partnershipStatus = safeText(partnershipRow?.status, "").toLowerCase();
  const memberRole = getMemberRoleInPartnership(partnershipRow, currentAvatarKey);
  const counterpartIdentity = getCounterpartIdentity(partnershipRow, currentAvatarKey);

  const isCurrentFocus =
    Boolean(selectedPartnershipUuid && partnershipUuid) &&
    sameValue(selectedPartnershipUuid, partnershipUuid);

  const hasActivePartnership =
    partnershipStatus === "active" || partnershipStatus === "accepted";

  const partnerPublic =
    buildPublicMember(partnerRow) ||
    buildEmptyPartner(
      counterpartIdentity.counterpart_avatar_key,
      counterpartIdentity.counterpart_username
    );

  const memberPublic = buildPublicMember(memberRow);
  const partnerPresence = computePartnerPresence(
    memberRow,
    partnerRow,
    hasActivePartnership
  );

  const vesselState = computeVesselMode(
    memberRow?.path_type,
    hasActivePartnership,
    partnerPresence.partner_presence_active
  );

  const realmLine = buildRealmLine(memberRow, partnerRow);
  const bond = buildBondSummary(bondRow, partnershipUuid);

  return {
    partnership_id: partnershipLegacyId,
    partnership_uuid: partnershipUuid,
    status: partnershipStatus,
    member_role: memberRole,
    is_current_focus: isCurrentFocus,

    requester_avatar_key: safeText(partnershipRow?.requester_avatar_key, "") || null,
    requester_username: safeText(partnershipRow?.requester_username, "") || null,
    recipient_avatar_key: safeText(partnershipRow?.recipient_avatar_key, "") || null,
    recipient_username: safeText(partnershipRow?.recipient_username, "") || null,

    created_at: safeText(partnershipRow?.created_at, "") || null,
    accepted_at: safeText(partnershipRow?.accepted_at, "") || null,
    rejected_at: safeText(partnershipRow?.rejected_at, "") || null,
    removed_at: safeText(partnershipRow?.removed_at, "") || null,
    updated_at: safeText(partnershipRow?.updated_at, "") || null,

    can_accept: partnershipStatus === "pending" && memberRole === "recipient",
    can_deny:
      partnershipStatus === "pending" &&
      (memberRole === "recipient" || memberRole === "requester"),
    can_remove: ["pending", "active", "accepted"].includes(partnershipStatus),

    member: memberPublic,
    partner: partnerPublic,
    counterpart: partnerPublic,

    realm_line: realmLine,
    partner_realm: safeText(partnerPublic?.realm_display_name, "") || null,

    partner_presence_active: partnerPresence.partner_presence_active,
    partner_presence_status: partnerPresence.partner_presence_status,
    partner_presence_reason: partnerPresence.partner_presence_reason,
    partner_same_region: partnerPresence.partner_same_region,
    partner_hud_recent: partnerPresence.partner_hud_recent,
    partner_within_range: partnerPresence.partner_within_range,
    partner_distance_meters: partnerPresence.partner_distance_meters,

    vessel_mode: vesselState.vessel_mode,
    live_vessel_state: vesselState.live_vessel_state,

    bond,

    summary: {
      partner_name:
        safeText(partnerPublic?.display_name, "") ||
        safeText(partnerPublic?.character_name, "") ||
        safeText(partnerPublic?.sl_username, "") ||
        "Unknown Partner",
      realm_line: realmLine,
      bond_stage_name: safeText(bond?.current_stage_name, "Bond Seed"),
      bond_percent: safeNumber(bond?.bond_percent, 0),
      presence_status: partnerPresence.partner_presence_status
    }
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

async function loadMembersByAvatarKeys(avatarKeys) {
  const cleanKeys = Array.from(
    new Set((avatarKeys || []).map(safeUuid).filter(Boolean))
  );

  if (!cleanKeys.length) return [];

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .in("sl_avatar_key", cleanKeys);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadMembersByUsernames(usernames) {
  const cleanUsernames = Array.from(
    new Set((usernames || []).map((value) => safeText(value, "")).filter(Boolean))
  );

  if (!cleanUsernames.length) return [];

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .in("sl_username", cleanUsernames);

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadSelectedPartnershipRow(memberId) {
  const cleanMemberId = safeText(memberId, "");
  if (!cleanMemberId) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(MEMBER_SELECTED_PARTNERSHIPS_TABLE)
    .select("*")
    .eq("member_id", cleanMemberId)
    .order("updated_at", { ascending: false })
    .limit(1);

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return Array.isArray(data) && data.length ? data[0] : null;
}

async function loadPartnershipRowsForMember(memberAvatarKey) {
  const cleanAvatarKey = safeUuid(memberAvatarKey);
  if (!cleanAvatarKey) return [];

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select("*")
    .or(`requester_avatar_key.eq.${cleanAvatarKey},recipient_avatar_key.eq.${cleanAvatarKey}`)
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false });

  if (error) {
    throw error;
  }

  return data || [];
}

async function loadPartnerBondsForPartnerships(partnershipUuids) {
  const cleanUuids = Array.from(
    new Set((partnershipUuids || []).map(safeUuid).filter(Boolean))
  );

  if (!cleanUuids.length) return [];

  const { data, error } = await partnerSupabase
    .from("partner_bonds")
    .select("*")
    .in("partnership_id", cleanUuids);

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || [];
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
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

    const sessionAvatarKey = safeUuid(sessionRow?.sl_avatar_key);

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

    const memberId = getMemberId(memberRow);
    const selectedRow = await loadSelectedPartnershipRow(memberId);
    const selectedPartnershipUuid = safeUuid(selectedRow?.selected_partnership_id) || null;

    const partnershipRows = await loadPartnershipRowsForMember(sessionAvatarKey);

    const counterpartAvatarKeys = [];
    const counterpartUsernames = [];

    for (const row of partnershipRows) {
      const counterpart = getCounterpartIdentity(row, sessionAvatarKey);

      const avatarKey = safeUuid(counterpart?.counterpart_avatar_key);
      const username = safeText(counterpart?.counterpart_username, "");

      if (avatarKey) counterpartAvatarKeys.push(avatarKey);
      if (username) counterpartUsernames.push(username);
    }

    const counterpartMembersByAvatar = await loadMembersByAvatarKeys(counterpartAvatarKeys);
    const counterpartMembersByUsername = await loadMembersByUsernames(counterpartUsernames);

    const memberByAvatarKey = new Map();
    const memberByUsername = new Map();

    for (const row of counterpartMembersByAvatar) {
      const avatarKey = safeUuid(getAvatarKey(row));
      if (avatarKey) memberByAvatarKey.set(avatarKey, row);
    }

    for (const row of counterpartMembersByUsername) {
      const username = safeText(row?.sl_username, "").toLowerCase();
      if (username) memberByUsername.set(username, row);
    }

    const partnershipUuids = partnershipRows
      .map((row) => safeUuid(row?.id))
      .filter(Boolean);

    const partnerBondRows = await loadPartnerBondsForPartnerships(partnershipUuids);
    const bondByPartnershipUuid = new Map();

    for (const row of partnerBondRows) {
      const key =
        safeUuid(
          pickFirst(
            row?.partnership_uuid,
            row?.partnership_id
          )
        );

      if (key) {
        bondByPartnershipUuid.set(key, row);
      }
    }

    const activePartnerships = [];
    const incomingRequests = [];
    const outgoingRequests = [];
    const historicalPartnerships = [];

    for (const partnershipRow of partnershipRows) {
      const status = safeText(partnershipRow?.status, "").toLowerCase();
      const memberRole = getMemberRoleInPartnership(partnershipRow, sessionAvatarKey);

      if (memberRole === "unknown") {
        continue;
      }

      const counterpart = getCounterpartIdentity(partnershipRow, sessionAvatarKey);
      const counterpartAvatarKey = safeUuid(counterpart?.counterpart_avatar_key);
      const counterpartUsername = safeText(counterpart?.counterpart_username, "").toLowerCase();

      const partnerRow =
        (counterpartAvatarKey && memberByAvatarKey.get(counterpartAvatarKey)) ||
        (counterpartUsername && memberByUsername.get(counterpartUsername)) ||
        null;

      const partnershipUuid = safeUuid(partnershipRow?.id) || null;
      const bondRow = partnershipUuid
        ? bondByPartnershipUuid.get(partnershipUuid) || null
        : null;

      const item = buildListItem({
        partnershipRow,
        memberRow,
        partnerRow,
        selectedPartnershipUuid,
        currentAvatarKey: sessionAvatarKey,
        bondRow
      });

      if (status === "active" || status === "accepted") {
        activePartnerships.push(item);
      } else if (status === "pending" && memberRole === "recipient") {
        incomingRequests.push(item);
      } else if (status === "pending" && memberRole === "requester") {
        outgoingRequests.push(item);
      } else {
        historicalPartnerships.push(item);
      }
    }

    const sortedActive = sortNewestFirst(activePartnerships).sort((a, b) => {
      if (a.is_current_focus && !b.is_current_focus) return -1;
      if (!a.is_current_focus && b.is_current_focus) return 1;
      return 0;
    });

    const sortedIncoming = sortNewestFirst(incomingRequests);
    const sortedOutgoing = sortNewestFirst(outgoingRequests);
    const sortedHistorical = sortNewestFirst(historicalPartnerships);

    const currentFocusPartnership =
      sortedActive.find((row) => row.is_current_focus) ||
      sortedIncoming.find((row) => row.is_current_focus) ||
      sortedOutgoing.find((row) => row.is_current_focus) ||
      sortedHistorical.find((row) => row.is_current_focus) ||
      null;

    const railPartnerships = [
      ...sortedActive,
      ...sortedIncoming,
      ...sortedOutgoing,
      ...sortedHistorical
    ];

    return json(200, {
      success: true,
      message: "Partnership list loaded successfully.",

      selected_partnership_uuid: selectedPartnershipUuid,
      current_focus_partnership_uuid: currentFocusPartnership?.partnership_uuid || null,
      current_focus_partnership_id: currentFocusPartnership?.partnership_id || null,

      member: buildPublicMember(memberRow),

      counts: {
        total: railPartnerships.length,
        active: sortedActive.length,
        incoming: sortedIncoming.length,
        outgoing: sortedOutgoing.length,
        historical: sortedHistorical.length
      },

      active_partnerships: sortedActive,
      incoming_requests: sortedIncoming,
      outgoing_requests: sortedOutgoing,
      historical_partnerships: sortedHistorical,

      active: sortedActive,
      pending_incoming: sortedIncoming,
      pending_outgoing: sortedOutgoing,
      historical: sortedHistorical,

      rail_partnerships: railPartnerships,

      has_active_partnership: sortedActive.length > 0,
      has_selected_partnership: Boolean(selectedPartnershipUuid),
      has_current_focus: Boolean(currentFocusPartnership),

      computed: {
        total_partnerships: railPartnerships.length,
        active_count: sortedActive.length,
        incoming_count: sortedIncoming.length,
        outgoing_count: sortedOutgoing.length,
        historical_count: sortedHistorical.length,
        selected_partnership_uuid: selectedPartnershipUuid,
        current_focus_partnership_uuid: currentFocusPartnership?.partnership_uuid || null
      }
    });
  } catch (error) {
    console.error("[load-partnership-list] fatal error:", error);

    return json(500, {
      success: false,
      message: safeText(error?.message, "Failed to load partnership list.")
    });
  }
};