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
const WALLET_TABLE = "member_wallets";
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

function isColumnError(error) {
  const message = safeText(error?.message, "").toLowerCase();
  return message.includes("column") && message.includes("does not exist");
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

function getCultivationPoints(row) {
  return safeNumber(
    pickFirst(
      row?.vestiges,
      row?.vestiges_balance,
      row?.vestiges_total,
      row?.cp,
      row?.cp_total
    ),
    0
  );
}

function getQiCurrent(row) {
  return safeNumber(
    pickFirst(
      row?.auric_current,
      row?.current_qi
    ),
    0
  );
}

function getQiMaximum(row) {
  return safeNumber(
    pickFirst(
      row?.auric_maximum,
      row?.max_qi,
      row?.auric_cap
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

function getWalletBalance(row) {
  return safeNumber(
    pickFirst(
      row?.ascension_tokens_balance,
      row?.ascension_tokens,
      row?.token_balance
    ),
    0
  );
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

function buildPublicMember(row) {
  if (!row) return null;

  return {
    member_id: getMemberId(row) || null,
    sl_username: safeText(row.sl_username, ""),
    sl_avatar_key: getAvatarKey(row),
    character_name: safeText(row.character_name, ""),
    display_name: safeText(row.display_name, ""),
    path_type: safeText(row.path_type, "single"),
    realm_name: safeText(row.realm_name, ""),
    realm_display_name: getRealmDisplayName(row),
    vestiges: getCultivationPoints(row),
    auric_current: getQiCurrent(row),
    auric_maximum: getQiMaximum(row),
    mortal_energy: getMortalEnergy(row),
    last_hud_sync_at: safeText(row.last_hud_sync_at, ""),
    current_region_name: safeText(row.current_region_name, ""),
    current_position_x: hasValidPosition(row) ? safeNumber(row.current_position_x) : null,
    current_position_y: hasValidPosition(row) ? safeNumber(row.current_position_y) : null,
    current_position_z: hasValidPosition(row) ? safeNumber(row.current_position_z) : null
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

function getBondStageName(percent) {
  const p = clamp(safeNumber(percent, 0), 0, 100);

  if (p >= 100) return "Eternal Bond";
  if (p >= 80) return "Bond Spirit";
  if (p >= 60) return "Bond Soul";
  if (p >= 40) return "Bond Core";
  if (p >= 20) return "Bond Root";
  return "Bond Seed";
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function maxIso(...values) {
  const valid = values
    .map((value) => {
      const text = safeText(value, "");
      const ms = Date.parse(text);
      return Number.isFinite(ms) ? { text, ms } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.ms - a.ms);

  return valid.length ? valid[0].text : null;
}

function minIso(...values) {
  const valid = values
    .map((value) => {
      const text = safeText(value, "");
      const ms = Date.parse(text);
      return Number.isFinite(ms) ? { text, ms } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.ms - b.ms);

  return valid.length ? valid[0].text : null;
}

function getPairStateFromRows(selfRow, partnerRow) {
  const selfStatus = normalizeLower(selfRow?.status);
  const partnerStatus = normalizeLower(partnerRow?.status);

  const selfOffering = !!selfRow?.offering_complete;
  const partnerOffering = !!partnerRow?.offering_complete;
  const bothOffering = selfOffering && partnerOffering;

  const selfCompleted = selfStatus === "completed";
  const partnerCompleted = partnerStatus === "completed";
  const pairCompleted = selfCompleted && partnerCompleted;

  let displayState = "locked";
  let pauseReason = null;

  if (pairCompleted) {
    displayState = "pair_completed";
  } else if (selfCompleted || partnerCompleted) {
    displayState = "awaiting_partner_completion";
    pauseReason = "awaiting_partner_completion";
  } else if (selfStatus === "active" || partnerStatus === "active") {
    displayState = "active";
  } else if (selfStatus === "paused" || partnerStatus === "paused") {
    displayState = "paused";
    pauseReason = "participant_paused";
  } else if (bothOffering) {
    displayState = "ready_to_start";
  } else if (selfOffering || partnerOffering) {
    displayState = "waiting_for_partner_offering";
    pauseReason = "waiting_for_partner_offering";
  } else if (selfStatus === "available" || partnerStatus === "available") {
    displayState = "ready_for_offering";
  }

  return {
    self_status: selfStatus || null,
    partner_status: partnerStatus || null,
    self_offering_complete: selfOffering,
    partner_offering_complete: partnerOffering,
    both_offering_complete: bothOffering,
    self_completed: selfCompleted,
    partner_completed: partnerCompleted,
    pair_completed: pairCompleted,
    display_state: displayState,
    pause_reason: pauseReason
  };
}

function deriveBondSessionFromRows(rows, currentMemberId, partnerMemberId) {
  const sorted = (rows || [])
    .slice()
    .sort((a, b) => {
      const volumeDiff =
        safeNumber(a?.bond_volume_number, 0) - safeNumber(b?.bond_volume_number, 0);
      if (volumeDiff !== 0) return volumeDiff;

      return safeNumber(a?.bond_book_number, 0) - safeNumber(b?.bond_book_number, 0);
    });

  if (!sorted.length) return null;

  const grouped = new Map();

  for (const row of sorted) {
    const key = `${safeNumber(row?.bond_volume_number, 0)}:${safeNumber(row?.bond_book_number, 0)}`;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key).push(row);
  }

  let chosen = null;
  const groups = Array.from(grouped.values());

  for (const group of groups) {
    const selfRow = group.find((row) => safeText(row?.member_id, "") === safeText(currentMemberId, ""));
    const partnerRow = group.find((row) => safeText(row?.member_id, "") === safeText(partnerMemberId, ""));
    const pairState = getPairStateFromRows(selfRow, partnerRow);

    if (!pairState.pair_completed) {
      chosen = { selfRow, partnerRow, pairState };
      break;
    }
  }

  if (!chosen) {
    const lastGroup = groups[groups.length - 1] || [];
    const selfRow = lastGroup.find((row) => safeText(row?.member_id, "") === safeText(currentMemberId, ""));
    const partnerRow = lastGroup.find((row) => safeText(row?.member_id, "") === safeText(partnerMemberId, ""));
    const pairState = getPairStateFromRows(selfRow, partnerRow);
    chosen = { selfRow, partnerRow, pairState };
  }

  const selfRow = chosen.selfRow || null;
  const partnerRow = chosen.partnerRow || null;
  const pairState = chosen.pairState;

  return {
    id: null,
    partnership_uuid: safeText(selfRow?.partnership_uuid || partnerRow?.partnership_uuid, "") || null,
    status: pairState.display_state,
    pause_reason: pairState.pause_reason,
    partner_a_avatar_key: null,
    partner_b_avatar_key: null,
    bond_volume_number: safeNumber(selfRow?.bond_volume_number || partnerRow?.bond_volume_number, 0),
    bond_book_number: safeNumber(selfRow?.bond_book_number || partnerRow?.bond_book_number, 0),
    self_status: pairState.self_status,
    partner_status: pairState.partner_status,
    self_offering_complete: pairState.self_offering_complete,
    partner_offering_complete: pairState.partner_offering_complete,
    both_offering_complete: pairState.both_offering_complete,
    pair_completed: pairState.pair_completed,
    self_minutes_accumulated: safeNumber(selfRow?.minutes_accumulated, 0),
    partner_minutes_accumulated: safeNumber(partnerRow?.minutes_accumulated, 0),
    self_auric_accumulated: safeNumber(selfRow?.auric_accumulated, 0),
    partner_auric_accumulated: safeNumber(partnerRow?.auric_accumulated, 0),
    updated_at: maxIso(
      selfRow?.updated_at,
      selfRow?.last_progress_at,
      selfRow?.paused_at,
      selfRow?.completed_at,
      partnerRow?.updated_at,
      partnerRow?.last_progress_at,
      partnerRow?.paused_at,
      partnerRow?.completed_at
    ),
    created_at: minIso(
      selfRow?.created_at,
      selfRow?.offering_completed_at,
      selfRow?.started_at,
      partnerRow?.created_at,
      partnerRow?.offering_completed_at,
      partnerRow?.started_at
    )
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

async function loadMemberByUsername(username) {
  const cleanUsername = safeText(username, "");
  if (!cleanUsername) return null;

  const { data, error } = await supabase
    .from(MEMBER_TABLE)
    .select("*")
    .eq("sl_username", cleanUsername)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

async function resolvePartnerMember(avatarKey, username) {
  const byAvatar = await loadMemberByAvatarKey(avatarKey);
  if (byAvatar) return byAvatar;

  const byUsername = await loadMemberByUsername(username);
  if (byUsername) return byUsername;

  return null;
}

async function loadWalletByAvatarKey(avatarKey) {
  const validAvatarKey = safeUuid(avatarKey);
  if (!validAvatarKey) return null;

  const { data, error } = await supabase
    .from(WALLET_TABLE)
    .select("*")
    .eq("sl_avatar_key", validAvatarKey)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
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

async function loadFallbackPartnershipForMember(memberAvatarKey) {
  const cleanAvatarKey = safeUuid(memberAvatarKey);
  if (!cleanAvatarKey) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select("*")
    .or(`requester_avatar_key.eq.${cleanAvatarKey},recipient_avatar_key.eq.${cleanAvatarKey}`)
    .order("updated_at", { ascending: false })
    .order("created_at", { ascending: false })
    .limit(1);

  if (error) {
    throw error;
  }

  return Array.isArray(data) && data.length ? data[0] : null;
}

async function loadPartnerBond(partnershipUuid, legacyPartnershipId) {
  const cleanUuid = safeUuid(partnershipUuid);
  if (!cleanUuid) return null;

  const { data, error } = await partnerSupabase
    .from("partner_bonds")
    .select("*")
    .eq("partnership_id", cleanUuid)
    .maybeSingle();

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || null;
}

async function loadPartnerBondMemberBookRows(partnershipUuid, memberIds) {
  const cleanUuid = safeUuid(partnershipUuid);
  const cleanMemberIds = (memberIds || [])
    .map((id) => safeText(id, ""))
    .filter(Boolean);

  if (!cleanUuid || !cleanMemberIds.length) return [];

  const { data, error } = await partnerSupabase
    .from("partner_bond_member_book_states")
    .select("*")
    .eq("partnership_uuid", cleanUuid)
    .in("member_id", cleanMemberIds)
    .order("bond_volume_number", { ascending: true })
    .order("bond_book_number", { ascending: true });

  if (error && !isNoRowsError(error)) {
    throw error;
  }

  return data || [];
}

function buildEmptyPartner(counterpartAvatarKey, counterpartUsername) {
  return {
    member_id: null,
    sl_username: safeText(counterpartUsername, ""),
    sl_avatar_key: safeText(counterpartAvatarKey, ""),
    character_name: "",
    display_name: "",
    path_type: "single",
    realm_name: "",
    realm_display_name: "",
    vestiges: 0,
    auric_current: 0,
    auric_maximum: 0,
    mortal_energy: 0,
    last_hud_sync_at: "",
    current_region_name: "",
    current_position_x: null,
    current_position_y: null,
    current_position_z: null
  };
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

    const memberWallet = await loadWalletByAvatarKey(sessionAvatarKey);

    const body = parseBody(event.body);
    const query = event.queryStringParameters || {};
    const requestSource = { ...query, ...body };

    const requestedRefs = extractPartnershipRefs(requestSource);
    const memberId = getMemberId(memberRow);
    const selectedRow = await loadSelectedPartnershipRow(memberId);
    const selectedPartnershipUuid = safeUuid(selectedRow?.selected_partnership_id);

    let partnershipRow = null;
    let recordSource = "none";

    if (requestedRefs.partnership_uuid) {
      partnershipRow = await loadPartnershipByUuid(requestedRefs.partnership_uuid);
      recordSource = "explicit_uuid";
    } else if (requestedRefs.partnership_id) {
      partnershipRow = await loadPartnershipByLegacyId(requestedRefs.partnership_id);
      recordSource = "explicit_legacy_id";
    } else if (selectedPartnershipUuid) {
      partnershipRow = await loadPartnershipByUuid(selectedPartnershipUuid);
      recordSource = "selected";
    } else {
      partnershipRow = await loadFallbackPartnershipForMember(sessionAvatarKey);
      recordSource = "fallback_latest";
    }

    if (!partnershipRow) {
      return json(404, {
        success: false,
        message: "No partnership record found for this member."
      });
    }

    const memberRole = getMemberRoleInPartnership(partnershipRow, sessionAvatarKey);

    if (memberRole === "unknown") {
      return json(403, {
        success: false,
        message: "You do not have permission to view this partnership record."
      });
    }

    const counterpartIdentity = getCounterpartIdentity(partnershipRow, sessionAvatarKey);
    const counterpartAvatarKey = counterpartIdentity.counterpart_avatar_key;
    const counterpartUsername = counterpartIdentity.counterpart_username;

    const partnerMemberRow = await resolvePartnerMember(
      counterpartAvatarKey,
      counterpartUsername
    );

    const partnerWallet = partnerMemberRow
      ? await loadWalletByAvatarKey(getAvatarKey(partnerMemberRow))
      : null;

    const partnershipUuid = safeUuid(partnershipRow.id);
    const partnershipLegacyId = safeNumber(partnershipRow.partnership_id, 0) || null;
    const isCurrentFocus =
      Boolean(selectedPartnershipUuid && partnershipUuid) &&
      sameValue(selectedPartnershipUuid, partnershipUuid);

    const partnershipStatus = safeText(partnershipRow.status, "none").toLowerCase();
    const hasActivePartnership =
      partnershipStatus === "active" || partnershipStatus === "accepted";

    const partnerPresence = computePartnerPresence(
      memberRow,
      partnerMemberRow,
      hasActivePartnership
    );

    const vesselState = computeVesselMode(
      memberRow.path_type,
      hasActivePartnership,
      partnerPresence.partner_presence_active
    );

    const partnerMemberId = getMemberId(partnerMemberRow);
    const bondRow = partnershipUuid
      ? await loadPartnerBond(partnershipUuid, partnershipLegacyId)
      : null;

    const memberBookRows =
      partnershipUuid && memberId && partnerMemberId
        ? await loadPartnerBondMemberBookRows(partnershipUuid, [memberId, partnerMemberId])
        : [];

    const bondSessionRow = deriveBondSessionFromRows(
      memberBookRows,
      memberId,
      partnerMemberId
    );

    const memberPublic = buildPublicMember(memberRow);
    const partnerPublic = buildPublicMember(partnerMemberRow) ||
      buildEmptyPartner(counterpartAvatarKey, counterpartUsername);

    const personalCultivationPoints = getCultivationPoints(memberRow);
    const partnerCultivationPoints = getCultivationPoints(partnerMemberRow);
    const sharedCultivationPoints = personalCultivationPoints + partnerCultivationPoints;

    const personalAscensionTokens = getWalletBalance(memberWallet);
    const partnerAscensionTokens = getWalletBalance(partnerWallet);
    const sharedAscensionTokens = personalAscensionTokens + partnerAscensionTokens;

    const personalAuricCurrent = getQiCurrent(memberRow);
    const personalAuricMaximum = getQiMaximum(memberRow);
    const partnerAuricCurrent = getQiCurrent(partnerMemberRow);
    const partnerAuricMaximum = getQiMaximum(partnerMemberRow);

    const sharedAuricCurrent = personalAuricCurrent + partnerAuricCurrent;
    const sharedAuricMaximum = personalAuricMaximum + partnerAuricMaximum;

    const partner1Realm = getRealmDisplayName(memberRow);
    const partner2Realm = getRealmDisplayName(partnerMemberRow);
    const realmLine = buildRealmLine(memberRow, partnerMemberRow);

    const bondPercent = safeNumber(
      pickFirst(
        bondRow?.bond_percent,
        0
      ),
      0
    );

    return json(200, {
      success: true,
      message: "Partnership record loaded successfully.",

      record_source: recordSource,

      selected_partnership_uuid: selectedPartnershipUuid || null,
      current_record_partnership_uuid: partnershipUuid || null,
      current_record_partnership_id: partnershipLegacyId,
      is_current_focus: isCurrentFocus,

      member: memberPublic,
      partner: partnerPublic,

      partnership: {
        partnership_id: partnershipLegacyId,
        partnership_uuid: partnershipUuid || null,
        status: safeText(partnershipRow.status, ""),
        member_role: memberRole,
        is_current_focus: isCurrentFocus,
        requester_avatar_key: safeText(partnershipRow.requester_avatar_key, ""),
        requester_username: safeText(partnershipRow.requester_username, ""),
        recipient_avatar_key: safeText(partnershipRow.recipient_avatar_key, ""),
        recipient_username: safeText(partnershipRow.recipient_username, ""),
        created_at: safeText(partnershipRow.created_at, "") || null,
        accepted_at: safeText(partnershipRow.accepted_at, "") || null,
        rejected_at: safeText(partnershipRow.rejected_at, "") || null,
        removed_at: safeText(partnershipRow.removed_at, "") || null,
        updated_at: safeText(partnershipRow.updated_at, "") || null,
        can_accept: partnershipStatus === "pending" && memberRole === "recipient",
        can_deny:
          partnershipStatus === "pending" &&
          (memberRole === "recipient" || memberRole === "requester"),
        can_remove: ["pending", "active", "accepted"].includes(partnershipStatus)
      },

      bond: bondRow
        ? {
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
            ) || null,
            status: safeText(bondRow.status, "idle"),
            pause_reason: safeText(bondRow.pause_reason, "") || null,
            total_shared_minutes: safeNumber(bondRow.total_shared_minutes, 0),
            completed_books_count: safeNumber(bondRow.completed_books_count, 0),
            current_volume_id: safeText(bondRow.current_volume_id, "") || null,
            current_book_id: safeText(bondRow.current_book_id, "") || null,
            updated_at: safeText(bondRow.updated_at, "") || null
          }
        : {
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
          },

      bond_session: bondSessionRow
        ? {
            id: safeText(bondSessionRow.id, "") || null,
            partnership_uuid: safeUuid(bondSessionRow.partnership_uuid) || partnershipUuid || null,
            status: safeText(bondSessionRow.status, "") || null,
            display_state: safeText(bondSessionRow.status, "") || null,
            pause_reason: safeText(bondSessionRow.pause_reason, "") || null,
            partner_a_avatar_key: null,
            partner_b_avatar_key: null,
            bond_volume_number: safeNumber(bondSessionRow.bond_volume_number, 0),
            bond_book_number: safeNumber(bondSessionRow.bond_book_number, 0),
            self_status: safeText(bondSessionRow.self_status, "") || null,
            partner_status: safeText(bondSessionRow.partner_status, "") || null,
            self_offering_complete: !!bondSessionRow.self_offering_complete,
            partner_offering_complete: !!bondSessionRow.partner_offering_complete,
            both_offering_complete: !!bondSessionRow.both_offering_complete,
            pair_completed: !!bondSessionRow.pair_completed,
            self_minutes_accumulated: safeNumber(bondSessionRow.self_minutes_accumulated, 0),
            partner_minutes_accumulated: safeNumber(bondSessionRow.partner_minutes_accumulated, 0),
            self_auric_accumulated: safeNumber(bondSessionRow.self_auric_accumulated, 0),
            partner_auric_accumulated: safeNumber(bondSessionRow.partner_auric_accumulated, 0),
            updated_at: safeText(bondSessionRow.updated_at, "") || null,
            created_at: safeText(bondSessionRow.created_at, "") || null
          }
        : null,

      vessel_mode: vesselState.vessel_mode,
      live_vessel_state: vesselState.live_vessel_state,

      partner_presence_active: partnerPresence.partner_presence_active,
      partner_presence_status: partnerPresence.partner_presence_status,
      partner_presence_reason: partnerPresence.partner_presence_reason,
      partner_same_region: partnerPresence.partner_same_region,
      partner_hud_recent: partnerPresence.partner_hud_recent,
      partner_within_range: partnerPresence.partner_within_range,
      partner_distance_meters: partnerPresence.partner_distance_meters,

      personal_vestiges: personalCultivationPoints,
      partner_vestiges: partnerCultivationPoints,
      shared_vestiges: sharedCultivationPoints,

      personal_ascension_tokens: personalAscensionTokens,
      partner_ascension_tokens: partnerAscensionTokens,
      shared_ascension_tokens: sharedAscensionTokens,

      personal_auric_current: personalAuricCurrent,
      personal_auric_maximum: personalAuricMaximum,
      partner_auric_current: partnerAuricCurrent,
      partner_auric_maximum: partnerAuricMaximum,
      shared_auric_current: sharedAuricCurrent,
      shared_auric_maximum: sharedAuricMaximum,

      partner_1_mortal_energy: getMortalEnergy(memberRow),
      partner_2_mortal_energy: getMortalEnergy(partnerMemberRow),

      realm_line: realmLine,
      partner_1_realm: partner1Realm,
      partner_2_realm: partner2Realm,

      computed: {
        partnership_status: partnershipStatus,
        member_role: memberRole,
        realm_line: realmLine,
        partner_1_realm: partner1Realm,
        partner_2_realm: partner2Realm,
        vessel_mode: vesselState.vessel_mode,
        live_vessel_state: vesselState.live_vessel_state,
        partner_presence_active: partnerPresence.partner_presence_active,
        partner_presence_reason: partnerPresence.partner_presence_reason,
        has_bond_record: Boolean(bondRow),
        has_bond_session: Boolean(bondSessionRow),
        selected_partnership_uuid: selectedPartnershipUuid || null,
        current_record_partnership_uuid: partnershipUuid || null,
        is_current_focus: isCurrentFocus
      }
    });
  } catch (error) {
    console.error("[load-partnership-record] fatal error:", error);

    return json(500, {
      success: false,
      message: safeText(error?.message, "Failed to load partnership record.")
    });
  }
};