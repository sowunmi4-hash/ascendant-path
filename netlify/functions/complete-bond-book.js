const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const RUNTIME_VIEW = "partner_bond_runtime_state_view";
const RUNTIME_TABLE = "partner_bond_runtime_states";

const SESSION_STATUS_IDLE = "idle";
const SESSION_STATUS_PAUSED = "paused";
const PAUSE_REASON_AWAITING_COMPLETION = "awaiting_book_completion";

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

function parseCookies(cookieHeader = "") {
  const cookies = {};

  cookieHeader.split(";").forEach((part) => {
    const trimmed = part.trim();
    if (!trimmed) return;

    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) return;

    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();

    cookies[key] = decodeURIComponent(value);
  });

  return cookies;
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  const text = safeLower(value);
  return ["true", "1", "yes", "y", "on", "active"].includes(text);
}

function parsePositiveInteger(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
}

function firstFilled(...values) {
  for (const value of values) {
    const text = safeText(value);
    if (text) return text;
  }
  return "";
}

function round4(value) {
  return Number(safeNumber(value, 0).toFixed(4));
}

function normalizeIdentifier(value) {
  return safeLower(value).replace(/\s+/g, "");
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function isColumnMissingError(error) {
  return safeLower(error?.message).includes("column");
}

function getRuntimePauseReason(runtime) {
  return safeLower(
    runtime?.pause_reason ||
      runtime?.session_pause_reason ||
      runtime?.bond_pause_reason
  );
}

function getMemberPrimaryId(member) {
  return member?.member_id || member?.id || null;
}

function unwrapRpcPayload(data, functionName) {
  if (!data) return null;

  if (Array.isArray(data)) {
    if (!data.length) return null;
    const first = data[0];
    if (
      first &&
      typeof first === "object" &&
      functionName &&
      Object.prototype.hasOwnProperty.call(first, functionName)
    ) {
      return first[functionName];
    }
    return first;
  }

  if (
    data &&
    typeof data === "object" &&
    functionName &&
    Object.prototype.hasOwnProperty.call(data, functionName)
  ) {
    return data[functionName];
  }

  return data;
}

function getPartnerRole(activePartnership, selfAvatarKey, selfUsername) {
  const requesterAvatar = normalizeIdentifier(activePartnership?.requester_avatar_key);
  const requesterUsername = normalizeIdentifier(activePartnership?.requester_username);
  const recipientAvatar = normalizeIdentifier(activePartnership?.recipient_avatar_key);
  const recipientUsername = normalizeIdentifier(activePartnership?.recipient_username);

  const selfAvatar = normalizeIdentifier(selfAvatarKey);
  const selfUser = normalizeIdentifier(selfUsername);

  if (
    (selfAvatar && selfAvatar === requesterAvatar) ||
    (selfUser && selfUser === requesterUsername)
  ) {
    return "partner_a";
  }

  if (
    (selfAvatar && selfAvatar === recipientAvatar) ||
    (selfUser && selfUser === recipientUsername)
  ) {
    return "partner_b";
  }

  return "";
}

function resolveSessionLeader(activePartnership, runtime = null) {
  const runtimeRole = safeLower(
    runtime?.leader_role || runtime?.session_leader_role
  );
  const runtimeAvatar = safeText(
    runtime?.leader_avatar_key || runtime?.session_leader_avatar_key
  );
  const runtimeUsername = safeLower(
    runtime?.leader_username || runtime?.session_leader_username
  );

  if (runtimeRole) {
    return {
      role: runtimeRole,
      avatar_key: runtimeAvatar || null,
      username: runtimeUsername || null
    };
  }

  return {
    role: "partner_a",
    avatar_key: safeText(activePartnership?.requester_avatar_key) || null,
    username: safeLower(activePartnership?.requester_username) || null
  };
}

function buildSessionRolePayload(activePartnership, currentPartnerRole, runtime = null) {
  const sessionLeader = resolveSessionLeader(activePartnership, runtime);
  const isSessionLeader = currentPartnerRole === sessionLeader.role;

  return {
    session_leader_role: sessionLeader.role,
    session_leader_avatar_key: sessionLeader.avatar_key,
    session_leader_username: sessionLeader.username,
    is_session_leader: isSessionLeader,
    session_display_role: isSessionLeader ? "leader" : "mirror",
    official_sync_allowed: isSessionLeader
  };
}

function pickBondBookStatus(row) {
  return safeLower(firstFilled(row?.status, row?.book_status, "locked"));
}

function computeBookProgressPercent(row) {
  const requiredMinutes = Math.max(
    1,
    safeNumber(row?.required_shared_minutes, 0)
  );
  const sharedMinutes = Math.max(
    0,
    safeNumber(row?.shared_minutes_accumulated, 0)
  );

  return round4((sharedMinutes / requiredMinutes) * 100);
}

function findNextBookAfter(bookStates, volumeNumber, bookNumber) {
  const rows = Array.isArray(bookStates) ? bookStates : [];

  return (
    rows.find((row) => {
      return (
        safeNumber(row?.bond_volume_number, 0) === safeNumber(volumeNumber, 0) &&
        safeNumber(row?.bond_book_number, 0) === safeNumber(bookNumber, 0) + 1
      );
    }) || null
  );
}

function findAnyActionableBook(bookStates) {
  const rows = Array.isArray(bookStates) ? bookStates : [];

  return (
    rows.find((row) => ["active", "paused", "available"].includes(pickBondBookStatus(row))) ||
    rows.find((row) => pickBondBookStatus(row) === "locked") ||
    null
  );
}

async function loadMember(slAvatarKey, slUsername) {
  let query = supabase
    .from("cultivation_members")
    .select(`
      member_id,
      sl_avatar_key,
      sl_username,
      display_name,
      qi_current,
      cultivation_points,
      v2_cultivation_status,
      v2_active_stage_key,
      v2_breakthrough_gate_open
    `)
    .limit(1);

  if (slAvatarKey) {
    query = query.eq("sl_avatar_key", slAvatarKey);
  } else if (slUsername) {
    query = query.eq("sl_username", slUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  if (!data) return null;

  return {
    ...data,
    id: data.member_id || null
  };
}

async function loadSessionRow(sessionToken) {
  const { data, error } = await supabase
    .from("website_sessions")
    .select("*")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load website session: ${error.message}`);
  }

  return data || null;
}

async function loadMemberFromSessionToken(sessionToken) {
  const sessionRow = await loadSessionRow(sessionToken);
  if (!sessionRow) return { sessionRow: null, member: null };

  const member = await loadMember(
    safeText(sessionRow.sl_avatar_key),
    safeText(sessionRow.sl_username)
  );

  return { sessionRow, member };
}

async function touchSessionAndPresence(sessionToken, member) {
  const now = new Date().toISOString();
  const work = [];

  if (sessionToken) {
    work.push(
      supabase
        .from("website_sessions")
        .update({ updated_at: now })
        .eq("session_token", sessionToken)
    );
  }

  if (member?.sl_avatar_key) {
    work.push(
      supabase
        .from("cultivation_members")
        .update({ last_presence_at: now })
        .eq("sl_avatar_key", member.sl_avatar_key)
    );
  }

  if (work.length) {
    await Promise.all(work);
  }
}

async function loadSelectedPartnershipUuid(memberId) {
  if (!memberId) return null;

  const { data, error } = await supabase
    .schema("partner")
    .from("member_selected_partnerships")
    .select("selected_partnership_id, updated_at")
    .eq("member_id", memberId)
    .order("updated_at", { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(`Failed to load selected partnership: ${error.message}`);
  }

  if (!Array.isArray(data) || !data.length) return null;

  return safeText(data[0]?.selected_partnership_id) || null;
}

async function saveSelectedPartnership(memberId, partnershipUuid) {
  if (!memberId || !partnershipUuid || !isUuid(partnershipUuid)) return false;

  const { error } = await supabase.schema("partner").rpc("set_member_selected_partnership", {
    p_member_id: memberId,
    p_selected_partnership_id: partnershipUuid
  });

  if (error) {
    console.error("saveSelectedPartnership error:", error);
    return false;
  }

  return true;
}

async function loadActivePartnershipRowsForMember(member) {
  const avatarKey = safeText(member?.sl_avatar_key);
  const username = safeText(member?.sl_username);

  if (!avatarKey && !username) return [];

  let query = supabase
    .schema("partner")
    .from("cultivation_partnerships")
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

  return Array.isArray(data) ? data : [];
}

async function loadActivePartnershipByUuidForMember(member, partnershipUuid) {
  const cleanUuid = safeText(partnershipUuid);
  if (!isUuid(cleanUuid)) return null;

  const avatarKey = safeText(member?.sl_avatar_key);
  const username = safeText(member?.sl_username);

  let query = supabase
    .schema("partner")
    .from("cultivation_partnerships")
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
    .eq("status", "active")
    .limit(1);

  if (avatarKey) {
    query = query.or(
      `requester_avatar_key.eq.${avatarKey},recipient_avatar_key.eq.${avatarKey}`
    );
  } else if (username) {
    query = query.or(
      `requester_username.eq.${username},recipient_username.eq.${username}`
    );
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load active partnership by UUID: ${error.message}`);
  }

  return data || null;
}

async function loadActivePartnershipByLegacyIdForMember(member, legacyPartnershipId) {
  const cleanId = parsePositiveInteger(legacyPartnershipId);
  if (!cleanId) return null;

  const avatarKey = safeText(member?.sl_avatar_key);
  const username = safeText(member?.sl_username);

  let query = supabase
    .schema("partner")
    .from("cultivation_partnerships")
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
    .eq("status", "active")
    .limit(1);

  if (avatarKey) {
    query = query.or(
      `requester_avatar_key.eq.${avatarKey},recipient_avatar_key.eq.${avatarKey}`
    );
  } else if (username) {
    query = query.or(
      `requester_username.eq.${username},recipient_username.eq.${username}`
    );
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load active partnership by legacy id: ${error.message}`);
  }

  return data || null;
}

function resolveFocusActivePartnership({
  explicitPartnershipRow,
  selectedPartnershipRow,
  activeRows
}) {
  if (explicitPartnershipRow) return explicitPartnershipRow;
  if (selectedPartnershipRow) return selectedPartnershipRow;
  if (Array.isArray(activeRows) && activeRows.length) return activeRows[0];
  return null;
}

async function loadBondRuntimeView(partnershipUuid) {
  const cleanId = safeText(partnershipUuid);
  if (!isUuid(cleanId)) return null;

  const { error: initError } = await supabase.rpc(
    "get_partner_bond_runtime_state",
    {
      p_partnership_uuid: cleanId
    }
  );

  if (initError) {
    throw new Error(`Failed to initialize partner bond runtime state: ${initError.message}`);
  }

  const { data, error } = await supabase
    .from(RUNTIME_VIEW)
    .select("*")
    .eq("partnership_uuid", cleanId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load bond runtime state view: ${error.message}`);
  }

  return data || null;
}

async function loadBondBookCatalog(bookId) {
  const { data, error } = await supabase
    .schema("partner")
    .from("bond_books")
    .select("*")
    .eq("id", bookId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load bond book catalog: ${error.message}`);
  }

  return data || null;
}

async function loadBondBookStateById(bookStateId) {
  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_book_states")
    .select("*")
    .eq("id", bookStateId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partner bond book state: ${error.message}`);
  }

  return data || null;
}

async function loadPartnerBondRowByUuid(partnershipUuid) {
  const { data, error } = await supabase
    .from("partner_bonds")
    .select("*")
    .eq("partnership_uuid", partnershipUuid)
    .limit(1)
    .maybeSingle();

  if (!error && data) return data;

  if (error && !isColumnMissingError(error)) {
    throw new Error(`Failed to load partner bond row: ${error.message}`);
  }

  const fallback = await supabase
    .from("partner_bonds")
    .select("*")
    .eq("partnership_id", partnershipUuid)
    .limit(1)
    .maybeSingle();

  if (fallback.error) {
    throw new Error(`Failed to load partner bond row fallback: ${fallback.error.message}`);
  }

  return fallback.data || null;
}

async function loadPartnerBondBookStates(partnershipUuid) {
  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_book_states")
    .select("*")
    .eq("partnership_id", partnershipUuid)
    .order("bond_volume_number", { ascending: true })
    .order("bond_book_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond book states: ${error.message}`);
  }

  return data || [];
}

async function loadPartnerBondVolumeStates(partnershipUuid) {
  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_volume_states")
    .select("*")
    .eq("partnership_id", partnershipUuid)
    .order("bond_volume_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond volume states: ${error.message}`);
  }

  return data || [];
}

async function updatePartnerBond(partnershipUuid, payload) {
  const attempts = ["partnership_uuid", "partnership_id"];
  let lastError = null;

  for (const column of attempts) {
    const { data, error } = await supabase
      .from("partner_bonds")
      .update(payload)
      .eq(column, partnershipUuid)
      .select("*")
      .maybeSingle();

    if (!error && data) {
      return data;
    }

    if (!error && !data) {
      continue;
    }

    if (isColumnMissingError(error)) {
      lastError = error;
      continue;
    }

    throw new Error(`Failed to update partner bond ledger: ${error.message}`);
  }

  if (lastError) {
    throw new Error(`Failed to update partner bond ledger fallback: ${lastError.message}`);
  }

  return null;
}

async function updateBondBookState(bookStateId, payload) {
  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_book_states")
    .update(payload)
    .eq("id", bookStateId)
    .select("*")
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to update bond book state: ${error.message}`);
  }

  return data || null;
}

async function updateBondVolumeState(volumeStateId, options = {}) {
  const nowIso = new Date().toISOString();

  const isCompleted = !!options.is_completed;
  const completedAt = options.completed_at ?? null;
  const activeStatus = isCompleted ? "completed" : "active";

  const payloads = [
    {
      is_completed: isCompleted,
      completed_at: completedAt,
      updated_at: nowIso
    },
    {
      volume_status: activeStatus,
      completed_at: completedAt,
      updated_at: nowIso
    },
    {
      status: activeStatus,
      completed_at: completedAt,
      updated_at: nowIso
    },
    {
      state: activeStatus,
      completed_at: completedAt,
      updated_at: nowIso
    },
    {
      is_completed: isCompleted,
      updated_at: nowIso
    },
    {
      updated_at: nowIso
    }
  ];

  let lastError = null;

  for (const payload of payloads) {
    const { data, error } = await supabase
      .schema("partner")
      .from("partner_bond_volume_states")
      .update(payload)
      .eq("id", volumeStateId)
      .select("*")
      .maybeSingle();

    if (!error && data) {
      return data;
    }

    if (error && isColumnMissingError(error)) {
      lastError = error;
      continue;
    }

    if (error) {
      lastError = error;
      break;
    }
  }

  if (lastError) {
    throw new Error(`Failed to update partner bond volume state: ${lastError.message}`);
  }

  return null;
}

async function tryAdvancePartnerBondBook(partnershipUuid, volumeNumber, bookNumber) {
  const { data, error } = await supabase.rpc("advance_partner_bond_book", {
    p_partnership_id: partnershipUuid,
    p_bond_volume_number: volumeNumber,
    p_bond_book_number: bookNumber
  });

  if (error) {
    throw new Error(`Failed to advance partner bond book: ${error.message}`);
  }

  return unwrapRpcPayload(data, "advance_partner_bond_book");
}

async function updatePartnerBondRuntimePartnerFlags({
  partnershipUuid,
  partnerAOffered,
  partnerBOffered,
  partnerAReady,
  partnerBReady,
  partnerAMeditating,
  partnerBMeditating
}) {
  const { data, error } = await supabase.rpc(
    "update_partner_bond_runtime_partner_flags",
    {
      p_partnership_uuid: partnershipUuid,
      p_partner_a_offered: partnerAOffered,
      p_partner_b_offered: partnerBOffered,
      p_partner_a_ready: partnerAReady,
      p_partner_b_ready: partnerBReady,
      p_partner_a_meditating: partnerAMeditating,
      p_partner_b_meditating: partnerBMeditating
    }
  );

  if (error) {
    throw new Error(`Failed to update partner bond runtime partner flags: ${error.message}`);
  }

  return data || null;
}

async function setPartnerBondRuntimeSessionState({
  partnershipUuid,
  sessionStatus,
  pauseReason = null,
  awaitingCompletion = false
}) {
  const { data, error } = await supabase.rpc(
    "set_partner_bond_runtime_session_state",
    {
      p_partnership_uuid: partnershipUuid,
      p_session_status: sessionStatus,
      p_pause_reason: pauseReason,
      p_awaiting_completion: awaitingCompletion
    }
  );

  if (error) {
    throw new Error(`Failed to update partner bond runtime session state: ${error.message}`);
  }

  return data || null;
}

async function patchPartnerBondRuntimeRow(partnershipUuid, payload) {
  const { data, error } = await supabase
    .from(RUNTIME_TABLE)
    .update(payload)
    .eq("partnership_uuid", partnershipUuid)
    .select("*")
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to patch partner bond runtime row: ${error.message}`);
  }

  return data || null;
}

async function setRuntimePointersAfterAdvance(partnershipUuid, nextBook, nowIso) {
  const nextVolumeNumber = nextBook ? safeNumber(nextBook?.bond_volume_number, 0) || null : null;
  const nextBookNumber = nextBook ? safeNumber(nextBook?.bond_book_number, 0) || null : null;
  const nextBookStateId = nextBook ? safeText(nextBook?.id) || null : null;

  const storedMinutes = nextBook
    ? Math.max(0, safeNumber(nextBook?.shared_minutes_accumulated, 0))
    : 0;

  const requiredMinutes = nextBook
    ? Math.max(0, safeNumber(nextBook?.required_shared_minutes, 0))
    : 0;

  const progressPercent = nextBook
    ? computeBookProgressPercent(nextBook)
    : 0;

  const payload = {
    current_volume_number: nextVolumeNumber,
    current_book_number: nextBookNumber,
    current_book_state_id: nextBookStateId,
    stored_minutes: storedMinutes,
    required_minutes: requiredMinutes,
    progress_percent: progressPercent,
    progress_started_at: null,
    last_progress_at: null,
    updated_at: nowIso
  };

  return await patchPartnerBondRuntimeRow(partnershipUuid, payload);
}

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
    const body = parseBody(event);
    const query = event.queryStringParameters || {};
    const requestSource = { ...query, ...body };

    const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
    const cookies = parseCookies(cookieHeader);
    const sessionCookieName = process.env.SESSION_COOKIE_NAME || "ap_session";
    const sessionToken = safeText(cookies[sessionCookieName]);

    const slAvatarKey = safeText(requestSource.sl_avatar_key);
    const slUsername = safeLower(requestSource.sl_username);

    const requestedPartnershipUuid = safeText(
      requestSource.partnership_uuid || requestSource.selected_partnership_uuid
    );

    const requestedLegacyPartnershipId =
      parsePositiveInteger(
        requestSource.partnership_id || requestSource.legacy_partnership_id
      ) || null;

    let sessionRow = null;
    let member = null;

    if (sessionToken) {
      const sessionResult = await loadMemberFromSessionToken(sessionToken);
      sessionRow = sessionResult.sessionRow;
      member = sessionResult.member;

      if (!sessionRow) {
        return buildResponse(401, {
          success: false,
          message: "Invalid or expired website session."
        });
      }
    } else {
      if (!slAvatarKey && !slUsername) {
        return buildResponse(400, {
          success: false,
          message: "sl_avatar_key or sl_username is required."
        });
      }

      member = await loadMember(slAvatarKey, slUsername);
    }

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "Cultivation member not found."
      });
    }

    await touchSessionAndPresence(sessionToken || null, member);

    const activePartnershipRows = await loadActivePartnershipRowsForMember(member);

    let explicitPartnershipRow = null;
    let selectedPartnershipUuid = null;
    let selectedPartnershipRow = null;

    if (requestedPartnershipUuid) {
      if (!isUuid(requestedPartnershipUuid)) {
        return buildResponse(400, {
          success: false,
          message: "partnership_uuid is invalid."
        });
      }

      explicitPartnershipRow = await loadActivePartnershipByUuidForMember(
        member,
        requestedPartnershipUuid
      );

      if (!explicitPartnershipRow) {
        return buildResponse(409, {
          success: false,
          message:
            "The requested partnership_uuid is not an active partnership for this member."
        });
      }
    } else if (requestedLegacyPartnershipId) {
      explicitPartnershipRow = await loadActivePartnershipByLegacyIdForMember(
        member,
        requestedLegacyPartnershipId
      );

      if (!explicitPartnershipRow) {
        return buildResponse(409, {
          success: false,
          message:
            "The requested partnership_id is not an active partnership for this member."
        });
      }
    } else {
      try {
        selectedPartnershipUuid = await loadSelectedPartnershipUuid(
          getMemberPrimaryId(member)
        );

        if (selectedPartnershipUuid) {
          selectedPartnershipRow = await loadActivePartnershipByUuidForMember(
            member,
            selectedPartnershipUuid
          );
        }
      } catch (selectedError) {
        console.error("complete-bond-book selected partnership error:", selectedError);
      }
    }

    const activePartnership = resolveFocusActivePartnership({
      explicitPartnershipRow,
      selectedPartnershipRow,
      activeRows: activePartnershipRows
    });

    if (!activePartnership) {
      return buildResponse(409, {
        success: false,
        message: "No active partnership was found for this member."
      });
    }

    const partnershipUuid = safeText(activePartnership.id);
    const legacyPartnershipId = parsePositiveInteger(activePartnership.partnership_id);

    if (!partnershipUuid) {
      return buildResponse(500, {
        success: false,
        message: "Active partnership is missing its UUID key."
      });
    }

    const memberPrimaryId = getMemberPrimaryId(member);
    if (memberPrimaryId) {
      await saveSelectedPartnership(memberPrimaryId, partnershipUuid);
      selectedPartnershipUuid = partnershipUuid;
    }

    let runtime = await loadBondRuntimeView(partnershipUuid);

    if (!runtime) {
      return buildResponse(409, {
        success: false,
        message: "No active bond runtime could be found for completion.",
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId
      });
    }

    const partnerRole = getPartnerRole(
      activePartnership,
      safeText(member.sl_avatar_key),
      safeLower(member.sl_username)
    );

    if (!partnerRole) {
      return buildResponse(409, {
        success: false,
        message: "This member does not belong to the active partnership."
      });
    }

    const sessionRole = buildSessionRolePayload(activePartnership, partnerRole, runtime);

    if (!sessionRole.is_session_leader) {
      return buildResponse(403, {
        success: false,
        message: "Only the session leader can finalize bond book completion.",
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId,
        session_role: sessionRole
      });
    }

    const currentVolumeNumber = safeNumber(
      runtime.bond_volume_number,
      safeNumber(runtime.current_volume_number, 0)
    );

    const currentBookNumber = safeNumber(
      runtime.bond_book_number,
      safeNumber(runtime.current_book_number, 0)
    );

    const currentBookStateId = safeText(
      runtime.partner_bond_book_state_id || runtime.current_book_state_id
    );

    const currentCatalogBookId = safeText(runtime.catalog_book_id);
    const currentVolumeStateId = safeText(
      runtime.partner_bond_volume_state_id || runtime.current_volume_state_id
    );

    if (!currentVolumeNumber || !currentBookNumber || !currentBookStateId || !currentCatalogBookId) {
      return buildResponse(409, {
        success: false,
        message: "The current bond runtime is missing its active book pointers.",
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId
      });
    }

    const [catalogBook, bookState] = await Promise.all([
      loadBondBookCatalog(currentCatalogBookId),
      loadBondBookStateById(currentBookStateId)
    ]);

    if (!catalogBook) {
      return buildResponse(500, {
        success: false,
        message: "The active bond book catalog entry could not be found."
      });
    }

    if (!bookState) {
      return buildResponse(500, {
        success: false,
        message: "The active partner bond book state could not be found."
      });
    }

    const requiredSharedMinutes = Math.max(
      1,
      safeNumber(
        runtime.required_minutes,
        safeNumber(
          bookState.required_shared_minutes,
          safeNumber(catalogBook.required_shared_minutes, 0)
        )
      )
    );

    const sharedMinutesAccumulated = Math.max(
      0,
      safeNumber(
        runtime.stored_minutes,
        safeNumber(bookState.shared_minutes_accumulated, 0)
      )
    );

    const rawBookStatus = pickBondBookStatus(bookState);
    const runtimePauseReason = getRuntimePauseReason(runtime);

    const bookReadyForCompletion =
      rawBookStatus === "completed" ||
      safeBoolean(runtime.awaiting_completion) ||
      (
        rawBookStatus === "paused" &&
        runtimePauseReason === PAUSE_REASON_AWAITING_COMPLETION
      ) ||
      sharedMinutesAccumulated >= requiredSharedMinutes;

    if (!bookReadyForCompletion) {
      return buildResponse(409, {
        success: false,
        message: `Bond Book ${currentBookNumber} is not ready for completion yet.`,
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId,
        session_role: sessionRole,
        runtime: {
          volume_number: currentVolumeNumber,
          book_number: currentBookNumber,
          session_status: safeText(runtime.session_status),
          pause_reason: safeText(runtimePauseReason)
        },
        book: {
          state_id: currentBookStateId,
          status: rawBookStatus,
          shared_minutes_accumulated: sharedMinutesAccumulated,
          required_shared_minutes: requiredSharedMinutes
        }
      });
    }

    const nowIso = new Date().toISOString();

    if (rawBookStatus !== "completed") {
      await updateBondBookState(currentBookStateId, {
        status: "completed",
        shared_minutes_accumulated: requiredSharedMinutes,
        completed_at: firstFilled(bookState.completed_at, nowIso),
        updated_at: nowIso
      });
    }

    const advanceResult = await tryAdvancePartnerBondBook(
      partnershipUuid,
      currentVolumeNumber,
      currentBookNumber
    );

    let [finalBondRow, finalBookStates, finalVolumeStates] = await Promise.all([
      loadPartnerBondRowByUuid(partnershipUuid),
      loadPartnerBondBookStates(partnershipUuid),
      loadPartnerBondVolumeStates(partnershipUuid)
    ]);

    const directNextBook = findNextBookAfter(
      finalBookStates,
      currentVolumeNumber,
      currentBookNumber
    );

    const actionableBook = findAnyActionableBook(finalBookStates);
    const nextBook = directNextBook || actionableBook || null;

    const nextBookStatus = nextBook ? pickBondBookStatus(nextBook) : null;
    const nextBookNumber = nextBook ? safeNumber(nextBook.bond_book_number, 0) : null;
    const nextVolumeNumber = nextBook ? safeNumber(nextBook.bond_volume_number, 0) : null;

    const currentVolumeState =
      (finalVolumeStates || []).find((row) => safeText(row.id) === currentVolumeStateId) ||
      (finalVolumeStates || []).find(
        (row) => safeNumber(row.bond_volume_number, 0) === currentVolumeNumber
      ) ||
      null;

    const volumeCompleted =
      safeBoolean(currentVolumeState?.is_completed) ||
      ["completed"].includes(
        safeLower(
          currentVolumeState?.status ||
            currentVolumeState?.volume_status ||
            currentVolumeState?.state
        )
      );

    if (volumeCompleted && currentVolumeState?.id) {
      await updateBondVolumeState(currentVolumeState.id, {
        is_completed: true,
        completed_at: firstFilled(currentVolumeState.completed_at, nowIso)
      });
    }

    await setRuntimePointersAfterAdvance(
      partnershipUuid,
      nextBook,
      nowIso
    );

    await updatePartnerBondRuntimePartnerFlags({
      partnershipUuid,
      partnerAOffered: safeBoolean(nextBook?.partner_a_cp_offered),
      partnerBOffered: safeBoolean(nextBook?.partner_b_cp_offered),
      partnerAReady: false,
      partnerBReady: false,
      partnerAMeditating: false,
      partnerBMeditating: false
    });

    await setPartnerBondRuntimeSessionState({
      partnershipUuid,
      sessionStatus: SESSION_STATUS_IDLE,
      pauseReason: null,
      awaitingCompletion: false
    });

    await updatePartnerBond(partnershipUuid, {
      status: SESSION_STATUS_IDLE,
      pause_reason: null,
      updated_at: nowIso
    });

    runtime = await loadBondRuntimeView(partnershipUuid);
    finalBondRow = await loadPartnerBondRowByUuid(partnershipUuid);

    return buildResponse(200, {
      success: true,
      message: volumeCompleted
        ? `Bond Book ${currentBookNumber} was completed and Volume ${currentVolumeNumber} is now complete.`
        : nextBook
          ? `Bond Book ${currentBookNumber} was completed and Book ${nextBookNumber} is now the next target.`
          : `Bond Book ${currentBookNumber} was completed successfully.`,
      action: volumeCompleted ? "volume_completed" : "book_completed",
      partnership_resolution: {
        requested_partnership_uuid: requestedPartnershipUuid || null,
        requested_partnership_id: requestedLegacyPartnershipId || null,
        selected_partnership_uuid: selectedPartnershipUuid || null,
        focus_partnership_uuid: partnershipUuid,
        focus_partnership_id: legacyPartnershipId,
        active_partnership_count: activePartnershipRows.length,
        has_multiple_active_partnerships: activePartnershipRows.length > 1
      },
      session_role: sessionRole,
      completed_book: {
        volume_number: currentVolumeNumber,
        book_number: currentBookNumber,
        book_state_id: currentBookStateId,
        status: "completed"
      },
      next_book: nextBook
        ? {
            volume_number: nextVolumeNumber,
            book_number: nextBookNumber,
            state_id: safeText(nextBook.id),
            catalog_book_id: safeText(nextBook.bond_book_id),
            status: nextBookStatus,
            ready_for_offering: nextBookStatus === "available",
            is_same_volume: nextVolumeNumber === currentVolumeNumber
          }
        : null,
      volume: {
        volume_number: currentVolumeNumber,
        is_completed: volumeCompleted
      },
      bond: {
        partnership_uuid: partnershipUuid,
        partnership_id: legacyPartnershipId,
        bond_percent: round4(finalBondRow?.bond_percent),
        total_shared_minutes: safeNumber(finalBondRow?.total_shared_minutes, 0),
        total_shared_qi_offered: round4(finalBondRow?.total_shared_qi_offered),
        completed_books_count: safeNumber(finalBondRow?.completed_books_count, 0),
        status: safeText(finalBondRow?.status, SESSION_STATUS_IDLE),
        pause_reason: safeText(finalBondRow?.pause_reason, "")
      },
      runtime,
      advance_result: advanceResult || null
    });
  } catch (error) {
    console.error("complete-bond-book error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to complete bond book.",
      error: error.message || "Unknown error."
    });
  }
};