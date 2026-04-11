const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const PARTNERSHIP_TABLE = "cultivation_partnerships";
const MEMBER_SELECTED_PARTNERSHIPS_TABLE = "member_selected_partnerships";
const CURRENCY_NAME = "Ascension Tokens";

const COOKIE_NAME = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();

function parseCookies(header) {
  const cookies = {};
  if (!header) return cookies;
  header.split(";").forEach(function(part) {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    try { cookies[key] = decodeURIComponent(val); } catch(e) { cookies[key] = val; }
  });
  return cookies;
}

async function resolveSession(event) {
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return null;

  const { data, error } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();

  if (error || !data) return null;
  return data;
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
  const text = String(value || "").trim();
  return text || fallback;
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function looksLikeUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function requireId(value, label) {
  const clean = safeText(value);
  const lowered = safeLower(clean);

  if (!clean || lowered === "undefined" || lowered === "null") {
    throw new Error(`Missing valid ${label}.`);
  }

  return clean;
}

function getPartnerRole(partnershipRow, slAvatarKey) {
  const requester = safeLower(partnershipRow?.requester_avatar_key);
  const recipient = safeLower(partnershipRow?.recipient_avatar_key);
  const current = safeLower(slAvatarKey);

  if (!current) return "";
  if (current === requester) return "partner_a";
  if (current === recipient) return "partner_b";
  return "";
}

function normalizePartnershipRow(row) {
  if (!row) return null;

  const resolvedUuid = requireId(row.id, "partnership_uuid");

  return {
    ...row,
    id: resolvedUuid
  };
}

async function loadMemberAccount(sl_avatar_key, normalizedUsername) {
  if (!safeText(sl_avatar_key) && !safeText(normalizedUsername)) {
    return null;
  }

  let query = supabase
    .from("cultivation_members")
    .select("id:member_id, sl_avatar_key, sl_username, display_name")
    .limit(1);

  if (safeText(sl_avatar_key)) {
    query = query.eq("sl_avatar_key", sl_avatar_key);
  } else {
    query = query.eq("sl_username", normalizedUsername);
  }

  const { data, error } = await query.maybeSingle();

  if (error) {
    throw new Error(`Failed to load cultivation member: ${error.message}`);
  }

  return data || null;
}

async function loadPartnershipByUuid(partnershipUuid) {
  const resolvedUuid = requireId(partnershipUuid, "partnership_uuid");

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      recipient_avatar_key,
      requester_username,
      recipient_username,
      status,
      created_at,
      updated_at
    `)
    .eq("id", resolvedUuid)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by UUID: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadPartnershipByLegacyId(legacyPartnershipId) {
  const resolvedLegacyId = requireId(
    legacyPartnershipId,
    "legacy partnership_id"
  );

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      recipient_avatar_key,
      requester_username,
      recipient_username,
      status,
      created_at,
      updated_at
    `)
    .eq("partnership_id", resolvedLegacyId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load partnership by legacy ID: ${error.message}`);
  }

  return normalizePartnershipRow(data);
}

async function loadSelectedPartnershipReference(memberId) {
  if (!safeText(memberId)) return "";

  const { data, error } = await supabase
    .schema("partner")
    .from(MEMBER_SELECTED_PARTNERSHIPS_TABLE)
    .select("*")
    .eq("member_id", memberId)
    .limit(2);

  if (error) {
    throw new Error(`Failed to load selected partnership: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(
      `Multiple selected partnership rows found for member ${memberId}.`
    );
  }

  const row = rows[0] || null;
  if (!row) return "";

  return (
    safeText(row.partnership_id) ||
    safeText(row.partnership_uuid) ||
    safeText(row.selected_partnership_id) ||
    safeText(row.selected_partnership_uuid) ||
    ""
  );
}

async function loadActivePartnershipRows(sl_avatar_key) {
  if (!safeText(sl_avatar_key)) return [];

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      recipient_avatar_key,
      requester_username,
      recipient_username,
      status,
      created_at,
      updated_at
    `)
    .or(`requester_avatar_key.eq.${sl_avatar_key},recipient_avatar_key.eq.${sl_avatar_key}`)
    .eq("status", "active")
    .order("created_at", { ascending: true });

  if (error) {
    throw new Error(`Failed to load active partnerships: ${error.message}`);
  }

  return (data || []).map(normalizePartnershipRow);
}

async function loadSingleActivePartnership(sl_avatar_key) {
  const rows = await loadActivePartnershipRows(sl_avatar_key);

  if (rows.length === 1) {
    return {
      row: rows[0],
      multiple: false,
      count: 1
    };
  }

  return {
    row: null,
    multiple: rows.length > 1,
    count: rows.length
  };
}

function buildResolvedBondContext({
  partnership,
  sl_avatar_key,
  source,
  hasAnyActivePartnerships,
  multipleActiveFound,
  selectedPartnershipRequired,
  selectionReferenceFound,
  selectionReferenceInvalid,
  selectionReferenceMissing,
  selectionReferenceInactive,
  explicitResolutionFailed,
  explicitResolutionReason
}) {
  const buyerRole = partnership ? getPartnerRole(partnership, sl_avatar_key) : "";

  return {
    has_active_partnership: Boolean(hasAnyActivePartnerships),
    has_multiple_active_partnerships: Boolean(multipleActiveFound),
    selected_partnership_required: Boolean(selectedPartnershipRequired),
    selected_partnership_found: Boolean(selectionReferenceFound),
    selected_partnership_invalid: Boolean(selectionReferenceInvalid),
    selected_partnership_missing: Boolean(selectionReferenceMissing),
    selected_partnership_inactive: Boolean(selectionReferenceInactive),
    explicit_resolution_failed: Boolean(explicitResolutionFailed),
    explicit_resolution_reason: explicitResolutionReason || null,

    partnership: partnership || null,
    partnership_uuid: partnership?.id || null,
    legacy_partnership_id: partnership?.partnership_id || null,
    buyer_role: buyerRole || null,
    partnership_source: source || null
  };
}

async function resolveBondContext({
  memberId,
  sl_avatar_key,
  requestedPartnershipUuid,
  requestedLegacyPartnershipId
}) {
  const activeRows = await loadActivePartnershipRows(sl_avatar_key);
  const hasAnyActivePartnerships = activeRows.length > 0;
  const multipleActiveFound = activeRows.length > 1;

  let selectionReferenceFound = false;
  let selectionReferenceInvalid = false;
  let selectionReferenceMissing = false;
  let selectionReferenceInactive = false;
  let explicitResolutionFailed = false;
  let explicitResolutionReason = null;

  if (safeText(requestedPartnershipUuid)) {
    if (!looksLikeUuid(requestedPartnershipUuid)) {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "invalid_partnership_uuid"
      });
    }

    const row = await loadPartnershipByUuid(requestedPartnershipUuid);

    if (!row) {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_uuid_not_found"
      });
    }

    if (safeLower(row.status) !== "active") {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_not_active"
      });
    }

    if (!getPartnerRole(row, sl_avatar_key)) {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "not_member_of_requested_partnership"
      });
    }

    return buildResolvedBondContext({
      partnership: row,
      sl_avatar_key,
      source: "explicit_partnership_uuid",
      hasAnyActivePartnerships,
      multipleActiveFound,
      selectedPartnershipRequired: false,
      selectionReferenceFound,
      selectionReferenceInvalid,
      selectionReferenceMissing,
      selectionReferenceInactive,
      explicitResolutionFailed,
      explicitResolutionReason
    });
  }

  if (safeText(requestedLegacyPartnershipId)) {
    const row = await loadPartnershipByLegacyId(requestedLegacyPartnershipId);

    if (!row) {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "legacy_partnership_id_not_found"
      });
    }

    if (safeLower(row.status) !== "active") {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "partnership_not_active"
      });
    }

    if (!getPartnerRole(row, sl_avatar_key)) {
      return buildResolvedBondContext({
        partnership: null,
        sl_avatar_key,
        source: null,
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: multipleActiveFound,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed: true,
        explicitResolutionReason: "not_member_of_requested_partnership"
      });
    }

    return buildResolvedBondContext({
      partnership: row,
      sl_avatar_key,
      source: "explicit_legacy_partnership_id",
      hasAnyActivePartnerships,
      multipleActiveFound,
      selectedPartnershipRequired: false,
      selectionReferenceFound,
      selectionReferenceInvalid,
      selectionReferenceMissing,
      selectionReferenceInactive,
      explicitResolutionFailed,
      explicitResolutionReason
    });
  }

  const selectedReference = await loadSelectedPartnershipReference(memberId);

  if (selectedReference) {
    selectionReferenceFound = true;

    let selectedRow = null;

    if (looksLikeUuid(selectedReference)) {
      selectedRow = await loadPartnershipByUuid(selectedReference);
    } else {
      selectedRow = await loadPartnershipByLegacyId(selectedReference);
    }

    if (!selectedRow) {
      selectionReferenceMissing = true;
    } else if (safeLower(selectedRow.status) !== "active") {
      selectionReferenceInactive = true;
    } else if (!getPartnerRole(selectedRow, sl_avatar_key)) {
      selectionReferenceInvalid = true;
    } else {
      return buildResolvedBondContext({
        partnership: selectedRow,
        sl_avatar_key,
        source: "selected_partnership",
        hasAnyActivePartnerships,
        multipleActiveFound,
        selectedPartnershipRequired: false,
        selectionReferenceFound,
        selectionReferenceInvalid,
        selectionReferenceMissing,
        selectionReferenceInactive,
        explicitResolutionFailed,
        explicitResolutionReason
      });
    }
  }

  const singleActive = await loadSingleActivePartnership(sl_avatar_key);

  if (singleActive.row) {
    return buildResolvedBondContext({
      partnership: singleActive.row,
      sl_avatar_key,
      source: "single_active_fallback",
      hasAnyActivePartnerships,
      multipleActiveFound,
      selectedPartnershipRequired: false,
      selectionReferenceFound,
      selectionReferenceInvalid,
      selectionReferenceMissing,
      selectionReferenceInactive,
      explicitResolutionFailed,
      explicitResolutionReason
    });
  }

  return buildResolvedBondContext({
    partnership: null,
    sl_avatar_key,
    source: null,
    hasAnyActivePartnerships,
    multipleActiveFound,
    selectedPartnershipRequired: multipleActiveFound,
    selectionReferenceFound,
    selectionReferenceInvalid,
    selectionReferenceMissing,
    selectionReferenceInactive,
    explicitResolutionFailed,
    explicitResolutionReason
  });
}

async function loadBondVolumeStates(partnershipId) {
  if (!safeText(partnershipId)) return [];

  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_volume_states")
    .select("*")
    .eq("partnership_id", partnershipId)
    .order("bond_volume_number", { ascending: true });

  if (error) {
    throw new Error(`Failed to load bond volume states: ${error.message}`);
  }

  return data || [];
}

async function loadMemberWallet(sl_avatar_key, normalizedUsername) {
  if (!sl_avatar_key && !normalizedUsername) {
    return null;
  }

  let walletQuery = supabase
    .from("member_wallets")
    .select(`
      sl_avatar_key,
      sl_username,
      ascension_tokens_balance,
      total_tokens_credited,
      total_tokens_spent,
      updated_at
    `)
    .limit(1);

  if (sl_avatar_key) {
    walletQuery = walletQuery.eq("sl_avatar_key", sl_avatar_key);
  } else {
    walletQuery = walletQuery.eq("sl_username", normalizedUsername);
  }

  const { data, error } = await walletQuery;

  if (error) {
    throw new Error(`Failed to load member wallet: ${error.message}`);
  }

  return Array.isArray(data) && data.length > 0 ? data[0] : null;
}

async function loadOwnedCultivationLibrary(sl_avatar_key, normalizedUsername) {
  if (!sl_avatar_key && !normalizedUsername) {
    return [];
  }

  let libraryQuery = supabase
    .from("member_library_view")
    .select(`
      id,
      sl_avatar_key,
      sl_username,
      store_item_id,
      item_key,
      realm_name,
      volume_number,
      item_name,
      volume_status,
      insight_current,
      insight_required,
      base_status,
      early_status,
      middle_status,
      late_status,
      current_section,
      owned_at,
      completed_at,
      updated_at
    `);

  if (sl_avatar_key) {
    libraryQuery = libraryQuery.eq("sl_avatar_key", sl_avatar_key);
  } else {
    libraryQuery = libraryQuery.eq("sl_username", normalizedUsername);
  }

  const { data, error } = await libraryQuery;

  if (error) {
    throw new Error(`Failed to load member library: ${error.message}`);
  }

  return data || [];
}

async function loadStoreAdmins(normalizedUsername) {
  if (!normalizedUsername) return false;

  const { data, error } = await supabase
    .schema("library")
    .from("library_store_admins")
    .select("sl_username, is_active")
    .eq("sl_username", normalizedUsername)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    return false;
  }

  return Boolean(data);
}

function buildCultivationItem(item, ownedEntry, walletBalance) {
  const priceAmount = safeNumber(item.price_amount, 0);
  const stock = safeNumber(item.stock, 0);
  const inStock = stock > 0;
  const owned = Boolean(ownedEntry);
  const canAfford = owned ? false : walletBalance >= priceAmount;
  const tokensNeeded = owned ? 0 : Math.max(0, priceAmount - walletBalance);

  return {
    id: item.id,
    item_key: item.item_key,
    category: safeText(item.category, "cultivation"),
    item_type: safeText(item.item_type, "cultivation_volume"),
    is_shared_purchase: Boolean(item.is_shared_purchase),

    realm_name: item.realm_name,
    volume_number: safeNumber(item.volume_number, 0),
    item_name: item.item_name,
    description: item.description,
    price_currency: item.price_currency || CURRENCY_NAME,
    price_amount: priceAmount,

    stock,
    in_stock: inStock,
    is_active: Boolean(item.is_active),

    owned,
    owned_volume_status: ownedEntry ? ownedEntry.volume_status : "unclaimed",
    insight_current: ownedEntry ? safeNumber(ownedEntry.insight_current, 0) : 0,
    insight_required: ownedEntry ? safeNumber(ownedEntry.insight_required, 100) : 100,
    current_section: ownedEntry ? ownedEntry.current_section : null,

    section_statuses: {
      base: ownedEntry ? ownedEntry.base_status : "sealed",
      early: ownedEntry ? ownedEntry.early_status : "sealed",
      middle: ownedEntry ? ownedEntry.middle_status : "sealed",
      late: ownedEntry ? ownedEntry.late_status : "sealed"
    },

    can_afford: canAfford,
    can_purchase: !owned && inStock && canAfford,
    purchase_block_reason: owned
      ? "already_owned"
      : !inStock
      ? "out_of_stock"
      : !canAfford
      ? "insufficient_tokens"
      : null,
    tokens_needed: tokensNeeded,

    owned_at: ownedEntry ? ownedEntry.owned_at : null,
    completed_at: ownedEntry ? ownedEntry.completed_at : null
  };
}

function buildBondItem({
  item,
  walletBalance,
  resolvedBondContext,
  bondStateMap
}) {
  const activePartnership = resolvedBondContext.partnership;
  const buyerRole = resolvedBondContext.buyer_role || "";
  const priceAmount = safeNumber(item.price_amount, 0);
  const stock = safeNumber(item.stock, 0);
  const inStock = stock > 0;
  const volumeNumber = safeNumber(item.volume_number, 0);

  const bondState = bondStateMap.get(volumeNumber) || null;
  const previousState = volumeNumber > 1 ? bondStateMap.get(volumeNumber - 1) || null : null;

  const hasActivePartnership = Boolean(resolvedBondContext.has_active_partnership);
  const hasResolvedPartnership = Boolean(activePartnership);
  const previousVolumeRequired = volumeNumber > 1;
  const previousVolumeCompleted = volumeNumber === 1 ? true : Boolean(previousState?.is_completed);

  const yourSharePaid =
    buyerRole === "partner_a"
      ? Boolean(bondState?.partner_a_paid)
      : buyerRole === "partner_b"
      ? Boolean(bondState?.partner_b_paid)
      : false;

  const partnerSharePaid =
    buyerRole === "partner_a"
      ? Boolean(bondState?.partner_b_paid)
      : buyerRole === "partner_b"
      ? Boolean(bondState?.partner_a_paid)
      : false;

  const isUnlocked = Boolean(bondState?.is_unlocked);
  const isCompleted = Boolean(bondState?.is_completed);

  const canAfford = !yourSharePaid && walletBalance >= priceAmount;

  let purchaseBlockReason = null;

  if (!hasActivePartnership) {
    purchaseBlockReason = "active_partnership_required";
  } else if (!hasResolvedPartnership) {
    purchaseBlockReason = "active_partnership_required";
  } else if (!buyerRole) {
    purchaseBlockReason = "invalid_partnership_member";
  } else if (!previousVolumeCompleted) {
    purchaseBlockReason = "previous_volume_incomplete";
  } else if (isCompleted) {
    purchaseBlockReason = "volume_completed";
  } else if (isUnlocked) {
    purchaseBlockReason = "volume_unlocked";
  } else if (yourSharePaid) {
    purchaseBlockReason = "your_share_paid";
  } else if (!inStock) {
    purchaseBlockReason = "out_of_stock";
  } else if (!canAfford) {
    purchaseBlockReason = "insufficient_tokens";
  }

  const canPurchase = purchaseBlockReason === null;

  let pairProgressState = "locked";

  if (!hasActivePartnership) {
    pairProgressState = "no_partnership";
  } else if (!hasResolvedPartnership) {
    pairProgressState = resolvedBondContext.selected_partnership_required
      ? "selection_required"
      : "unresolved_partnership";
  } else if (!previousVolumeCompleted) {
    pairProgressState = "previous_volume_required";
  } else if (isCompleted) {
    pairProgressState = "completed";
  } else if (isUnlocked) {
    pairProgressState = "unlocked";
  } else if (yourSharePaid && partnerSharePaid) {
    pairProgressState = "fully_paid";
  } else if (yourSharePaid && !partnerSharePaid) {
    pairProgressState = "waiting_for_partner";
  } else if (!yourSharePaid && partnerSharePaid) {
    pairProgressState = "waiting_for_you";
  } else {
    pairProgressState = "available";
  }

  return {
    id: item.id,
    item_key: item.item_key,
    category: safeText(item.category, "bond"),
    item_type: safeText(item.item_type, "bond_volume"),
    is_shared_purchase: Boolean(item.is_shared_purchase),

    realm_name: item.realm_name,
    volume_number: volumeNumber,
    item_name: item.item_name,
    description: item.description,
    price_currency: item.price_currency || CURRENCY_NAME,
    price_amount: priceAmount,

    stock,
    in_stock: inStock,
    is_active: Boolean(item.is_active),

    owned: isUnlocked,
    completed: isCompleted,
    can_afford: canAfford,
    can_purchase: canPurchase,
    tokens_needed: yourSharePaid ? 0 : Math.max(0, priceAmount - walletBalance),

    bond_volume_state: {
      partnership_id: activePartnership?.id || null,
      legacy_partnership_id: activePartnership?.partnership_id || null,
      partnership_source: resolvedBondContext.partnership_source || null,
      buyer_role: buyerRole || null,
      your_share_paid: yourSharePaid,
      partner_share_paid: partnerSharePaid,
      partner_a_paid: Boolean(bondState?.partner_a_paid),
      partner_b_paid: Boolean(bondState?.partner_b_paid),
      is_unlocked: isUnlocked,
      is_completed: isCompleted,
      pair_progress_state: pairProgressState,
      previous_volume_required: previousVolumeRequired,
      previous_volume_completed: previousVolumeCompleted,
      unlocked_at: bondState?.unlocked_at || null,
      completed_at: bondState?.completed_at || null
    },

    purchase_block_reason: purchaseBlockReason
  };
}

exports.handler = async (event) => {
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
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SECRET_KEY) {
      return buildResponse(500, {
        success: false,
        message: "Missing Supabase environment variables."
      });
    }

    const body = parseBody(event);
    const query = event.queryStringParameters || {};

    let sl_avatar_key = safeText(query.sl_avatar_key || body.sl_avatar_key);
    let sl_username = safeText(query.sl_username || body.sl_username);

    if (!sl_avatar_key && !sl_username) {
      const session = await resolveSession(event);
      if (session) {
        sl_avatar_key = safeText(session.sl_avatar_key);
        sl_username = safeText(session.sl_username);
      }
    }

    const normalizedUsername = safeLower(sl_username);

    const requestedPartnershipUuid = safeText(
      query.partnership_uuid ||
      body.partnership_uuid ||
      body.selected_partnership_uuid
    );

    const requestedLegacyPartnershipId = safeText(
      query.partnership_id ||
      query.legacy_partnership_id ||
      body.partnership_id ||
      body.selected_partnership_id ||
      body.legacy_partnership_id ||
      body.partnership_legacy_id ||
      body.legacyPartnershipId
    );

    const { data: storeItems, error: storeError } = await supabase
      .from("library_store_items")
      .select(`
        id,
        item_key,
        realm_name,
        volume_number,
        item_name,
        description,
        price_currency,
        price_amount,
        stock,
        is_active,
        category,
        item_type,
        is_shared_purchase,
        created_at,
        updated_at
      `)
      .eq("is_active", true)
      .order("category", { ascending: true })
      .order("volume_number", { ascending: true });

    if (storeError) {
      return buildResponse(500, {
        success: false,
        message: "Failed to load store items.",
        error: storeError.message
      });
    }

    const member = await loadMemberAccount(sl_avatar_key, normalizedUsername);
    const memberId = member?.id || null;
    const resolvedAvatarKey = safeText(member?.sl_avatar_key || sl_avatar_key);
    const resolvedUsername = safeLower(member?.sl_username || normalizedUsername);

    const isAdmin = await loadStoreAdmins(resolvedUsername);

    const wallet = await loadMemberWallet(resolvedAvatarKey, resolvedUsername);
    const walletFound = Boolean(wallet);
    const walletBalance = safeNumber(wallet?.ascension_tokens_balance, 0);

    const ownedCultivationRows = await loadOwnedCultivationLibrary(
      resolvedAvatarKey,
      resolvedUsername
    );

    const ownedCultivationMap = new Map(
      ownedCultivationRows.map((row) => [row.item_key, row])
    );

    const resolvedBondContext = await resolveBondContext({
      memberId,
      sl_avatar_key: resolvedAvatarKey,
      requestedPartnershipUuid,
      requestedLegacyPartnershipId
    });

    const bondStates = resolvedBondContext.partnership_uuid
      ? await loadBondVolumeStates(resolvedBondContext.partnership_uuid)
      : [];

    const bondStateMap = new Map(
      bondStates.map((row) => [safeNumber(row.bond_volume_number, 0), row])
    );

    const items = (storeItems || []).map((item) => {
      const category = safeLower(item.category || "cultivation");
      const itemType = safeLower(item.item_type || "cultivation_volume");

      if (category === "bond" || itemType === "bond_volume") {
        return buildBondItem({
          item,
          walletBalance,
          resolvedBondContext,
          bondStateMap
        });
      }

      const ownedEntry = ownedCultivationMap.get(item.item_key) || null;
      return buildCultivationItem(item, ownedEntry, walletBalance);
    });

    const cultivationItems = items.filter((item) => {
      const category = safeLower(item.category);
      const itemType = safeLower(item.item_type);
      return category === "cultivation" || itemType === "cultivation_volume";
    });

    const bondItems = items.filter((item) => {
      const category = safeLower(item.category);
      const itemType = safeLower(item.item_type);
      return category === "bond" || itemType === "bond_volume";
    });

    return buildResponse(200, {
      success: true,
      message: "Store items loaded successfully.",
      user_context: {
        member_id: memberId,
        sl_avatar_key: resolvedAvatarKey || null,
        sl_username: resolvedUsername || null,
        display_name: member?.display_name || null,

        is_store_admin: isAdmin,

        has_active_partnership: Boolean(resolvedBondContext.has_active_partnership),
        has_multiple_active_partnerships: Boolean(
          resolvedBondContext.has_multiple_active_partnerships
        ),
        selected_partnership_required: Boolean(
          resolvedBondContext.selected_partnership_required
        ),
        selected_partnership_found: Boolean(
          resolvedBondContext.selected_partnership_found
        ),
        selected_partnership_invalid: Boolean(
          resolvedBondContext.selected_partnership_invalid
        ),
        selected_partnership_missing: Boolean(
          resolvedBondContext.selected_partnership_missing
        ),
        selected_partnership_inactive: Boolean(
          resolvedBondContext.selected_partnership_inactive
        ),

        explicit_resolution_failed: Boolean(
          resolvedBondContext.explicit_resolution_failed
        ),
        explicit_resolution_reason:
          resolvedBondContext.explicit_resolution_reason || null,

        partnership_id: resolvedBondContext.partnership_uuid || null,
        partnership_uuid: resolvedBondContext.partnership_uuid || null,
        legacy_partnership_id: resolvedBondContext.legacy_partnership_id || null,
        buyer_role: resolvedBondContext.buyer_role || null,
        partnership_source: resolvedBondContext.partnership_source || null
      },
      wallet: {
        currency_name: CURRENCY_NAME,
        ascension_tokens_balance: walletBalance,
        wallet_found: walletFound
      },
      categories: {
        cultivation_count: cultivationItems.length,
        bond_count: bondItems.length
      },
      total_items: items.length,
      items,
      cultivation_items: cultivationItems,
      bond_items: bondItems
    });
  } catch (error) {
    return buildResponse(500, {
      success: false,
      message: "Unexpected error while loading store items.",
      error: error.message
    });
  }
};
