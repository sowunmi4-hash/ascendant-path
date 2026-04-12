const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const PARTNERSHIP_TABLE = "cultivation_partnerships";
const MEMBER_SELECTED_PARTNERSHIPS_TABLE = "member_selected_partnerships";

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

function safeText(value) {
  return String(value || "").trim();
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

function makeReferenceCode(prefix, slAvatarKey, itemKey) {
  const cleanAvatar =
    safeText(slAvatarKey).replace(/[^a-zA-Z0-9]/g, "").slice(0, 16) || "avatar";
  const cleanItem =
    safeText(itemKey).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 32) || "item";
  const unix = Date.now();
  return `${prefix}-${cleanAvatar}-${cleanItem}-${unix}`;
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

function requireId(value, label) {
  const clean = safeText(value);
  const lowered = safeLower(clean);

  if (!clean || lowered === "undefined" || lowered === "null") {
    throw new Error(`Missing valid ${label}.`);
  }

  return clean;
}

function looksLikeUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    safeText(value)
  );
}

function normalizePartnershipRow(row) {
  if (!row) return null;

  const resolvedId = requireId(
    row.id || row.partnership_uuid || row.partnership_id,
    "partnership_id"
  );

  return {
    ...row,
    id: resolvedId
  };
}

async function restoreStock(itemId, expectedCurrentStock, restoredStock) {
  try {
    await supabase
      .from("library_store_items")
      .update({ stock: restoredStock })
      .eq("id", itemId)
      .eq("stock", expectedCurrentStock);
  } catch (error) {
    console.error("Stock restore failed:", {
      itemId,
      expectedCurrentStock,
      restoredStock,
      message: error?.message
    });
  }
}

async function consumeOneStoreStock(itemId) {
  const { data: currentItem, error: loadError } = await supabase
    .from("library_store_items")
    .select("id, stock")
    .eq("id", itemId)
    .limit(1)
    .maybeSingle();

  if (loadError) {
    return {
      success: false,
      reason: "stock_lookup_failed",
      error: loadError.message
    };
  }

  const currentStock = safeNumber(currentItem?.stock, 0);

  if (!currentItem || currentStock <= 0) {
    return {
      success: false,
      reason: "out_of_stock",
      current_stock: currentStock
    };
  }

  const newStock = currentStock - 1;

  const { data: updateRows, error: updateError } = await supabase
    .from("library_store_items")
    .update({ stock: newStock })
    .eq("id", itemId)
    .eq("stock", currentStock)
    .select("id, stock");

  if (updateError || !updateRows || updateRows.length === 0) {
    return {
      success: false,
      reason: "stock_changed",
      current_stock: currentStock,
      error: updateError?.message || "Stock changed during update."
    };
  }

  return {
    success: true,
    previousStock: currentStock,
    newStock
  };
}

async function creditRefund({
  sl_avatar_key,
  sl_username,
  token_amount,
  related_item_key,
  notes,
  reference_code
}) {
  try {
    const { data, error } = await supabase.rpc("apply_ascension_token_entry", {
      p_sl_avatar_key: sl_avatar_key,
      p_sl_username: sl_username,
      p_entry_type: "refund",
      p_token_amount: token_amount,
      p_linden_amount: null,
      p_reference_code: reference_code,
      p_related_item_key: related_item_key,
      p_notes: notes
    });

    if (error) {
      console.error("Refund RPC failed:", {
        sl_avatar_key,
        sl_username,
        token_amount,
        related_item_key,
        reference_code,
        message: error.message
      });
      return { success: false, error: error.message };
    }

    const row = Array.isArray(data) && data.length > 0 ? data[0] : null;

    return {
      success: true,
      balance_before: row?.balance_before ?? null,
      balance_after: row?.balance_after ?? null,
      ledger_id: row?.ledger_id ?? null
    };
  } catch (error) {
    console.error("Refund error:", {
      sl_avatar_key,
      sl_username,
      token_amount,
      related_item_key,
      reference_code,
      message: error?.message
    });
    return { success: false, error: error.message };
  }
}

async function deductTokens({
  sl_avatar_key,
  sl_username,
  token_amount,
  related_item_key,
  notes,
  reference_code
}) {
  const { data, error } = await supabase.rpc("apply_ascension_token_entry", {
    p_sl_avatar_key: sl_avatar_key,
    p_sl_username: sl_username,
    p_entry_type: "store_spend",
    p_token_amount: token_amount,
    p_linden_amount: null,
    p_reference_code: reference_code,
    p_related_item_key: related_item_key,
    p_notes: notes
  });

  if (error) {
    return { success: false, error };
  }

  const row = Array.isArray(data) && data.length > 0 ? data[0] : null;

  return {
    success: true,
    row
  };
}

async function getWalletBalance(sl_avatar_key) {
  const { data } = await supabase
    .from("member_wallets")
    .select("ascension_tokens_balance")
    .eq("sl_avatar_key", sl_avatar_key)
    .maybeSingle();

  return safeNumber(data?.ascension_tokens_balance, 0);
}

async function loadPartnershipByUuid(partnershipUuid) {
  const resolvedPartnershipUuid = requireId(partnershipUuid, "partnership_uuid");

  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      recipient_avatar_key,
      status
    `)
    .eq("id", resolvedPartnershipUuid)
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
      status
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
  const resolvedMemberId = requireId(memberId, "member_id");

  const { data, error } = await supabase
    .schema("partner")
    .from(MEMBER_SELECTED_PARTNERSHIPS_TABLE)
    .select("*")
    .eq("member_id", resolvedMemberId)
    .limit(2);

  if (error) {
    throw new Error(`Failed to load selected partnership: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(
      `Multiple selected partnership rows found for member ${resolvedMemberId}.`
    );
  }

  const row = rows[0] || null;
  if (!row) return null;

  return (
    safeText(row.partnership_id) ||
    safeText(row.partnership_uuid) ||
    safeText(row.selected_partnership_id) ||
    safeText(row.selected_partnership_uuid) ||
    ""
  );
}

async function loadSingleActivePartnership(sl_avatar_key) {
  const { data, error } = await supabase
    .schema("partner")
    .from(PARTNERSHIP_TABLE)
    .select(`
      id,
      partnership_id,
      requester_avatar_key,
      recipient_avatar_key,
      status
    `)
    .or(
      `requester_avatar_key.eq.${sl_avatar_key},recipient_avatar_key.eq.${sl_avatar_key}`
    )
    .eq("status", "active")
    .limit(2);

  if (error) {
    throw new Error(`Failed to load active partnership: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    return {
      row: null,
      multiple: true
    };
  }

  return {
    row: normalizePartnershipRow(rows[0] || null),
    multiple: false
  };
}

function buildPartnershipContext(row, sl_avatar_key, source) {
  if (!row) return null;

  const buyerRole = getPartnerRole(row, sl_avatar_key);

  if (!buyerRole) {
    throw new Error("This avatar does not belong to the resolved partnership.");
  }

  return {
    source,
    buyerRole,
    partnership: {
      ...row,
      id: requireId(row.id || row.partnership_id, "partnership_id")
    }
  };
}

async function resolveBondPurchasePartnership({
  memberId,
  sl_avatar_key,
  requestedPartnershipUuid,
  requestedLegacyPartnershipId
}) {
  if (requestedPartnershipUuid) {
    if (!looksLikeUuid(requestedPartnershipUuid)) {
      return {
        success: false,
        statusCode: 400,
        message: "Invalid partnership_uuid format."
      };
    }

    const explicitRow = await loadPartnershipByUuid(requestedPartnershipUuid);

    if (!explicitRow) {
      return {
        success: false,
        statusCode: 404,
        message: "The requested partnership was not found."
      };
    }

    if (safeLower(explicitRow.status) !== "active") {
      return {
        success: false,
        statusCode: 409,
        message: "The requested partnership is not active."
      };
    }

    try {
      return {
        success: true,
        ...buildPartnershipContext(
          explicitRow,
          sl_avatar_key,
          "explicit_partnership_uuid"
        )
      };
    } catch (error) {
      return {
        success: false,
        statusCode: 403,
        message: error.message
      };
    }
  }

  if (requestedLegacyPartnershipId) {
    const explicitLegacyRow = await loadPartnershipByLegacyId(
      requestedLegacyPartnershipId
    );

    if (!explicitLegacyRow) {
      return {
        success: false,
        statusCode: 404,
        message: "The requested legacy partnership_id was not found."
      };
    }

    if (safeLower(explicitLegacyRow.status) !== "active") {
      return {
        success: false,
        statusCode: 409,
        message: "The requested partnership is not active."
      };
    }

    try {
      return {
        success: true,
        ...buildPartnershipContext(
          explicitLegacyRow,
          sl_avatar_key,
          "explicit_legacy_partnership_id"
        )
      };
    } catch (error) {
      return {
        success: false,
        statusCode: 403,
        message: error.message
      };
    }
  }

  const selectedPartnershipReference = await loadSelectedPartnershipReference(memberId);

  if (selectedPartnershipReference) {
    let selectedRow = null;

    if (looksLikeUuid(selectedPartnershipReference)) {
      selectedRow = await loadPartnershipByUuid(selectedPartnershipReference);
    } else {
      selectedRow = await loadPartnershipByLegacyId(selectedPartnershipReference);
    }

    if (!selectedRow) {
      return {
        success: false,
        statusCode: 409,
        message:
          "The saved selected partnership no longer exists. Re-select the partnership and try again."
      };
    }

    if (safeLower(selectedRow.status) !== "active") {
      return {
        success: false,
        statusCode: 409,
        message:
          "Your selected partnership is not active. Re-select an active partnership and try again."
      };
    }

    try {
      return {
        success: true,
        ...buildPartnershipContext(
          selectedRow,
          sl_avatar_key,
          "selected_partnership"
        )
      };
    } catch (error) {
      return {
        success: false,
        statusCode: 403,
        message: error.message
      };
    }
  }

  const singleActive = await loadSingleActivePartnership(sl_avatar_key);

  if (singleActive.multiple) {
    return {
      success: false,
      statusCode: 409,
      message:
        "Multiple active partnerships were found. Select a partnership first or send partnership_uuid with the purchase request."
    };
  }

  if (!singleActive.row) {
    return {
      success: false,
      statusCode: 409,
      message: "You need an active partnership before buying a Bond Volume."
    };
  }

  try {
    return {
      success: true,
      ...buildPartnershipContext(
        singleActive.row,
        sl_avatar_key,
        "single_active_fallback"
      )
    };
  } catch (error) {
    return {
      success: false,
      statusCode: 403,
      message: error.message
    };
  }
}

async function loadBondVolumeState(partnershipId, bondVolumeNumber) {
  const resolvedPartnershipId = requireId(partnershipId, "partnership_id");

  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_volume_states")
    .select("*")
    .eq("partnership_id", resolvedPartnershipId)
    .eq("bond_volume_number", bondVolumeNumber)
    .limit(2);

  if (error) {
    throw new Error(`Failed to load bond volume state: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(
      `Duplicate partner_bond_volume_states rows found for partnership ${resolvedPartnershipId} and bond volume ${bondVolumeNumber}.`
    );
  }

  return rows[0] || null;
}

async function loadPreviousBondVolumeCompletion(
  partnershipId,
  currentVolumeNumber
) {
  const resolvedPartnershipId = requireId(partnershipId, "partnership_id");

  if (safeNumber(currentVolumeNumber, 0) <= 1) {
    return { required: false, completed: true, row: null };
  }

  const previousVolumeNumber = safeNumber(currentVolumeNumber, 0) - 1;

  const { data, error } = await supabase
    .schema("partner")
    .from("partner_bond_volume_states")
    .select("*")
    .eq("partnership_id", resolvedPartnershipId)
    .eq("bond_volume_number", previousVolumeNumber)
    .limit(2);

  if (error) {
    throw new Error(
      `Failed to load previous bond volume state: ${error.message}`
    );
  }

  const rows = Array.isArray(data) ? data : [];

  if (rows.length > 1) {
    throw new Error(
      `Duplicate previous partner_bond_volume_states rows found for partnership ${resolvedPartnershipId} and bond volume ${previousVolumeNumber}.`
    );
  }

  const row = rows[0] || null;

  return {
    required: true,
    completed: Boolean(row?.is_completed),
    row
  };
}

async function logPurchase({ sl_avatar_key, sl_username, item, itemPrice }) {
  const { error } = await supabase.from("library_purchase_log").insert({
    sl_avatar_key,
    sl_username,
    store_item_id: item.id,
    item_key: item.item_key,
    item_name: item.item_name,
    price_currency: item.price_currency || "Ascension Tokens",
    price_amount: itemPrice
  });

  return error ? error.message : null;
}

async function purchaseCultivationVolume({
  member,
  resolvedUsername,
  item,
  itemPrice,
  sl_avatar_key
}) {
  const { data: existingOwnership, error: ownershipError } = await supabase
    .from("member_library")
    .select(`
      id,
      sl_avatar_key,
      sl_username,
      store_item_id,
      volume_status,
      insight_current,
      owned_at
    `)
    .eq("sl_avatar_key", sl_avatar_key)
    .eq("store_item_id", item.id)
    .maybeSingle();

  if (ownershipError) {
    return buildResponse(500, {
      success: false,
      message: "Failed to check existing library ownership.",
      error: ownershipError.message
    });
  }

  if (existingOwnership) {
    return buildResponse(409, {
      success: false,
      message: "You already own this volume.",
      item: {
        id: item.id,
        item_key: item.item_key,
        item_name: item.item_name
      },
      owned_volume: existingOwnership
    });
  }

  const purchaseReferenceCode = makeReferenceCode(
    "store-spend",
    sl_avatar_key,
    item.item_key
  );
  const refundReferenceCode = `${purchaseReferenceCode}-refund`;

  const spendResult = await deductTokens({
    sl_avatar_key,
    sl_username: resolvedUsername,
    token_amount: itemPrice,
    related_item_key: item.item_key,
    notes: `Store purchase for ${item.item_name}`,
    reference_code: purchaseReferenceCode
  });

  if (!spendResult.success) {
    const lowered = safeLower(spendResult.error?.message);

    if (lowered.includes("insufficient ascension tokens")) {
      const currentBalance = await getWalletBalance(sl_avatar_key);

      return buildResponse(409, {
        success: false,
        message: "You do not have enough Ascension Tokens to buy this volume.",
        currency_required: itemPrice,
        currency_balance: currentBalance,
        currency_shortfall: Math.max(0, itemPrice - currentBalance),
        item: {
          id: item.id,
          item_key: item.item_key,
          item_name: item.item_name,
          price_amount: item.price_amount,
          price_currency: item.price_currency
        }
      });
    }

    return buildResponse(500, {
      success: false,
      message: "Failed to deduct Ascension Tokens.",
      error: spendResult.error?.message || "Unknown token deduction error."
    });
  }

  const spendRow = spendResult.row || null;
  const balanceBefore = safeNumber(spendRow?.balance_before, 0);
  const balanceAfterSpend = safeNumber(spendRow?.balance_after, 0);

  const newStock = safeNumber(item.stock, 0) - 1;

  const { data: stockUpdateRows, error: stockUpdateError } = await supabase
    .from("library_store_items")
    .update({ stock: newStock })
    .eq("id", item.id)
    .eq("stock", item.stock)
    .select("id, stock");

  if (stockUpdateError || !stockUpdateRows || stockUpdateRows.length === 0) {
    await creditRefund({
      sl_avatar_key,
      sl_username: resolvedUsername,
      token_amount: itemPrice,
      related_item_key: item.item_key,
      notes: `Refund because stock update failed for ${item.item_name}`,
      reference_code: refundReferenceCode
    });

    return buildResponse(409, {
      success: false,
      message:
        "This volume could not be purchased because the stock changed. Your Ascension Tokens were refunded.",
      item: {
        id: item.id,
        item_key: item.item_key,
        item_name: item.item_name
      }
    });
  }

  const { data: libraryInsert, error: libraryInsertError } = await supabase
    .from("member_library")
    .insert({
      sl_avatar_key,
      sl_username: resolvedUsername,
      store_item_id: item.id,
      volume_status: "owned",
      insight_current: 0,
      insight_required: 100,
      base_status: "sealed",
      early_status: "sealed",
      middle_status: "sealed",
      late_status: "sealed",
      current_section: null
    })
    .select(`
      id,
      sl_avatar_key,
      sl_username,
      store_item_id,
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
      created_at,
      updated_at
    `)
    .maybeSingle();

  if (libraryInsertError) {
    await restoreStock(item.id, newStock, item.stock);

    await creditRefund({
      sl_avatar_key,
      sl_username: resolvedUsername,
      token_amount: itemPrice,
      related_item_key: item.item_key,
      notes: `Refund because library insert failed for ${item.item_name}`,
      reference_code: refundReferenceCode
    });

    return buildResponse(500, {
      success: false,
      message:
        "Purchase failed while adding the volume to the player library. Ascension Tokens were refunded.",
      error: libraryInsertError.message
    });
  }

  const purchaseLogError = await logPurchase({
    sl_avatar_key,
    sl_username: resolvedUsername,
    item,
    itemPrice
  });

  const warnings = [];
  if (purchaseLogError) {
    warnings.push("Volume purchased, but purchase log entry failed.");
  }

  return buildResponse(200, {
    success: true,
    message: "Volume purchased successfully.",
    warnings,
    purchase_type: "cultivation",
    currency_charged: true,
    currency_type: "ascension_tokens",
    user: {
      member_id: member.id || null,
      sl_avatar_key,
      sl_username: resolvedUsername || null,
      display_name: member.display_name || null
    },
    wallet: {
      currency: "Ascension Tokens",
      spent: itemPrice,
      balance_before: balanceBefore,
      balance_after: balanceAfterSpend
    },
    purchased_item: {
      id: item.id,
      item_key: item.item_key,
      category: item.category,
      item_type: item.item_type,
      realm_name: item.realm_name,
      volume_number: item.volume_number,
      item_name: item.item_name,
      description: item.description,
      price_currency: item.price_currency || "Ascension Tokens",
      price_amount: itemPrice,
      remaining_stock: newStock
    },
    library_entry: libraryInsert
  });
}

async function purchaseBondVolume({
  member,
  resolvedUsername,
  item,
  itemPrice,
  sl_avatar_key,
  requestedPartnershipUuid,
  requestedLegacyPartnershipId
}) {
  const partnershipResolution = await resolveBondPurchasePartnership({
    memberId: member.id,
    sl_avatar_key,
    requestedPartnershipUuid,
    requestedLegacyPartnershipId
  });

  if (!partnershipResolution.success) {
    return buildResponse(partnershipResolution.statusCode || 409, {
      success: false,
      message: partnershipResolution.message,
      item: {
        id: item.id,
        item_key: item.item_key,
        item_name: item.item_name
      }
    });
  }

  const activePartnership = partnershipResolution.partnership;
  const buyerRole = partnershipResolution.buyerRole;
  const partnershipSource = partnershipResolution.source;
  const partnershipId = requireId(
    activePartnership.id || activePartnership.partnership_id,
    "partnership_id"
  );
  const bondVolumeNumber = safeNumber(item.volume_number, 0);

  console.error("Bond purchase start:", {
    partnershipId,
    bondVolumeNumber,
    itemId: item.id,
    itemKey: item.item_key,
    buyerRole,
    partnershipSource,
    requestedPartnershipUuid,
    requestedLegacyPartnershipId,
    sl_avatar_key
  });

  const previousVolumeCheck = await loadPreviousBondVolumeCompletion(
    partnershipId,
    bondVolumeNumber
  );

  if (previousVolumeCheck.required && !previousVolumeCheck.completed) {
    return buildResponse(409, {
      success: false,
      message:
        "You must complete the previous Bond Volume before buying this one.",
      required_previous_volume: bondVolumeNumber - 1,
      current_volume: bondVolumeNumber,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  const existingBondState = await loadBondVolumeState(
    partnershipId,
    bondVolumeNumber
  );

  if (existingBondState?.is_completed) {
    return buildResponse(409, {
      success: false,
      message:
        "This Bond Volume has already been completed by your partnership.",
      bond_volume_state: existingBondState,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  if (buyerRole === "partner_a" && existingBondState?.partner_a_paid) {
    return buildResponse(409, {
      success: false,
      message: "You have already paid your share for this Bond Volume.",
      bond_volume_state: existingBondState,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  if (buyerRole === "partner_b" && existingBondState?.partner_b_paid) {
    return buildResponse(409, {
      success: false,
      message: "You have already paid your share for this Bond Volume.",
      bond_volume_state: existingBondState,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  if (existingBondState?.is_unlocked) {
    return buildResponse(409, {
      success: false,
      message: "This Bond Volume is already unlocked for your partnership.",
      bond_volume_state: existingBondState,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  const willUnlockNow =
    buyerRole === "partner_a"
      ? Boolean(existingBondState?.partner_b_paid) &&
        !Boolean(existingBondState?.partner_a_paid)
      : Boolean(existingBondState?.partner_a_paid) &&
        !Boolean(existingBondState?.partner_b_paid);

  const purchaseReferenceCode = makeReferenceCode(
    "bond-store-spend",
    sl_avatar_key,
    item.item_key
  );
  const refundReferenceCode = `${purchaseReferenceCode}-refund`;

  const spendResult = await deductTokens({
    sl_avatar_key,
    sl_username: resolvedUsername,
    token_amount: itemPrice,
    related_item_key: item.item_key,
    notes: `Bond Volume purchase contribution for ${item.item_name}`,
    reference_code: purchaseReferenceCode
  });

  if (!spendResult.success) {
    const lowered = safeLower(spendResult.error?.message);

    if (lowered.includes("insufficient ascension tokens")) {
      const currentBalance = await getWalletBalance(sl_avatar_key);

      return buildResponse(409, {
        success: false,
        message:
          "You do not have enough Ascension Tokens to pay your share for this Bond Volume.",
        currency_required: itemPrice,
        currency_balance: currentBalance,
        currency_shortfall: Math.max(0, itemPrice - currentBalance),
        item: {
          id: item.id,
          item_key: item.item_key,
          item_name: item.item_name,
          price_amount: item.price_amount,
          price_currency: item.price_currency
        },
        partnership: {
          id: partnershipId,
          source: partnershipSource
        }
      });
    }

    return buildResponse(500, {
      success: false,
      message: "Failed to deduct Ascension Tokens for this Bond Volume.",
      error: spendResult.error?.message || "Unknown token deduction error.",
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  const spendRow = spendResult.row || null;
  const balanceBefore = safeNumber(spendRow?.balance_before, 0);
  const balanceAfterSpend = safeNumber(spendRow?.balance_after, 0);

  let stockConsumed = false;
  let previousStock = null;
  let remainingStock = safeNumber(item.stock, 0);

  if (willUnlockNow) {
    const stockResult = await consumeOneStoreStock(item.id);

    if (!stockResult.success) {
      await creditRefund({
        sl_avatar_key,
        sl_username: resolvedUsername,
        token_amount: itemPrice,
        related_item_key: item.item_key,
        notes: `Refund because shared bond stock could not be consumed for ${item.item_name}`,
        reference_code: refundReferenceCode
      });

      return buildResponse(409, {
        success: false,
        message:
          stockResult.reason === "out_of_stock"
            ? "This Bond Volume is out of stock and could not be unlocked. Your Ascension Tokens were refunded."
            : "This Bond Volume could not be unlocked because the stock changed. Your Ascension Tokens were refunded.",
        item: {
          id: item.id,
          item_key: item.item_key,
          item_name: item.item_name
        },
        partnership: {
          id: partnershipId,
          source: partnershipSource
        }
      });
    }

    stockConsumed = true;
    previousStock = stockResult.previousStock;
    remainingStock = stockResult.newStock;
  }

  console.error("Bond purchase before RPC:", {
    partnershipId,
    bondVolumeNumber,
    itemId: item.id,
    willUnlockNow,
    stockConsumed,
    remainingStock,
    partnershipSource
  });

  const { data: purchaseRows, error: purchaseError } = await supabase.rpc(
    "apply_partner_bond_volume_purchase",
    {
      p_partnership_id: partnershipId,
      p_buyer_avatar_key: sl_avatar_key,
      p_store_item_id: item.id
    }
  );

  if (purchaseError) {
    console.error("Bond purchase RPC failed:", {
      partnershipId,
      bondVolumeNumber,
      itemId: item.id,
      itemKey: item.item_key,
      buyerRole,
      partnershipSource,
      willUnlockNow,
      stockConsumed,
      message: purchaseError.message,
      details: purchaseError.details,
      hint: purchaseError.hint,
      code: purchaseError.code
    });

    if (stockConsumed) {
      await restoreStock(item.id, remainingStock, previousStock);
    }

    await creditRefund({
      sl_avatar_key,
      sl_username: resolvedUsername,
      token_amount: itemPrice,
      related_item_key: item.item_key,
      notes: `Refund because bond purchase application failed for ${item.item_name}`,
      reference_code: refundReferenceCode
    });

    return buildResponse(500, {
      success: false,
      message:
        "Purchase failed while applying Bond Volume ownership. Ascension Tokens were refunded.",
      error: purchaseError.message,
      details: purchaseError.details || null,
      hint: purchaseError.hint || null,
      code: purchaseError.code || null,
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  const purchaseState =
    Array.isArray(purchaseRows) && purchaseRows.length > 0
      ? purchaseRows[0]
      : null;

  if (!purchaseState) {
    console.error("Bond purchase returned no rows:", {
      partnershipId,
      bondVolumeNumber,
      itemId: item.id,
      itemKey: item.item_key,
      buyerRole,
      partnershipSource
    });

    if (stockConsumed) {
      await restoreStock(item.id, remainingStock, previousStock);
    }

    await creditRefund({
      sl_avatar_key,
      sl_username: resolvedUsername,
      token_amount: itemPrice,
      related_item_key: item.item_key,
      notes: `Refund because bond purchase returned no result for ${item.item_name}`,
      reference_code: refundReferenceCode
    });

    return buildResponse(500, {
      success: false,
      message:
        "Purchase failed while applying Bond Volume ownership. Ascension Tokens were refunded.",
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  if (purchaseState.buyer_already_paid) {
    if (stockConsumed) {
      await restoreStock(item.id, remainingStock, previousStock);
    }

    await creditRefund({
      sl_avatar_key,
      sl_username: resolvedUsername,
      token_amount: itemPrice,
      related_item_key: item.item_key,
      notes: `Refund because buyer had already paid for ${item.item_name}`,
      reference_code: refundReferenceCode
    });

    return buildResponse(409, {
      success: false,
      message: "You have already paid your share for this Bond Volume.",
      partnership: {
        id: partnershipId,
        source: partnershipSource
      }
    });
  }

  const refreshedBondState = await loadBondVolumeState(
    partnershipId,
    bondVolumeNumber
  );

  const purchaseLogError = await logPurchase({
    sl_avatar_key,
    sl_username: resolvedUsername,
    item,
    itemPrice
  });

  const warnings = [];
  if (purchaseLogError) {
    warnings.push("Bond Volume purchased, but purchase log entry failed.");
  }

  return buildResponse(200, {
    success: true,
    message: purchaseState.is_unlocked
      ? "Bond Volume unlocked successfully for your partnership."
      : "Your share for the Bond Volume was paid successfully. Waiting for your partner.",
    warnings,
    purchase_type: "bond",
    currency_charged: true,
    currency_type: "ascension_tokens",
    user: {
      member_id: member.id || null,
      sl_avatar_key,
      sl_username: resolvedUsername || null,
      display_name: member.display_name || null
    },
    wallet: {
      currency: "Ascension Tokens",
      spent: itemPrice,
      balance_before: balanceBefore,
      balance_after: balanceAfterSpend
    },
    purchased_item: {
      id: item.id,
      item_key: item.item_key,
      category: item.category,
      item_type: item.item_type,
      realm_name: item.realm_name,
      volume_number: item.volume_number,
      item_name: item.item_name,
      description: item.description,
      price_currency: item.price_currency || "Ascension Tokens",
      price_amount: itemPrice,
      remaining_stock: remainingStock
    },
    partnership: {
      id: partnershipId,
      legacy_partnership_id: activePartnership.partnership_id || null,
      requester_avatar_key: activePartnership.requester_avatar_key || null,
      recipient_avatar_key: activePartnership.recipient_avatar_key || null,
      buyer_role: buyerRole,
      source: partnershipSource
    },
    bond_purchase_state: {
      buyer_side: purchaseState.buyer_side || buyerRole,
      buyer_already_paid: Boolean(purchaseState.buyer_already_paid),
      partner_a_paid: Boolean(purchaseState.partner_a_paid),
      partner_b_paid: Boolean(purchaseState.partner_b_paid),
      is_unlocked: Boolean(purchaseState.is_unlocked),
      initialized_books: safeNumber(purchaseState.initialized_books, 0),
      stock_consumed_on_unlock: stockConsumed
    },
    bond_volume_state: refreshedBondState
  });
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
    let inputUsername = safeLower(query.sl_username || body.sl_username);

    if (!sl_avatar_key && !inputUsername) {
      const session = await resolveSession(event);
      if (session) {
        sl_avatar_key = safeText(session.sl_avatar_key);
        inputUsername = safeLower(session.sl_username);
      }
    }
    const item_key = safeText(query.item_key || body.item_key);
    const store_item_id = body.store_item_id || query.store_item_id || null;

    const partnership_uuid = safeText(
      query.partnership_uuid ||
      body.partnership_uuid ||
      body.selected_partnership_uuid
    );

    const legacy_partnership_id = safeText(
      query.partnership_id ||
      query.legacy_partnership_id ||
      body.partnership_id ||
      body.selected_partnership_id ||
      body.legacy_partnership_id ||
      body.partnership_legacy_id ||
      body.legacyPartnershipId
    );

    if (!sl_avatar_key) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: sl_avatar_key."
      });
    }

    if (!item_key && !store_item_id) {
      return buildResponse(400, {
        success: false,
        message:
          "Missing required store item reference. Provide item_key or store_item_id."
      });
    }

    const { data: memberRows, error: memberError } = await supabase
      .from("cultivation_members")
      .select("id:member_id, sl_avatar_key, sl_username, display_name")
      .eq("sl_avatar_key", sl_avatar_key)
      .limit(1);

    if (memberError) {
      return buildResponse(500, {
        success: false,
        message: "Failed to load member account.",
        error: memberError.message
      });
    }

    const member =
      Array.isArray(memberRows) && memberRows.length > 0 ? memberRows[0] : null;

    if (!member) {
      return buildResponse(404, {
        success: false,
        message: "No cultivation member record found for this avatar."
      });
    }

    const resolvedUsername = safeLower(member.sl_username || inputUsername);

    let itemLookup = supabase
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
        is_shared_purchase
      `)
      .eq("is_active", true)
      .limit(1);

    if (item_key) {
      itemLookup = itemLookup.eq("item_key", item_key);
    } else {
      itemLookup = itemLookup.eq("id", store_item_id);
    }

    const { data: itemRows, error: itemError } = await itemLookup;

    if (itemError) {
      return buildResponse(500, {
        success: false,
        message: "Failed to load store item.",
        error: itemError.message
      });
    }

    const item = Array.isArray(itemRows) && itemRows.length > 0 ? itemRows[0] : null;

    if (!item) {
      return buildResponse(404, {
        success: false,
        message: "Store item not found or inactive."
      });
    }

    const itemPrice = safeNumber(item.price_amount, 0);

    if (itemPrice <= 0) {
      return buildResponse(409, {
        success: false,
        message:
          "This store item does not have a valid Ascension Token price configured.",
        item: {
          id: item.id,
          item_key: item.item_key,
          item_name: item.item_name,
          price_amount: item.price_amount,
          price_currency: item.price_currency,
          category: item.category,
          item_type: item.item_type
        }
      });
    }

    if (safeNumber(item.stock, 0) <= 0) {
      return buildResponse(409, {
        success: false,
        message: "This item is currently out of stock.",
        item: {
          id: item.id,
          item_key: item.item_key,
          item_name: item.item_name,
          stock: item.stock,
          category: item.category,
          item_type: item.item_type
        }
      });
    }

    const itemCategory = safeLower(item.category || "cultivation");
    const itemType = safeLower(item.item_type || "cultivation_volume");

    if (itemCategory === "bond" || itemType === "bond_volume") {
      return await purchaseBondVolume({
        member,
        resolvedUsername,
        item,
        itemPrice,
        sl_avatar_key,
        requestedPartnershipUuid: partnership_uuid,
        requestedLegacyPartnershipId: legacy_partnership_id
      });
    }

    return await purchaseCultivationVolume({
      member,
      resolvedUsername,
      item,
      itemPrice,
      sl_avatar_key
    });
  } catch (error) {
    console.error("Unexpected error while purchasing store item:", {
      message: error?.message,
      stack: error?.stack
    });

    return buildResponse(500, {
      success: false,
      message: "Unexpected error while purchasing store item.",
      error: error?.message || "Unknown error"
    });
  }
};