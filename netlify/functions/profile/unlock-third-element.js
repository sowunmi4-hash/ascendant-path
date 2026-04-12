const { createClient } = require("@supabase/supabase-js");
const crypto = require("crypto");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const REALM_INDEX_MAP = {
  mortal: 1,
  "mortal realm": 1,

  "qi gathering": 2,
  "qi gathering realm": 2,

  foundation: 3,
  "foundation realm": 3,

  "core formation": 4,
  "core formation realm": 4,

  "nascent soul": 5,
  "nascent soul realm": 5,

  "soul transformation": 6,
  "soul transformation realm": 6,

  "void refinement": 7,
  "void refinement realm": 7,

  "body integration": 8,
  "body integration realm": 8,

  mahayana: 9,
  "mahayana realm": 9,

  tribulation: 10,
  "tribulation realm": 10
};

const THIRD_UNLOCK_REALM_INDEX = 5; // Nascent Soul

const PATH_ELEMENT_BY_BASE_PAIR = {
  "metal|wood": "Sound",
  "metal|water": "Ice",
  "metal|fire": "Lightning",
  "metal|earth": "Gravity",

  "wood|water": "Wind",
  "wood|fire": "Light",
  "wood|earth": "Poison",

  "water|fire": "Thunder",
  "water|earth": "Darkness",

  "fire|earth": "Shadow"
};

const THIRD_POOL_MAP = {
  "metal|sound": ["Will", "Mind"],
  "metal|ice": ["Pure Yin", "Moon"],
  "metal|lightning": ["Yang", "Calamity"],
  "metal|gravity": ["Space", "Fortune"],

  "wood|sound": ["Soul", "Destiny"],
  "wood|wind": ["Time", "Destiny"],
  "wood|light": ["Creation", "Life"],
  "wood|poison": ["Demon", "Death"],

  "water|ice": ["Pure Yin", "Moon"],
  "water|wind": ["Time", "Void"],
  "water|thunder": ["Dragon", "Karma"],
  "water|darkness": ["Pure Yin", "Void"],

  "fire|lightning": ["Yang", "Sun"],
  "fire|light": ["Sun", "Creation"],
  "fire|thunder": ["Dragon", "Phoenix"],
  "fire|shadow": ["Chaos", "Demon"],

  "earth|gravity": ["Space", "Fortune"],
  "earth|poison": ["Death", "Demon"],
  "earth|darkness": ["Pure Yin", "Death"],
  "earth|shadow": ["Demon", "Chaos"]
};

const QI_PROFILE_BY_PRIMARY = {
  metal: "Ironborn",
  wood: "Verdant",
  water: "Tidal",
  fire: "Ember",
  earth: "Stoneborn"
};

const FOUNDATION_PROFILE_BY_PATH = {
  "metal|sound": "Resonant Steel",
  "metal|ice": "Glacial Steel",
  "metal|lightning": "Storm Steel",
  "metal|gravity": "Titan Steel",

  "wood|sound": "Verdant Echo",
  "wood|wind": "Verdant Gale",
  "wood|light": "Verdant Dawn",
  "wood|poison": "Verdant Venom",

  "water|ice": "Tidal Frost",
  "water|wind": "Tidal Gale",
  "water|thunder": "Tidal Storm",
  "water|darkness": "Tidal Veil",

  "fire|lightning": "Ember Volt",
  "fire|light": "Ember Dawn",
  "fire|thunder": "Ember Storm",
  "fire|shadow": "Ember Shade",

  "earth|gravity": "Stone Weight",
  "earth|poison": "Stone Venom",
  "earth|darkness": "Stone Veil",
  "earth|shadow": "Stone Shade"
};

const FUSED_PROFILE_BY_PATH = {
  "metal|sound": "Steelsong",
  "metal|ice": "Froststeel",
  "metal|lightning": "Stormsteel",
  "metal|gravity": "Titansteel",

  "wood|sound": "Echobloom",
  "wood|wind": "Galebloom",
  "wood|light": "Dawnbloom",
  "wood|poison": "Venombloom",

  "water|ice": "Frosttide",
  "water|wind": "Galetide",
  "water|thunder": "Stormtide",
  "water|darkness": "Veiltide",

  "fire|lightning": "Embervolt",
  "fire|light": "Emberdawn",
  "fire|thunder": "Stormember",
  "fire|shadow": "Embershade",

  "earth|gravity": "Stoneweight",
  "earth|poison": "Venomstone",
  "earth|darkness": "Veilstone",
  "earth|shadow": "Shadestone"
};

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
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
  const text = String(value || "").trim();
  return text || fallback;
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeBoolean(value) {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1") return true;
  if (value === 0 || value === "0") return false;

  const lowered = safeLower(value);
  if (lowered === "true") return true;
  if (lowered === "false") return false;

  return false;
}

function normalizeElement(value) {
  return safeLower(value).replace(/\s+/g, " ");
}

function normalizePairKey(a, b) {
  const first = normalizeElement(a);
  const second = normalizeElement(b);
  if (!first || !second) return "";
  return [first, second].sort().join("|");
}

function fallbackTitleCase(value) {
  return safeText(value)
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getRealmIndex(member) {
  const directIndex = Number(member?.realm_index);
  if (Number.isInteger(directIndex) && directIndex >= 1) {
    return directIndex;
  }

  const candidates = [
    member?.realm_key,
    member?.realm_name,
    member?.realm_display_name
  ];

  for (const candidate of candidates) {
    const normalized = safeLower(candidate);
    if (normalized && Object.prototype.hasOwnProperty.call(REALM_INDEX_MAP, normalized)) {
      return REALM_INDEX_MAP[normalized];
    }
  }

  return 1;
}

function derivePathElement(primaryElement, secondaryElement) {
  const pairKey = normalizePairKey(primaryElement, secondaryElement);
  return PATH_ELEMENT_BY_BASE_PAIR[pairKey] || "";
}

function buildThirdPoolKey(primaryElement, pathElement) {
  const primary = normalizeElement(primaryElement);
  const path = normalizeElement(pathElement);
  if (!primary || !path) return "";
  return `${primary}|${path}`;
}

function pickRandomElement(pool) {
  if (!Array.isArray(pool) || pool.length === 0) return "";
  return pool[crypto.randomInt(pool.length)];
}

function getEffectivePrimaryElement(traitsRow) {
  return safeText(
    traitsRow?.current_primary_element || traitsRow?.natural_primary_element
  );
}

function getEffectiveSecondaryElement(traitsRow) {
  return safeText(
    traitsRow?.current_secondary_element || traitsRow?.natural_secondary_element
  );
}

function getEffectivePathElement(traitsRow) {
  const explicitPath = safeText(
    traitsRow?.current_path_element || traitsRow?.natural_path_element
  );

  if (explicitPath) return explicitPath;

  const primaryElement = getEffectivePrimaryElement(traitsRow);
  const secondaryElement = getEffectiveSecondaryElement(traitsRow);

  if (!primaryElement || !secondaryElement) return "";
  return derivePathElement(primaryElement, secondaryElement);
}

function getEffectiveThirdElement(traitsRow) {
  return safeText(
    traitsRow?.current_third_element || traitsRow?.natural_third_element
  );
}

function isThirdUnlocked(traitsRow) {
  return (
    safeBoolean(
      traitsRow?.third_element_unlocked ||
        traitsRow?.is_third_unlocked ||
        traitsRow?.third_unlocked
    ) && Boolean(getEffectiveThirdElement(traitsRow))
  );
}

function buildSpiritRootProfileTitle(memberRow, traitsRowLike) {
  const realmIndex = getRealmIndex(memberRow);
  const primaryElement = safeLower(getEffectivePrimaryElement(traitsRowLike));
  const pathElement = safeLower(getEffectivePathElement(traitsRowLike));
  const thirdUnlocked = isThirdUnlocked(traitsRowLike);

  if (realmIndex <= 1) {
    return "Rootborn";
  }

  if (realmIndex >= 5 && thirdUnlocked && primaryElement && pathElement) {
    const profileKey = `${primaryElement}|${pathElement}`;
    return (
      FUSED_PROFILE_BY_PATH[profileKey] ||
      fallbackTitleCase(`${primaryElement} ${pathElement}`)
    );
  }

  if (realmIndex >= 3 && primaryElement && pathElement) {
    const profileKey = `${primaryElement}|${pathElement}`;
    return (
      FOUNDATION_PROFILE_BY_PATH[profileKey] ||
      `${fallbackTitleCase(primaryElement)} ${fallbackTitleCase(pathElement)}`
    );
  }

  if (realmIndex >= 2 && primaryElement) {
    return QI_PROFILE_BY_PRIMARY[primaryElement] || fallbackTitleCase(primaryElement);
  }

  return "Rootborn";
}

function setIfFieldExists(payload, row, fieldName, value) {
  if (row && Object.prototype.hasOwnProperty.call(row, fieldName)) {
    payload[fieldName] = value;
  }
}

function setAnyExistingFields(payload, row, fieldNames, value) {
  for (const fieldName of fieldNames) {
    setIfFieldExists(payload, row, fieldName, value);
  }
}

function applySpiritProfileFields(updatePayload, traitsRow, profileTitle) {
  setAnyExistingFields(
    updatePayload,
    traitsRow,
    [
      "natural_spirit_root_profile",
      "natural_root_profile",
      "natural_root_profile_text",
      "root_profile_text"
    ],
    profileTitle
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    [
      "current_spirit_root_profile",
      "current_root_profile",
      "current_root_profile_text"
    ],
    profileTitle
  );
}

function buildTraitsUpdatePayload(
  traitsRow,
  rolledThird,
  primaryElement,
  secondaryElement,
  pathElement,
  thirdPoolKey
) {
  const now = new Date().toISOString();
  const payload = {};

  if (pathElement) {
    setAnyExistingFields(payload, traitsRow, ["natural_path_element"], pathElement);
    setAnyExistingFields(payload, traitsRow, ["current_path_element"], pathElement);
  }

  if (primaryElement) {
    setAnyExistingFields(payload, traitsRow, ["natural_primary_element"], primaryElement);
    setAnyExistingFields(payload, traitsRow, ["current_primary_element"], primaryElement);
  }

  if (secondaryElement) {
    setAnyExistingFields(payload, traitsRow, ["natural_secondary_element"], secondaryElement);
    setAnyExistingFields(payload, traitsRow, ["current_secondary_element"], secondaryElement);
  }

  setAnyExistingFields(
    payload,
    traitsRow,
    ["natural_third_element", "third_element", "element_third"],
    rolledThird
  );

  setAnyExistingFields(payload, traitsRow, ["current_third_element"], rolledThird);

  setAnyExistingFields(
    payload,
    traitsRow,
    ["third_element_unlocked", "is_third_unlocked", "third_unlocked"],
    true
  );

  setAnyExistingFields(
    payload,
    traitsRow,
    ["third_element_unlocked_at", "third_unlocked_at"],
    now
  );

  setAnyExistingFields(
    payload,
    traitsRow,
    ["third_unlock_path", "third_path_key"],
    thirdPoolKey
  );

  setIfFieldExists(payload, traitsRow, "updated_at", now);

  return payload;
}

function snapshotForRollback(row, updatePayload) {
  const snapshot = {};
  for (const key of Object.keys(updatePayload)) {
    snapshot[key] = row[key];
  }
  return snapshot;
}

function buildMemberThirdUnlockRollbackSnapshot(memberRow) {
  const snapshot = {};

  const fieldNames = [
    "primary_element_key",
    "primary_element",
    "element_primary",

    "secondary_element_key",
    "secondary_element",
    "element_secondary",

    "path_element_key",
    "path_element",
    "element_path",

    "third_element_key",
    "third_element",
    "element_third",
    "tertiary_element_key",
    "tertiary_element",

    "dominant_root_element",
    "supporting_root_element",

    "third_element_unlocked",
    "is_third_unlocked",
    "third_unlocked",
    "third_element_unlocked_at",
    "third_unlocked_at",
    "updated_at"
  ];

  for (const fieldName of fieldNames) {
    if (Object.prototype.hasOwnProperty.call(memberRow, fieldName)) {
      snapshot[fieldName] = memberRow[fieldName];
    }
  }

  return snapshot;
}

async function refreshAndSyncMemberState(memberId) {
  const { error: refreshError } = await supabase.rpc("refresh_member_root_balance", {
    p_member_id: memberId
  });

  if (refreshError) {
    throw new Error(`Failed to refresh member root balance: ${refreshError.message}`);
  }

  const { error: syncError } = await supabase.rpc(
    "sync_member_root_balance_to_cultivation_members",
    {
      p_member_id: memberId
    }
  );

  if (syncError) {
    throw new Error(
      `Failed to sync member root balance to cultivation records: ${syncError.message}`
    );
  }
}

async function restoreMemberThirdSnapshot(memberId, snapshot) {
  if (!snapshot || !Object.keys(snapshot).length) return;

  const { error: updateError } = await supabase
    .from("cultivation_members")
    .update(snapshot)
    .eq("member_id", memberId);

  if (updateError) {
    throw new Error(`Failed to restore cultivation member snapshot: ${updateError.message}`);
  }

  await refreshAndSyncMemberState(memberId);
}

async function applyThirdUnlockToMember(memberId, rolledThird) {
  const { data, error } = await supabase.rpc("apply_approved_element_change", {
    p_member_id: memberId,
    p_slot_key: "third",
    p_new_element: safeText(rolledThird)
  });

  if (error) {
    throw new Error(`Failed to apply third element unlock: ${error.message}`);
  }

  return data || null;
}

async function requireMemberSession(event) {
  const cookieHeader = event.headers.cookie || event.headers.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  const sessionToken = cookies.ap_session;

  if (!sessionToken) {
    return {
      ok: false,
      response: json(401, {
        success: false,
        error: "not_logged_in"
      })
    };
  }

  const { data: sessionRow, error: sessionError } = await supabase
    .from("website_sessions")
    .select("*")
    .eq("session_token", sessionToken)
    .eq("is_active", true)
    .maybeSingle();

  if (sessionError || !sessionRow) {
    return {
      ok: false,
      response: json(401, {
        success: false,
        error: "invalid_session"
      })
    };
  }

  const { data: memberRow, error: memberError } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("sl_avatar_key", sessionRow.sl_avatar_key)
    .maybeSingle();

  if (memberError || !memberRow) {
    return {
      ok: false,
      response: json(403, {
        success: false,
        error: "member_not_found"
      })
    };
  }

  return {
    ok: true,
    session: sessionRow,
    member: memberRow
  };
}

async function loadMemberAndTraits(memberId) {
  const { data: memberRow, error: memberError } = await supabase
    .from("cultivation_members")
    .select("*")
    .eq("member_id", memberId)
    .maybeSingle();

  if (memberError || !memberRow) {
    throw new Error("Member not found.");
  }

  const { data: traitsRow, error: traitsError } = await supabase
    .schema("elements")
    .from("member_spiritual_traits")
    .select("*")
    .eq("member_id", memberId)
    .maybeSingle();

  if (traitsError || !traitsRow) {
    throw new Error("Member spiritual traits not found.");
  }

  return {
    memberRow,
    traitsRow
  };
}

function extractRootBalancePayload(balanceRow) {
  if (!balanceRow) return null;

  const pathWoodDelta =
    balanceRow.path_wood_delta ?? balanceRow.secondary_wood_delta ?? 0;
  const pathFireDelta =
    balanceRow.path_fire_delta ?? balanceRow.secondary_fire_delta ?? 0;
  const pathEarthDelta =
    balanceRow.path_earth_delta ?? balanceRow.secondary_earth_delta ?? 0;
  const pathMetalDelta =
    balanceRow.path_metal_delta ?? balanceRow.secondary_metal_delta ?? 0;
  const pathWaterDelta =
    balanceRow.path_water_delta ?? balanceRow.secondary_water_delta ?? 0;

  return {
    member_id: balanceRow.member_id,
    base_wood: balanceRow.base_wood,
    base_fire: balanceRow.base_fire,
    base_earth: balanceRow.base_earth,
    base_metal: balanceRow.base_metal,
    base_water: balanceRow.base_water,

    primary_wood_delta: balanceRow.primary_wood_delta,
    primary_fire_delta: balanceRow.primary_fire_delta,
    primary_earth_delta: balanceRow.primary_earth_delta,
    primary_metal_delta: balanceRow.primary_metal_delta,
    primary_water_delta: balanceRow.primary_water_delta,

    path_wood_delta: pathWoodDelta,
    path_fire_delta: pathFireDelta,
    path_earth_delta: pathEarthDelta,
    path_metal_delta: pathMetalDelta,
    path_water_delta: pathWaterDelta,

    secondary_wood_delta: pathWoodDelta,
    secondary_fire_delta: pathFireDelta,
    secondary_earth_delta: pathEarthDelta,
    secondary_metal_delta: pathMetalDelta,
    secondary_water_delta: pathWaterDelta,

    third_wood_delta: balanceRow.third_wood_delta,
    third_fire_delta: balanceRow.third_fire_delta,
    third_earth_delta: balanceRow.third_earth_delta,
    third_metal_delta: balanceRow.third_metal_delta,
    third_water_delta: balanceRow.third_water_delta,

    final_wood: balanceRow.final_wood,
    final_fire: balanceRow.final_fire,
    final_earth: balanceRow.final_earth,
    final_metal: balanceRow.final_metal,
    final_water: balanceRow.final_water,
    last_rebalanced_at: balanceRow.last_rebalanced_at || null
  };
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const auth = await requireMemberSession(event);
    if (!auth.ok) return auth.response;

    const memberRow = auth.member;
    const memberId = memberRow.member_id;
    const realmIndex = getRealmIndex(memberRow);

    if (realmIndex < THIRD_UNLOCK_REALM_INDEX) {
      return json(409, {
        success: false,
        error: "third_not_eligible",
        message: "Third element does not become eligible until Nascent Soul Realm.",
        current_realm: safeText(memberRow.realm_display_name || memberRow.realm_name),
        current_realm_index: realmIndex,
        required_realm: "Nascent Soul Realm",
        required_realm_index: THIRD_UNLOCK_REALM_INDEX
      });
    }

    const initialRows = await loadMemberAndTraits(memberId);
    const traitsRow = initialRows.traitsRow;

    const primaryElement = getEffectivePrimaryElement(traitsRow);
    const pathElement = getEffectivePathElement(traitsRow);
    const secondaryElement = getEffectiveSecondaryElement(traitsRow);
    const currentThirdElement = getEffectiveThirdElement(traitsRow);
    const thirdUnlocked = isThirdUnlocked(traitsRow);

    if (!primaryElement) {
      return json(409, {
        success: false,
        error: "primary_not_available",
        message: "Primary element must be available before third can be rolled."
      });
    }

    if (!pathElement) {
      return json(409, {
        success: false,
        error: "path_not_available",
        message: "Path element must be resolved before third can be rolled.",
        primary_element: primaryElement
      });
    }

    if (thirdUnlocked || currentThirdElement) {
      return json(409, {
        success: false,
        error: "third_already_unlocked",
        message: "Third element is already unlocked.",
        third_element: currentThirdElement
      });
    }

    const thirdPoolKey = buildThirdPoolKey(primaryElement, pathElement);
    const thirdPool = THIRD_POOL_MAP[thirdPoolKey];

    if (!thirdPool || thirdPool.length === 0) {
      return json(400, {
        success: false,
        error: "third_pool_not_found",
        message: "No third-element pool exists for the current primary and path combination.",
        primary_element: primaryElement,
        path_element: pathElement,
        third_pool_key: thirdPoolKey
      });
    }

    const rolledThird = pickRandomElement(thirdPool);

    if (!rolledThird) {
      return json(500, {
        success: false,
        error: "failed_to_roll_third",
        message: "Failed to roll a third element."
      });
    }

    const updatePayload = buildTraitsUpdatePayload(
      traitsRow,
      rolledThird,
      primaryElement,
      secondaryElement,
      pathElement,
      thirdPoolKey
    );

    const projectedTraits = {
      ...traitsRow,
      ...updatePayload
    };

    const spiritProfileTitle = buildSpiritRootProfileTitle(memberRow, projectedTraits);
    applySpiritProfileFields(updatePayload, traitsRow, spiritProfileTitle);

    if (!Object.keys(updatePayload).length) {
      return json(500, {
        success: false,
        error: "no_trait_fields_to_update",
        message: "No matching third-element fields were found in member_spiritual_traits."
      });
    }

    const previousTraitValues = snapshotForRollback(traitsRow, updatePayload);
    const previousMemberSnapshot = buildMemberThirdUnlockRollbackSnapshot(memberRow);

    let refreshedRootBalance = null;

    try {
      refreshedRootBalance = await applyThirdUnlockToMember(memberId, rolledThird);
    } catch (error) {
      try {
        await restoreMemberThirdSnapshot(memberId, previousMemberSnapshot);
      } catch (rollbackError) {
        console.error("unlock-third-element member rollback error:", rollbackError);
      }

      return json(500, {
        success: false,
        error: "failed_to_apply_member_third_unlock",
        message: error.message
      });
    }

    const { data: updatedTraits, error: updateError } = await supabase
      .schema("elements")
      .from("member_spiritual_traits")
      .update(updatePayload)
      .eq("member_id", memberId)
      .select("*")
      .maybeSingle();

    if (updateError) {
      try {
        await restoreMemberThirdSnapshot(memberId, previousMemberSnapshot);
      } catch (rollbackError) {
        console.error("unlock-third-element member rollback error:", rollbackError);
      }

      await supabase
        .schema("elements")
        .from("member_spiritual_traits")
        .update(previousTraitValues)
        .eq("member_id", memberId);

      return json(500, {
        success: false,
        error: "failed_to_save_third_unlock",
        message: updateError.message
      });
    }

    const finalRows = await loadMemberAndTraits(memberId);

    return json(200, {
      success: true,
      message: "Third element unlocked successfully.",
      member_id: memberId,
      sl_username: safeText(memberRow.sl_username),

      primary_element: primaryElement,
      secondary_element: secondaryElement,
      path_element: pathElement,
      third_element: rolledThird,

      third_pool: thirdPool,
      third_pool_key: thirdPoolKey,
      third_status: "unlocked",
      spirit_root_profile: spiritProfileTitle,

      member_root_balance: extractRootBalancePayload(refreshedRootBalance),

      resulting_elements: {
        primary_element: safeLower(
          finalRows.memberRow.primary_element || finalRows.traitsRow.current_primary_element
        ),
        secondary_element: safeLower(
          finalRows.memberRow.secondary_element || finalRows.traitsRow.current_secondary_element
        ),
        path_element: safeLower(
          finalRows.memberRow.path_element || finalRows.traitsRow.current_path_element
        ),
        third_element: safeLower(
          finalRows.memberRow.third_element || finalRows.traitsRow.current_third_element
        )
      },

      traits: updatedTraits
    });
  } catch (error) {
    console.error("unlock-third-element error:", error);

    return json(500, {
      success: false,
      error: "server_error",
      message: error.message
    });
  }
};