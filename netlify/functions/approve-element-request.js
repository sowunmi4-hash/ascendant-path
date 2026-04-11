const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const BASE_ELEMENT_OPTIONS = ["Metal", "Wood", "Water", "Fire", "Earth"];
const MAX_SINGLE_ROOT = 60;

const PRIMARY_UNLOCK_REALM_INDEX = 2;
const SECONDARY_UNLOCK_REALM_INDEX = 3;
const THIRD_UNLOCK_REALM_INDEX = 5;

const REALM_INDEX_MAP = {
  "mortal": 1,
  "mortal realm": 1,

  "qi gathering": 2,
  "qi gathering realm": 2,

  "foundation": 3,
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

  "mahayana": 9,
  "mahayana realm": 9,

  "tribulation": 10,
  "tribulation realm": 10
};

const PATH_ELEMENT_MAP = {
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

const ROOT_FIELD_CANDIDATES = {
  metal: ["current_metal_root", "natural_metal_root", "metal_root"],
  wood: ["current_wood_root", "natural_wood_root", "wood_root"],
  water: ["current_water_root", "natural_water_root", "water_root"],
  fire: ["current_fire_root", "natural_fire_root", "fire_root"],
  earth: ["current_earth_root", "natural_earth_root", "earth_root"]
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

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
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

function parsePositiveInteger(value) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) return null;
  if (!Number.isInteger(parsed)) return null;
  if (parsed <= 0) return null;

  return parsed;
}

function canonicalBaseElement(value) {
  const lowered = safeLower(value);
  return (
    BASE_ELEMENT_OPTIONS.find((element) => safeLower(element) === lowered) || ""
  );
}

function normalizePairKey(a, b) {
  return [safeLower(a), safeLower(b)].sort().join("|");
}

function derivePathElement(primaryBase, secondaryBase) {
  const key = normalizePairKey(primaryBase, secondaryBase);
  return PATH_ELEMENT_MAP[key] || "";
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

function getRootValueFromTraits(traitsRow, rootName) {
  const key = safeLower(rootName);
  const candidates = ROOT_FIELD_CANDIDATES[key] || [];

  for (const field of candidates) {
    const value = Number(traitsRow?.[field]);
    if (Number.isFinite(value)) {
      return value;
    }
  }

  return null;
}

function getCurrentRootMap(traitsRow) {
  const map = {};
  let hasMissingRoot = false;

  for (const rootName of BASE_ELEMENT_OPTIONS) {
    const value = getRootValueFromTraits(traitsRow, rootName);

    if (value === null) {
      hasMissingRoot = true;
      map[safeLower(rootName)] = null;
    } else {
      map[safeLower(rootName)] = value;
    }
  }

  return {
    map,
    hasMissingRoot
  };
}

function sumRootMap(rootMap) {
  return Object.values(rootMap).reduce((sum, value) => {
    const numeric = Number(value);
    return sum + (Number.isFinite(numeric) ? numeric : 0);
  }, 0);
}

function getExpectedMortalEnergy(traitsRow, memberRow) {
  const snapshot = Number(traitsRow?.mortal_energy_snapshot);
  if (Number.isFinite(snapshot) && snapshot >= 0) {
    return snapshot;
  }

  const memberEnergy = Number(memberRow?.mortal_energy);
  if (Number.isFinite(memberEnergy) && memberEnergy >= 0) {
    return memberEnergy;
  }

  return null;
}

function rankRoots(rootMap) {
  return BASE_ELEMENT_OPTIONS.map((rootName, index) => ({
    root: rootName,
    value: Number(rootMap[safeLower(rootName)] || 0),
    order: index
  })).sort((a, b) => {
    if (b.value !== a.value) return b.value - a.value;
    return a.order - b.order;
  });
}

function shouldFillCurrentElement(value) {
  const lowered = safeLower(value);
  return (
    !lowered ||
    lowered === "undetermined" ||
    lowered === "unknown" ||
    lowered === "sealed" ||
    lowered === "not awakened" ||
    lowered === "unawakened"
  );
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

function snapshotForRollback(row, updatePayload) {
  const snapshot = {};
  for (const key of Object.keys(updatePayload)) {
    snapshot[key] = row[key];
  }
  return snapshot;
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
  const explicit = safeText(
    traitsRow?.current_path_element || traitsRow?.natural_path_element
  );

  if (explicit) return explicit;

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

function fallbackTitleCase(value) {
  return safeText(value)
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function buildSpiritRootProfileTitle(memberRow, traitsRowLike) {
  const realmIndex = getRealmIndex(memberRow);
  const primaryElement = safeLower(getEffectivePrimaryElement(traitsRowLike));
  const pathElement = safeLower(getEffectivePathElement(traitsRowLike));
  const thirdUnlocked = isThirdUnlocked(traitsRowLike);

  if (realmIndex < PRIMARY_UNLOCK_REALM_INDEX) {
    return "Rootborn";
  }

  if (
    realmIndex >= THIRD_UNLOCK_REALM_INDEX &&
    thirdUnlocked &&
    primaryElement &&
    pathElement
  ) {
    const pathKey = `${primaryElement}|${pathElement}`;
    return (
      FUSED_PROFILE_BY_PATH[pathKey] ||
      fallbackTitleCase(`${primaryElement} ${pathElement}`)
    );
  }

  if (
    realmIndex >= SECONDARY_UNLOCK_REALM_INDEX &&
    primaryElement &&
    pathElement
  ) {
    const pathKey = `${primaryElement}|${pathElement}`;
    return (
      FOUNDATION_PROFILE_BY_PATH[pathKey] ||
      `${fallbackTitleCase(primaryElement)} ${fallbackTitleCase(pathElement)}`
    );
  }

  if (realmIndex >= PRIMARY_UNLOCK_REALM_INDEX && primaryElement) {
    return QI_PROFILE_BY_PRIMARY[primaryElement] || fallbackTitleCase(primaryElement);
  }

  return "Rootborn";
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

function buildProfileOnlyTraitUpdate(traitsRow, memberRow) {
  const updatePayload = {};
  const profileTitle = buildSpiritRootProfileTitle(memberRow, traitsRow);
  const now = new Date().toISOString();

  applySpiritProfileFields(updatePayload, traitsRow, profileTitle);
  setIfFieldExists(updatePayload, traitsRow, "updated_at", now);

  return {
    updatePayload,
    profileTitle
  };
}

function buildTraitUpdateForRootApproval(traitsRow, memberRow, requestRow) {
  const increaseRoot = canonicalBaseElement(requestRow.increase_root);
  const decreaseRoot = canonicalBaseElement(requestRow.decrease_root);
  const shiftAmount = parsePositiveInteger(requestRow.shift_amount);

  if (!increaseRoot || !decreaseRoot || !shiftAmount) {
    throw new Error("Root request is missing increase_root, decrease_root, or shift_amount.");
  }

  if (safeLower(increaseRoot) === safeLower(decreaseRoot)) {
    throw new Error("Increase root and decrease root cannot be the same.");
  }

  const { map: currentRoots, hasMissingRoot } = getCurrentRootMap(traitsRow);
  if (hasMissingRoot) {
    throw new Error("Current root data is incomplete.");
  }

  const increaseKey = safeLower(increaseRoot);
  const decreaseKey = safeLower(decreaseRoot);

  const currentIncreaseValue = Number(currentRoots[increaseKey] || 0);
  const currentDecreaseValue = Number(currentRoots[decreaseKey] || 0);

  if (currentDecreaseValue - shiftAmount < 0) {
    throw new Error(`${decreaseRoot} root cannot go below 0.`);
  }

  const proposedRoots = {
    ...currentRoots,
    [increaseKey]: currentIncreaseValue + shiftAmount,
    [decreaseKey]: currentDecreaseValue - shiftAmount
  };

  if (proposedRoots[increaseKey] > MAX_SINGLE_ROOT) {
    throw new Error(`${increaseRoot} root cannot go above ${MAX_SINGLE_ROOT}.`);
  }

  const expectedMortalEnergy = getExpectedMortalEnergy(traitsRow, memberRow);
  if (!Number.isFinite(expectedMortalEnergy)) {
    throw new Error("Mortal Energy snapshot is missing.");
  }

  const proposedTotal = sumRootMap(proposedRoots);
  if (proposedTotal !== expectedMortalEnergy) {
    throw new Error(
      `Proposed root total mismatch. Expected ${expectedMortalEnergy}, got ${proposedTotal}.`
    );
  }

  const ranked = rankRoots(proposedRoots);
  const dominantRoot = ranked[0]?.root || "Metal";
  const secondaryRoot = ranked[1]?.root || "Wood";
  const tertiaryRoot = ranked[2]?.root || "Water";

  const naturalPrimaryElement = dominantRoot;
  const naturalSecondaryElement = secondaryRoot;
  const naturalPathElement = derivePathElement(dominantRoot, secondaryRoot);

  const updatePayload = {};
  const now = new Date().toISOString();

  for (const rootName of BASE_ELEMENT_OPTIONS) {
    const lowered = safeLower(rootName);
    const value = Number(proposedRoots[lowered] || 0);

    setAnyExistingFields(
      updatePayload,
      traitsRow,
      [`natural_${lowered}_root`, `${lowered}_root`],
      value
    );

    setAnyExistingFields(
      updatePayload,
      traitsRow,
      [`current_${lowered}_root`],
      value
    );
  }

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["dominant_natural_root", "natural_dominant_root"],
    dominantRoot
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["secondary_natural_root", "natural_secondary_root"],
    secondaryRoot
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["tertiary_natural_root", "natural_tertiary_root"],
    tertiaryRoot
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["natural_primary_element"],
    naturalPrimaryElement
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["natural_secondary_element"],
    naturalSecondaryElement
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["natural_path_element"],
    naturalPathElement
  );

  if (shouldFillCurrentElement(traitsRow.current_primary_element)) {
    setAnyExistingFields(
      updatePayload,
      traitsRow,
      ["current_primary_element"],
      naturalPrimaryElement
    );
  }

  if (shouldFillCurrentElement(traitsRow.current_secondary_element)) {
    setAnyExistingFields(
      updatePayload,
      traitsRow,
      ["current_secondary_element"],
      naturalSecondaryElement
    );
  }

  if (shouldFillCurrentElement(traitsRow.current_path_element)) {
    setAnyExistingFields(
      updatePayload,
      traitsRow,
      ["current_path_element"],
      naturalPathElement
    );
  }

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["dominant_current_root_element"],
    dominantRoot
  );

  setAnyExistingFields(
    updatePayload,
    traitsRow,
    ["supporting_current_root_element"],
    secondaryRoot
  );

  setIfFieldExists(updatePayload, traitsRow, "updated_at", now);

  return {
    updatePayload,
    computed: {
      increase_root: increaseRoot,
      decrease_root: decreaseRoot,
      shift_amount: shiftAmount,
      natural_primary_element: naturalPrimaryElement,
      natural_secondary_element: naturalSecondaryElement,
      natural_path_element: naturalPathElement,
      dominant_natural_root: dominantRoot,
      secondary_natural_root: secondaryRoot,
      tertiary_natural_root: tertiaryRoot,
      current_roots: proposedRoots
    }
  };
}

function normalizeElementValueForMember(value) {
  const clean = safeText(value, "");
  return clean ? clean.toLowerCase() : null;
}

function buildMemberElementRollbackSnapshot(memberRow, requestTarget) {
  const snapshot = {};
  const target = safeLower(requestTarget);

  function capture(fields) {
    for (const fieldName of fields) {
      if (Object.prototype.hasOwnProperty.call(memberRow, fieldName)) {
        snapshot[fieldName] = memberRow[fieldName];
      }
    }
  }

  if (["primary", "secondary", "third"].includes(target)) {
    capture([
      "primary_element_key",
      "primary_element",
      "element_primary",

      "secondary_element_key",
      "secondary_element",
      "element_secondary",

      "path_element_key",
      "path_element",
      "element_path",

      "dominant_root_element",
      "supporting_root_element"
    ]);
  }

  if (target === "third") {
    capture([
      "third_element_key",
      "third_element",
      "element_third",
      "tertiary_element_key",
      "tertiary_element",

      "third_element_unlocked",
      "is_third_unlocked",
      "third_unlocked",
      "third_element_unlocked_at",
      "third_unlocked_at"
    ]);
  }

  if (Object.prototype.hasOwnProperty.call(memberRow, "updated_at")) {
    snapshot.updated_at = memberRow.updated_at;
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

async function restoreMemberElementSnapshot(memberId, snapshot) {
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

async function loadActiveThirdPathResolution(requestedThirdElement) {
  const cleanThird = safeLower(requestedThirdElement);

  if (!cleanThird) {
    throw new Error("Requested third element is required.");
  }

  const { data, error } = await supabase
    .schema("elements")
    .from("third_path_resolutions")
    .select("*")
    .eq("third_element_key", cleanThird)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load third-path resolution: ${error.message}`);
  }

  if (!data) {
    throw new Error(
      `No active third-path resolution was found for "${requestedThirdElement}".`
    );
  }

  return data;
}

function getApprovedElementValueForRpc(requestTarget, requestedElement) {
  const target = safeLower(requestTarget);

  if (target === "primary" || target === "secondary") {
    const canonical = canonicalBaseElement(requestedElement);
    if (!canonical) {
      throw new Error(
        `${target}_element must be one of: ${BASE_ELEMENT_OPTIONS.join(", ")}.`
      );
    }
    return canonical;
  }

  return safeText(requestedElement);
}

async function applyApprovedElementChange(memberId, requestTarget, requestedElement) {
  const approvedValue = getApprovedElementValueForRpc(requestTarget, requestedElement);

  const { data, error } = await supabase.rpc("apply_approved_element_change", {
    p_member_id: memberId,
    p_slot_key: safeLower(requestTarget),
    p_new_element: approvedValue
  });

  if (error) {
    throw new Error(`Failed to apply approved element change: ${error.message}`);
  }

  return data || null;
}

async function requireSafareehillsAdmin(event) {
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
        error: "admin_not_found"
      })
    };
  }

  const username = safeLower(memberRow.sl_username);

  const { data: adminRow, error: adminError } = await supabase
    .schema("library")
    .from("library_store_admins")
    .select("*")
    .eq("is_active", true)
    .eq("sl_username", username)
    .maybeSingle();

  if (adminError || !adminRow || username !== "safareehills") {
    return {
      ok: false,
      response: json(403, {
        success: false,
        error: "admin_access_denied"
      })
    };
  }

  return {
    ok: true,
    admin: memberRow
  };
}

async function findPendingRequestByBody(body) {
  const requestId = Number(body.request_id || body.element_request_id);

  if (!Number.isFinite(requestId) || requestId <= 0) {
    return {
      ok: false,
      response: json(400, {
        success: false,
        error: "missing_request_id",
        message: "request_id is required."
      })
    };
  }

  const { data: requestRow, error: requestError } = await supabase
    .schema("elements")
    .from("element_requests")
    .select("*")
    .eq("request_id", requestId)
    .eq("status", "pending")
    .maybeSingle();

  if (requestError) {
    return {
      ok: false,
      response: json(500, {
        success: false,
        error: "failed_to_load_request",
        message: requestError.message
      })
    };
  }

  if (!requestRow) {
    return {
      ok: false,
      response: json(404, {
        success: false,
        error: "pending_request_not_found"
      })
    };
  }

  return {
    ok: true,
    request: requestRow
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
    const auth = await requireSafareehillsAdmin(event);
    if (!auth.ok) return auth.response;

    const body = parseBody(event);
    const reviewNotes = safeText(body.review_notes);
    const reviewedAt = new Date().toISOString();

    const requestLookup = await findPendingRequestByBody(body);
    if (!requestLookup.ok) return requestLookup.response;

    const requestRow = requestLookup.request;
    const requestTarget = safeLower(requestRow.request_target);
    const isElementRequest = ["primary", "secondary", "third"].includes(requestTarget);
    const isRootRequest = ["root_minor", "root_major"].includes(requestTarget);

    const initialRows = await loadMemberAndTraits(requestRow.member_id);
    const memberRow = initialRows.memberRow;
    const traitsRow = initialRows.traitsRow;

    let traitUpdatePayload = {};
    let computedRootResult = null;
    let refreshedRootBalance = null;
    let previousMemberElementSnapshot = null;
    let previousTraitValues = {};
    let thirdResolution = null;
    let resultingMemberRow = memberRow;
    let resultingTraitsRow = traitsRow;
    let spiritProfileTitle = buildSpiritRootProfileTitle(memberRow, traitsRow);

    if (isElementRequest) {
      if (requestTarget === "third") {
        thirdResolution = await loadActiveThirdPathResolution(
          requestRow.requested_element
        );
      }

      previousMemberElementSnapshot = buildMemberElementRollbackSnapshot(
        memberRow,
        requestTarget
      );

      try {
        refreshedRootBalance = await applyApprovedElementChange(
          requestRow.member_id,
          requestTarget,
          requestRow.requested_element
        );
      } catch (error) {
        return json(500, {
          success: false,
          error: "failed_to_apply_member_element_change",
          message: error.message
        });
      }

      const reloaded = await loadMemberAndTraits(requestRow.member_id);
      resultingMemberRow = reloaded.memberRow;
      resultingTraitsRow = reloaded.traitsRow;

      const profileOnly = buildProfileOnlyTraitUpdate(
        resultingTraitsRow,
        resultingMemberRow
      );

      traitUpdatePayload = profileOnly.updatePayload;
      spiritProfileTitle = profileOnly.profileTitle;
      previousTraitValues = snapshotForRollback(resultingTraitsRow, traitUpdatePayload);

      if (Object.keys(traitUpdatePayload).length) {
        const { error: traitUpdateError } = await supabase
          .schema("elements")
          .from("member_spiritual_traits")
          .update(traitUpdatePayload)
          .eq("member_id", requestRow.member_id);

        if (traitUpdateError) {
          if (previousMemberElementSnapshot) {
            try {
              await restoreMemberElementSnapshot(
                requestRow.member_id,
                previousMemberElementSnapshot
              );
            } catch (rollbackError) {
              console.error(
                "approve-element-request member rollback error:",
                rollbackError
              );
            }
          }

          return json(500, {
            success: false,
            error: "failed_to_apply_traits",
            message: traitUpdateError.message
          });
        }

        const postProfileReload = await loadMemberAndTraits(requestRow.member_id);
        resultingMemberRow = postProfileReload.memberRow;
        resultingTraitsRow = postProfileReload.traitsRow;
      }
    } else if (isRootRequest) {
      const built = buildTraitUpdateForRootApproval(traitsRow, memberRow, requestRow);
      traitUpdatePayload = built.updatePayload;
      computedRootResult = built.computed;

      if (!Object.keys(traitUpdatePayload).length) {
        return json(500, {
          success: false,
          error: "no_root_fields_to_update",
          message: "No matching root fields were found for this approval."
        });
      }

      const projectedTraits = {
        ...traitsRow,
        ...traitUpdatePayload
      };

      spiritProfileTitle = buildSpiritRootProfileTitle(memberRow, projectedTraits);
      applySpiritProfileFields(traitUpdatePayload, traitsRow, spiritProfileTitle);
      previousTraitValues = snapshotForRollback(traitsRow, traitUpdatePayload);

      const { error: traitUpdateError } = await supabase
        .schema("elements")
        .from("member_spiritual_traits")
        .update(traitUpdatePayload)
        .eq("member_id", requestRow.member_id);

      if (traitUpdateError) {
        return json(500, {
          success: false,
          error: "failed_to_apply_traits",
          message: traitUpdateError.message
        });
      }

      const postRootReload = await loadMemberAndTraits(requestRow.member_id);
      resultingMemberRow = postRootReload.memberRow;
      resultingTraitsRow = postRootReload.traitsRow;
    } else {
      return json(400, {
        success: false,
        error: "invalid_request_target",
        message: "Unsupported request_target."
      });
    }

    const requestUpdatePayload = {
      status: "approved",
      review_notes: reviewNotes,
      reviewed_by: safeText(auth.admin.sl_username),
      reviewed_at: reviewedAt,
      updated_at: reviewedAt
    };

    if (Object.prototype.hasOwnProperty.call(requestRow, "approved_at")) {
      requestUpdatePayload.approved_at = reviewedAt;
    }

    const { data: updatedRequest, error: requestUpdateError } = await supabase
      .schema("elements")
      .from("element_requests")
      .update(requestUpdatePayload)
      .eq("request_id", requestRow.request_id)
      .select("*")
      .maybeSingle();

    if (requestUpdateError) {
      if (Object.keys(previousTraitValues).length) {
        await supabase
          .schema("elements")
          .from("member_spiritual_traits")
          .update(previousTraitValues)
          .eq("member_id", requestRow.member_id);
      }

      if (isElementRequest && previousMemberElementSnapshot) {
        try {
          await restoreMemberElementSnapshot(
            requestRow.member_id,
            previousMemberElementSnapshot
          );
        } catch (rollbackError) {
          console.error("approve-element-request member rollback error:", rollbackError);
        }
      }

      return json(500, {
        success: false,
        error: "failed_to_mark_request_approved",
        message: requestUpdateError.message
      });
    }

    return json(200, {
      success: true,
      message: isRootRequest
        ? "Root petition approved successfully."
        : "Element petition approved successfully.",
      request: updatedRequest,
      applied_trait_updates: traitUpdatePayload,
      spirit_root_profile: spiritProfileTitle,
      root_recalculation: computedRootResult,
      member_root_balance: extractRootBalancePayload(refreshedRootBalance),
      approved_element_change: isElementRequest
        ? {
            slot_key: requestTarget,
            requested_element: normalizeElementValueForMember(
              getApprovedElementValueForRpc(requestTarget, requestRow.requested_element)
            ),

            resulting_primary_element: normalizeElementValueForMember(
              resultingMemberRow.primary_element || resultingTraitsRow.current_primary_element
            ),
            resulting_secondary_element: normalizeElementValueForMember(
              resultingMemberRow.secondary_element ||
                resultingTraitsRow.current_secondary_element
            ),
            resulting_path_element: normalizeElementValueForMember(
              resultingMemberRow.path_element || resultingTraitsRow.current_path_element
            ),
            resulting_third_element: normalizeElementValueForMember(
              resultingMemberRow.third_element || resultingTraitsRow.current_third_element
            ),

            resolved_primary_element:
              requestTarget === "third" && thirdResolution
                ? safeLower(thirdResolution.primary_element_key)
                : null,
            resolved_path_element:
              requestTarget === "third" && thirdResolution
                ? safeLower(
                    thirdResolution.path_element_key ||
                      thirdResolution.secondary_element_key
                  )
                : null,
            resolved_third_element:
              requestTarget === "third" && thirdResolution
                ? safeLower(thirdResolution.third_element_key)
                : null
          }
        : null
    });
  } catch (error) {
    return json(500, {
      success: false,
      error: "server_error",
      message: error.message
    });
  }
};