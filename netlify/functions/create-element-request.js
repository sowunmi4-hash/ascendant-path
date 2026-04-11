const { createClient } = require("@supabase/supabase-js");

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

const PRIMARY_UNLOCK_REALM_INDEX = 2;
const SECONDARY_UNLOCK_REALM_INDEX = 3;
const THIRD_UNLOCK_REALM_INDEX = 5;

const PETITION_PRICING = {
  primary: {
    submission_cost: 35,
    attunement_cost: 15,
    total_cost: 50,
    refund_if_denied: 15
  },
  secondary: {
    submission_cost: 70,
    attunement_cost: 30,
    total_cost: 100,
    refund_if_denied: 30
  },
  third: {
    submission_cost: 140,
    attunement_cost: 60,
    total_cost: 200,
    refund_if_denied: 60
  },
  root_minor: {
    submission_cost: 35,
    attunement_cost: 15,
    total_cost: 50,
    refund_if_denied: 15
  },
  root_major: {
    submission_cost: 70,
    attunement_cost: 30,
    total_cost: 100,
    refund_if_denied: 30
  }
};

const ELEMENT_REQUEST_TARGETS = ["primary", "secondary", "third"];
const ROOT_REQUEST_TARGETS = ["root_minor", "root_major"];
const ALL_REQUEST_TARGETS = [
  ...ELEMENT_REQUEST_TARGETS,
  ...ROOT_REQUEST_TARGETS
];

const BASE_ELEMENT_OPTIONS = ["Metal", "Wood", "Water", "Fire", "Earth"];

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
  "metal|sound": ["Will", "Mind", "Spirit Qi"],
  "metal|ice": ["Pure Yin", "Moon", "Heaven & Earth Qi"],
  "metal|lightning": ["Yang", "Calamity", "Heaven & Earth Qi"],
  "metal|gravity": ["Space", "Fortune", "Immortal Qi"],

  "wood|sound": ["Soul", "Mind", "Destiny"],
  "wood|wind": ["Time", "Destiny", "Spirit Qi"],
  "wood|light": ["Creation", "Holy", "Life"],
  "wood|poison": ["Demon", "Death", "Abyssal Qi"],

  "water|ice": ["Pure Yin", "Moon", "Nether Qi"],
  "water|wind": ["Time", "Space", "Void"],
  "water|thunder": ["Dragon", "Karma", "Calamity"],
  "water|darkness": ["Pure Yin", "Void", "Nether Qi"],

  "fire|lightning": ["Yang", "Sun", "Calamity"],
  "fire|light": ["Sun", "Holy", "Creation"],
  "fire|thunder": ["Dragon", "Phoenix", "Calamity"],
  "fire|shadow": ["Chaos", "Demon", "Abyssal Qi"],

  "earth|gravity": ["Space", "Fortune", "Immortal Qi"],
  "earth|poison": ["Death", "Nether Qi", "Demon"],
  "earth|darkness": ["Pure Yin", "Death", "Nether Qi"],
  "earth|shadow": ["Demon", "Abyssal Qi", "Chaos"]
};

const ROOT_OPTIONS = ["Metal", "Wood", "Water", "Fire", "Earth"];

const ROOT_FIELD_CANDIDATES = {
  metal: ["current_metal_root", "natural_metal_root", "metal_root"],
  wood: ["current_wood_root", "natural_wood_root", "wood_root"],
  water: ["current_water_root", "natural_water_root", "water_root"],
  fire: ["current_fire_root", "natural_fire_root", "fire_root"],
  earth: ["current_earth_root", "natural_earth_root", "earth_root"]
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

function normalizeElement(value) {
  return safeLower(value).replace(/\s+/g, " ");
}

function normalizePairKey(a, b) {
  const first = normalizeElement(a);
  const second = normalizeElement(b);
  if (!first || !second) return "";
  return [first, second].sort().join("|");
}

function titleCaseElement(value) {
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

function getPricingForTarget(requestTarget) {
  return PETITION_PRICING[requestTarget] || null;
}

function canonicalBaseElement(value) {
  const lowered = safeLower(value);
  return BASE_ELEMENT_OPTIONS.find((element) => safeLower(element) === lowered) || "";
}

function derivePathElement(primaryElement, secondaryElement) {
  const pairKey = normalizePairKey(primaryElement, secondaryElement);
  return PATH_ELEMENT_BY_BASE_PAIR[pairKey] || "";
}

function buildPrimaryPathKey(primaryElement, pathElement) {
  const primary = normalizeElement(primaryElement);
  const path = normalizeElement(pathElement);
  if (!primary || !path) return "";
  return `${primary}|${path}`;
}

function getCurrentPrimaryElement(memberRow, traitsRow) {
  return safeText(
    memberRow?.primary_element_key ||
      memberRow?.primary_element ||
      memberRow?.element_primary ||
      traitsRow?.current_primary_element ||
      traitsRow?.natural_primary_element
  );
}

function getCurrentSecondaryElement(memberRow, traitsRow) {
  return safeText(
    memberRow?.secondary_element_key ||
      memberRow?.secondary_element ||
      memberRow?.element_secondary ||
      traitsRow?.current_secondary_element ||
      traitsRow?.natural_secondary_element
  );
}

function getCurrentPathElement(memberRow, traitsRow) {
  const explicitPath = safeText(
    memberRow?.path_element_key ||
      memberRow?.path_element ||
      memberRow?.element_path ||
      traitsRow?.current_path_element ||
      traitsRow?.natural_path_element
  );

  if (explicitPath) return explicitPath;

  return derivePathElement(
    getCurrentPrimaryElement(memberRow, traitsRow),
    getCurrentSecondaryElement(memberRow, traitsRow)
  );
}

function getCurrentThirdElement(memberRow, traitsRow) {
  return safeText(
    memberRow?.third_element_key ||
      memberRow?.third_element ||
      memberRow?.element_third ||
      memberRow?.tertiary_element_key ||
      memberRow?.tertiary_element ||
      traitsRow?.current_third_element ||
      traitsRow?.natural_third_element
  );
}

function isThirdUnlocked(memberRow, traitsRow) {
  const unlockedFlag =
    safeBoolean(memberRow?.third_element_unlocked) ||
    safeBoolean(memberRow?.is_third_unlocked) ||
    safeBoolean(memberRow?.third_unlocked) ||
    safeBoolean(traitsRow?.third_element_unlocked) ||
    safeBoolean(traitsRow?.is_third_unlocked) ||
    safeBoolean(traitsRow?.third_unlocked);

  return unlockedFlag && Boolean(getCurrentThirdElement(memberRow, traitsRow));
}

function getAllowedOptionsForTarget(requestTarget, context = {}) {
  if (requestTarget === "primary") return BASE_ELEMENT_OPTIONS;
  if (requestTarget === "secondary") return BASE_ELEMENT_OPTIONS;

  if (requestTarget === "third") {
    const thirdPoolKey = buildPrimaryPathKey(
      context.primaryElement,
      context.pathElement
    );

    return THIRD_POOL_MAP[thirdPoolKey] || [];
  }

  return [];
}

function isAllowedRequestedElement(requestTarget, requestedElement, context = {}) {
  const allowed = getAllowedOptionsForTarget(requestTarget, context);
  return allowed.some((value) => safeLower(value) === safeLower(requestedElement));
}

function canonicalRequestedElement(requestTarget, requestedElement, context = {}) {
  const allowed = getAllowedOptionsForTarget(requestTarget, context);
  return (
    allowed.find((value) => safeLower(value) === safeLower(requestedElement)) || ""
  );
}

function canonicalRootName(value) {
  const lowered = safeLower(value);
  return ROOT_OPTIONS.find((root) => safeLower(root) === lowered) || "";
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

  for (const rootName of ROOT_OPTIONS) {
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

function buildElementPetitionPreview({
  requestTarget,
  currentPrimaryElement,
  currentSecondaryElement,
  currentPathElement,
  currentThirdElement,
  requestedElement
}) {
  let nextPrimary = currentPrimaryElement;
  let nextSecondary = currentSecondaryElement;
  let nextThird = currentThirdElement;

  if (requestTarget === "primary") {
    nextPrimary = requestedElement;
  }

  if (requestTarget === "secondary") {
    nextSecondary = requestedElement;
  }

  if (requestTarget === "third") {
    nextThird = requestedElement;
  }

  const nextPath = derivePathElement(nextPrimary, nextSecondary);

  return {
    before: {
      primary_element: currentPrimaryElement,
      secondary_element: currentSecondaryElement,
      path_element: currentPathElement,
      third_element: currentThirdElement,
      base_pair_key: normalizePairKey(currentPrimaryElement, currentSecondaryElement),
      primary_path_key: buildPrimaryPathKey(currentPrimaryElement, currentPathElement)
    },
    after: {
      primary_element: nextPrimary,
      secondary_element: nextSecondary,
      path_element: nextPath,
      third_element: nextThird,
      base_pair_key: normalizePairKey(nextPrimary, nextSecondary),
      primary_path_key: buildPrimaryPathKey(nextPrimary, nextPath)
    },
    path_changes: safeLower(currentPathElement) !== safeLower(nextPath)
  };
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

    const body = parseBody(event);

    const requestTarget = safeLower(body.request_target);
    const realmIndex = getRealmIndex(auth.member);

    if (!ALL_REQUEST_TARGETS.includes(requestTarget)) {
      return json(400, {
        success: false,
        error: "invalid_request_target",
        message: "Request target must be primary, secondary, third, root_minor, or root_major."
      });
    }

    const pricing = getPricingForTarget(requestTarget);
    if (!pricing) {
      return json(400, {
        success: false,
        error: "invalid_pricing_target"
      });
    }

    const memberId = auth.member.member_id;
    const slAvatarKey = safeText(auth.member.sl_avatar_key);
    const slUsername = safeText(auth.member.sl_username).toLowerCase();

    const { data: traitsRow, error: traitsError } = await supabase
      .schema("elements")
      .from("member_spiritual_traits")
      .select("*")
      .eq("member_id", memberId)
      .maybeSingle();

    if (traitsError || !traitsRow) {
      return json(404, {
        success: false,
        error: "traits_not_found"
      });
    }

    const { data: pendingRequest, error: pendingError } = await supabase
      .schema("elements")
      .from("element_requests")
      .select("*")
      .eq("member_id", memberId)
      .eq("status", "pending")
      .limit(1)
      .maybeSingle();

    if (pendingError) {
      return json(500, {
        success: false,
        error: "failed_to_check_pending_request",
        message: pendingError.message
      });
    }

    if (pendingRequest) {
      return json(409, {
        success: false,
        error: "pending_request_exists",
        message: "You already have a pending petition under review."
      });
    }

    const currentPrimaryElement = getCurrentPrimaryElement(auth.member, traitsRow);
    const currentSecondaryElement = getCurrentSecondaryElement(auth.member, traitsRow);
    const currentPathElement = getCurrentPathElement(auth.member, traitsRow);
    const currentThirdElement = getCurrentThirdElement(auth.member, traitsRow);
    const thirdUnlocked = isThirdUnlocked(auth.member, traitsRow);

    let requestedElement = null;
    let increaseRoot = null;
    let decreaseRoot = null;
    let shiftAmount = null;
    let petitionPreview = null;

    if (ELEMENT_REQUEST_TARGETS.includes(requestTarget)) {
      const rawRequestedElement = safeText(body.requested_element);

      if (!rawRequestedElement) {
        return json(400, {
          success: false,
          error: "missing_requested_element",
          message: "Requested element is required."
        });
      }

      if (requestTarget === "primary") {
        if (realmIndex < PRIMARY_UNLOCK_REALM_INDEX) {
          return json(409, {
            success: false,
            error: "primary_not_unlocked",
            message: "Primary petitions begin at Qi Gathering."
          });
        }

        if (!currentPrimaryElement) {
          return json(409, {
            success: false,
            error: "primary_not_available",
            message: "Primary element is not available yet."
          });
        }

        if (!currentSecondaryElement) {
          return json(409, {
            success: false,
            error: "secondary_not_available",
            message: "Secondary base element must exist before petitioning a primary change."
          });
        }

        requestedElement = canonicalBaseElement(rawRequestedElement);

        if (!requestedElement) {
          return json(400, {
            success: false,
            error: "invalid_requested_element",
            message: "Primary petitions must request a base element."
          });
        }

        if (safeLower(requestedElement) === safeLower(currentPrimaryElement)) {
          return json(409, {
            success: false,
            error: "same_element_requested",
            message: "You already have that primary element."
          });
        }

        const derivedPath = derivePathElement(requestedElement, currentSecondaryElement);

        if (!derivedPath) {
          return json(400, {
            success: false,
            error: "invalid_base_pair",
            message: "That primary + secondary combination does not resolve to a valid path.",
            requested_primary_element: requestedElement,
            current_secondary_element: currentSecondaryElement
          });
        }

        petitionPreview = buildElementPetitionPreview({
          requestTarget,
          currentPrimaryElement,
          currentSecondaryElement,
          currentPathElement,
          currentThirdElement,
          requestedElement
        });
      }

      if (requestTarget === "secondary") {
        if (realmIndex < SECONDARY_UNLOCK_REALM_INDEX) {
          return json(409, {
            success: false,
            error: "secondary_not_unlocked",
            message: "Secondary petitions begin at Foundation."
          });
        }

        if (!currentPrimaryElement) {
          return json(409, {
            success: false,
            error: "primary_not_available",
            message: "Primary base element must exist before petitioning a secondary change."
          });
        }

        if (!currentSecondaryElement) {
          return json(409, {
            success: false,
            error: "secondary_not_available",
            message: "Secondary element is not available yet."
          });
        }

        requestedElement = canonicalBaseElement(rawRequestedElement);

        if (!requestedElement) {
          return json(400, {
            success: false,
            error: "invalid_requested_element",
            message: "Secondary petitions must request a base element."
          });
        }

        if (safeLower(requestedElement) === safeLower(currentSecondaryElement)) {
          return json(409, {
            success: false,
            error: "same_element_requested",
            message: "You already have that secondary element."
          });
        }

        const derivedPath = derivePathElement(currentPrimaryElement, requestedElement);

        if (!derivedPath) {
          return json(400, {
            success: false,
            error: "invalid_base_pair",
            message: "That primary + secondary combination does not resolve to a valid path.",
            current_primary_element: currentPrimaryElement,
            requested_secondary_element: requestedElement
          });
        }

        petitionPreview = buildElementPetitionPreview({
          requestTarget,
          currentPrimaryElement,
          currentSecondaryElement,
          currentPathElement,
          currentThirdElement,
          requestedElement
        });
      }

      if (requestTarget === "third") {
        if (realmIndex < THIRD_UNLOCK_REALM_INDEX) {
          return json(409, {
            success: false,
            error: "third_not_eligible",
            message: "Third element does not become eligible until Nascent Soul."
          });
        }

        if (!thirdUnlocked || !currentThirdElement) {
          return json(409, {
            success: false,
            error: "third_not_unlocked",
            message: "Third element must be unlocked first before petitioning a third change."
          });
        }

        if (!currentPrimaryElement || !currentPathElement) {
          return json(409, {
            success: false,
            error: "third_context_incomplete",
            message: "Primary and path elements must be present before petitioning a third change."
          });
        }

        const currentThirdPool = getAllowedOptionsForTarget("third", {
          primaryElement: currentPrimaryElement,
          pathElement: currentPathElement
        });

        if (!currentThirdPool.length) {
          return json(409, {
            success: false,
            error: "third_pool_not_found",
            message: "No valid third pool exists for your current primary and path.",
            primary_element: currentPrimaryElement,
            path_element: currentPathElement
          });
        }

        if (!isAllowedRequestedElement("third", rawRequestedElement, {
          primaryElement: currentPrimaryElement,
          pathElement: currentPathElement
        })) {
          return json(400, {
            success: false,
            error: "invalid_requested_element",
            message: "Requested third element is not valid for your current primary and path.",
            primary_element: currentPrimaryElement,
            path_element: currentPathElement,
            allowed_third_elements: currentThirdPool
          });
        }

        requestedElement = canonicalRequestedElement("third", rawRequestedElement, {
          primaryElement: currentPrimaryElement,
          pathElement: currentPathElement
        });

        if (safeLower(requestedElement) === safeLower(currentThirdElement)) {
          return json(409, {
            success: false,
            error: "same_element_requested",
            message: "You already have that third element."
          });
        }

        petitionPreview = {
          ...buildElementPetitionPreview({
            requestTarget,
            currentPrimaryElement,
            currentSecondaryElement,
            currentPathElement,
            currentThirdElement,
            requestedElement
          }),
          allowed_third_elements: currentThirdPool
        };
      }
    }

    if (ROOT_REQUEST_TARGETS.includes(requestTarget)) {
      if (realmIndex !== 1) {
        return json(409, {
          success: false,
          error: "root_petitions_closed",
          message: "Root petitions are only allowed in the Mortal Realm."
        });
      }

      increaseRoot = canonicalRootName(body.increase_root);
      decreaseRoot = canonicalRootName(body.decrease_root);
      shiftAmount = parsePositiveInteger(body.shift_amount);

      if (!increaseRoot) {
        return json(400, {
          success: false,
          error: "missing_increase_root",
          message: "You must choose which root to increase."
        });
      }

      if (!decreaseRoot) {
        return json(400, {
          success: false,
          error: "missing_decrease_root",
          message: "You must choose which root to decrease."
        });
      }

      if (!shiftAmount) {
        return json(400, {
          success: false,
          error: "invalid_shift_amount",
          message: "Shift amount must be a whole number greater than 0."
        });
      }

      if (safeLower(increaseRoot) === safeLower(decreaseRoot)) {
        return json(400, {
          success: false,
          error: "same_root_selected",
          message: "Increase root and decrease root cannot be the same."
        });
      }

      const { map: currentRoots, hasMissingRoot } = getCurrentRootMap(traitsRow);

      if (hasMissingRoot) {
        return json(409, {
          success: false,
          error: "root_data_incomplete",
          message: "Current root data is incomplete and cannot be petitioned yet."
        });
      }

      const decreaseKey = safeLower(decreaseRoot);
      const increaseKey = safeLower(increaseRoot);

      const currentDecreaseValue = Number(currentRoots[decreaseKey] || 0);
      const currentIncreaseValue = Number(currentRoots[increaseKey] || 0);

      if (currentDecreaseValue - shiftAmount < 0) {
        return json(400, {
          success: false,
          error: "root_would_go_below_zero",
          message: `${decreaseRoot} root cannot go below 0.`,
          current_root_value: currentDecreaseValue,
          requested_shift_amount: shiftAmount
        });
      }

      const expectedMortalEnergy = getExpectedMortalEnergy(traitsRow, auth.member);

      if (!Number.isFinite(expectedMortalEnergy)) {
        return json(409, {
          success: false,
          error: "missing_mortal_energy_snapshot",
          message: "Mortal Energy snapshot is missing for this member."
        });
      }

      const proposedRoots = {
        ...currentRoots,
        [increaseKey]: currentIncreaseValue + shiftAmount,
        [decreaseKey]: currentDecreaseValue - shiftAmount
      };

      if (proposedRoots[increaseKey] > 60) {
        return json(400, {
          success: false,
          error: "root_would_exceed_max",
          message: `${increaseRoot} root cannot go above 60.`,
          current_root_value: currentIncreaseValue,
          requested_shift_amount: shiftAmount,
          proposed_value: proposedRoots[increaseKey],
          max_allowed: 60
        });
      }

      const proposedTotal = sumRootMap(proposedRoots);

      if (proposedTotal !== expectedMortalEnergy) {
        return json(409, {
          success: false,
          error: "root_total_mismatch",
          message: "Proposed roots must still equal Mortal Energy.",
          expected_total: expectedMortalEnergy,
          proposed_total: proposedTotal
        });
      }

      petitionPreview = {
        increase_root: increaseRoot,
        decrease_root: decreaseRoot,
        shift_amount: shiftAmount,
        before: {
          metal: currentRoots.metal,
          wood: currentRoots.wood,
          water: currentRoots.water,
          fire: currentRoots.fire,
          earth: currentRoots.earth
        },
        after: {
          metal: proposedRoots.metal,
          wood: proposedRoots.wood,
          water: proposedRoots.water,
          fire: proposedRoots.fire,
          earth: proposedRoots.earth
        },
        mortal_energy_target: expectedMortalEnergy
      };
    }

    const { data: walletRow, error: walletError } = await supabase
      .from("member_wallets")
      .select("*")
      .eq("sl_avatar_key", slAvatarKey)
      .maybeSingle();

    if (walletError || !walletRow) {
      return json(404, {
        success: false,
        error: "wallet_not_found"
      });
    }

    const currentBalance = Number(walletRow.ascension_tokens_balance || 0);

    if (currentBalance < pricing.total_cost) {
      return json(400, {
        success: false,
        error: "insufficient_tokens",
        message: `You need ${pricing.total_cost} Ascension Tokens to submit this petition.`,
        tokens_required: pricing.total_cost,
        current_balance: currentBalance,
        shortfall: pricing.total_cost - currentBalance
      });
    }

    const newBalance = currentBalance - pricing.total_cost;

    const { error: walletUpdateError } = await supabase
      .from("member_wallets")
      .update({
        ascension_tokens_balance: newBalance,
        updated_at: new Date().toISOString()
      })
      .eq("sl_avatar_key", slAvatarKey);

    if (walletUpdateError) {
      return json(500, {
        success: false,
        error: "failed_to_charge_tokens",
        message: walletUpdateError.message
      });
    }

    const insertPayload = {
      member_id: memberId,
      sl_avatar_key: slAvatarKey,
      sl_username: slUsername,
      requested_element: requestedElement || "",
      request_target: requestTarget,
      increase_root: increaseRoot,
      decrease_root: decreaseRoot,
      shift_amount: shiftAmount,
      status: "pending",
      submission_cost: pricing.submission_cost,
      attunement_cost: pricing.attunement_cost,
      total_cost: pricing.total_cost,
      refund_if_denied: pricing.refund_if_denied
    };

    const { data: insertedRequest, error: insertError } = await supabase
      .schema("elements")
      .from("element_requests")
      .insert(insertPayload)
      .select("*")
      .maybeSingle();

    if (insertError) {
      await supabase
        .from("member_wallets")
        .update({
          ascension_tokens_balance: currentBalance,
          updated_at: new Date().toISOString()
        })
        .eq("sl_avatar_key", slAvatarKey);

      return json(500, {
        success: false,
        error: "failed_to_create_request",
        message: insertError.message
      });
    }

    return json(200, {
      success: true,
      message:
        ROOT_REQUEST_TARGETS.includes(requestTarget)
          ? "Root petition submitted successfully."
          : "Element petition submitted successfully.",
      request: insertedRequest,
      pricing: {
        request_target: requestTarget,
        submission_cost: pricing.submission_cost,
        attunement_cost: pricing.attunement_cost,
        total_cost: pricing.total_cost,
        refund_if_denied: pricing.refund_if_denied
      },
      wallet: {
        spent: pricing.total_cost,
        balance_after: newBalance
      },
      petition_preview: petitionPreview,
      current_structure: {
        primary_element: currentPrimaryElement,
        secondary_element: currentSecondaryElement,
        path_element: currentPathElement,
        third_element: currentThirdElement,
        base_pair_key: normalizePairKey(currentPrimaryElement, currentSecondaryElement),
        primary_path_key: buildPrimaryPathKey(currentPrimaryElement, currentPathElement)
      }
    });
  } catch (error) {
    return json(500, {
      success: false,
      error: "server_error",
      message: error.message
    });
  }
};