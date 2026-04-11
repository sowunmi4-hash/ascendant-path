const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

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

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function safeNullableNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
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

function buildPrimaryPathKey(primaryElement, pathElement) {
  const primary = normalizeElement(primaryElement);
  const path = normalizeElement(pathElement);

  if (!primary || !path) return "";
  return `${primary}|${path}`;
}

function derivePathElement(primaryElement, secondaryElement) {
  const pairKey = normalizePairKey(primaryElement, secondaryElement);
  return PATH_ELEMENT_BY_BASE_PAIR[pairKey] || "";
}

function titleCaseElement(value) {
  return safeText(value)
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatRequestTypeMeta(row) {
  const target = safeLower(row.request_target);

  if (target === "primary") {
    return {
      request_type_label: "Base Element 1 Petition",
      request_type_group: "element",
      request_type_copy:
        "Petition to change Base Element 1. Path Element is derived from Base Element 1 + Base Element 2.",
      slot_label: "Base Element 1",
      is_path_recalculation: true,
      admin_rule_note:
        "Approving this petition rewrites primary_element and recalculates path_element."
    };
  }

  if (target === "secondary") {
    return {
      request_type_label: "Base Element 2 Petition",
      request_type_group: "element",
      request_type_copy:
        "Petition to change Base Element 2. Path Element is derived from Base Element 1 + Base Element 2.",
      slot_label: "Base Element 2",
      is_path_recalculation: true,
      admin_rule_note:
        "Approving this petition rewrites secondary_element and recalculates path_element."
    };
  }

  if (target === "third") {
    return {
      request_type_label: "Third Element Petition",
      request_type_group: "element",
      request_type_copy:
        "Petition to change the true third element. Base slots and Path Element remain separate layers.",
      slot_label: "Third Element",
      is_path_recalculation: false,
      admin_rule_note:
        "Approving this petition rewrites third_element only and reapplies third root pressure."
    };
  }

  if (target === "root_minor") {
    return {
      request_type_label: "Minor Root Petition",
      request_type_group: "root",
      request_type_copy: "Mortal-only minor root reallocation.",
      slot_label: "Roots",
      is_path_recalculation: false,
      admin_rule_note: ""
    };
  }

  if (target === "root_major") {
    return {
      request_type_label: "Major Root Petition",
      request_type_group: "root",
      request_type_copy: "Mortal-only major root reallocation.",
      slot_label: "Roots",
      is_path_recalculation: false,
      admin_rule_note: ""
    };
  }

  return {
    request_type_label: "Unknown Petition",
    request_type_group: "unknown",
    request_type_copy: "Unrecognized petition type.",
    slot_label: "",
    is_path_recalculation: false,
    admin_rule_note: ""
  };
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

function buildCurrentStructure(memberRow, traitsRow) {
  const primaryElement = getCurrentPrimaryElement(memberRow, traitsRow);
  const secondaryElement = getCurrentSecondaryElement(memberRow, traitsRow);
  const pathElement = getCurrentPathElement(memberRow, traitsRow);
  const thirdElement = getCurrentThirdElement(memberRow, traitsRow);

  return {
    primary_element: primaryElement,
    secondary_element: secondaryElement,
    path_element: pathElement,
    third_element: thirdElement,
    base_pair_key: normalizePairKey(primaryElement, secondaryElement),
    base_pair_label:
      primaryElement && secondaryElement
        ? `${titleCaseElement(primaryElement)} + ${titleCaseElement(secondaryElement)}`
        : "",
    primary_path_key: buildPrimaryPathKey(primaryElement, pathElement),
    primary_path_label:
      primaryElement && pathElement
        ? `${titleCaseElement(primaryElement)} + ${titleCaseElement(pathElement)}`
        : ""
  };
}

function buildProjectedStructure(row, currentStructure) {
  const target = safeLower(row.request_target);

  if (!["primary", "secondary", "third"].includes(target)) {
    return null;
  }

  let nextPrimary = currentStructure.primary_element;
  let nextSecondary = currentStructure.secondary_element;
  let nextThird = currentStructure.third_element;

  const requestedElement = safeText(row.requested_element);

  if (target === "primary") {
    nextPrimary = requestedElement;
  }

  if (target === "secondary") {
    nextSecondary = requestedElement;
  }

  if (target === "third") {
    nextThird = requestedElement;
  }

  const nextPath = derivePathElement(nextPrimary, nextSecondary);

  return {
    before: {
      primary_element: currentStructure.primary_element,
      secondary_element: currentStructure.secondary_element,
      path_element: currentStructure.path_element,
      third_element: currentStructure.third_element,
      base_pair_key: currentStructure.base_pair_key,
      primary_path_key: currentStructure.primary_path_key
    },
    after: {
      primary_element: nextPrimary,
      secondary_element: nextSecondary,
      path_element: nextPath,
      third_element: nextThird,
      base_pair_key: normalizePairKey(nextPrimary, nextSecondary),
      primary_path_key: buildPrimaryPathKey(nextPrimary, nextPath)
    },
    path_changed: safeLower(currentStructure.path_element) !== safeLower(nextPath),
    third_changed: safeLower(currentStructure.third_element) !== safeLower(nextThird),
    third_context_changed:
      safeLower(currentStructure.path_element) !== safeLower(nextPath) &&
      Boolean(currentStructure.third_element)
  };
}

async function requireSafareehillsAdmin(event) {
  const cookieHeader = event.headers.cookie || event.headers.Cookie || "";
  const cookies = parseCookies(cookieHeader);
  const sessionToken = cookies.ap_session;

  if (!sessionToken) {
    return {
      ok: false,
      response: json(401, { success: false, error: "not_logged_in" })
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
      response: json(401, { success: false, error: "invalid_session" })
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
      response: json(403, { success: false, error: "admin_not_found" })
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
      response: json(403, { success: false, error: "admin_access_denied" })
    };
  }

  return {
    ok: true,
    admin: memberRow
  };
}

async function loadMemberMap(memberIds) {
  if (!memberIds.length) return new Map();

  const { data, error } = await supabase
    .from("cultivation_members")
    .select("*")
    .in("member_id", memberIds);

  if (error) {
    throw new Error(`Failed to load cultivation members: ${error.message}`);
  }

  return new Map((data || []).map((row) => [row.member_id, row]));
}

async function loadTraitsMap(memberIds) {
  if (!memberIds.length) return new Map();

  const { data, error } = await supabase
    .schema("elements")
    .from("member_spiritual_traits")
    .select("*")
    .in("member_id", memberIds);

  if (error) {
    throw new Error(`Failed to load member spiritual traits: ${error.message}`);
  }

  return new Map((data || []).map((row) => [row.member_id, row]));
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const auth = await requireSafareehillsAdmin(event);
    if (!auth.ok) return auth.response;

    const { data, error } = await supabase
      .schema("elements")
      .from("element_requests")
      .select("*")
      .order("created_at", { ascending: false });

    if (error) {
      return json(500, {
        success: false,
        error: "failed_to_load_requests",
        message: error.message
      });
    }

    const requestRows = data || [];
    const memberIds = [...new Set(requestRows.map((row) => row.member_id).filter(Boolean))];

    const memberMap = await loadMemberMap(memberIds);
    const traitsMap = await loadTraitsMap(memberIds);

    const requests = requestRows.map((row) => {
      const meta = formatRequestTypeMeta(row);
      const memberRow = memberMap.get(row.member_id) || null;
      const traitsRow = traitsMap.get(row.member_id) || null;

      const currentStructure =
        memberRow || traitsRow
          ? buildCurrentStructure(memberRow, traitsRow)
          : null;

      const projectedStructure = currentStructure
        ? buildProjectedStructure(row, currentStructure)
        : null;

      return {
        id: row.id ?? null,
        request_id: safeNumber(row.request_id, 0),
        member_id: row.member_id,
        sl_avatar_key: safeText(row.sl_avatar_key),
        sl_username: safeText(row.sl_username),

        request_target: safeLower(row.request_target),
        request_type_label: meta.request_type_label,
        request_type_group: meta.request_type_group,
        request_type_copy: meta.request_type_copy,
        slot_label: meta.slot_label,
        is_path_recalculation: meta.is_path_recalculation,
        admin_rule_note: meta.admin_rule_note,

        requested_element: safeText(row.requested_element),

        increase_root: safeText(row.increase_root),
        decrease_root: safeText(row.decrease_root),
        shift_amount: safeNullableNumber(row.shift_amount),

        status: safeLower(row.status),

        submission_cost: safeNumber(row.submission_cost, 0),
        attunement_cost: safeNumber(row.attunement_cost, 0),
        total_cost: safeNumber(row.total_cost, 0),
        refund_if_denied: safeNumber(row.refund_if_denied, 0),

        review_notes: safeText(row.review_notes),
        reviewed_by: safeText(row.reviewed_by),
        reviewed_at: row.reviewed_at || null,
        created_at: row.created_at || null,
        updated_at: row.updated_at || null,

        current_structure: currentStructure,
        projected_structure: projectedStructure
      };
    });

    return json(200, {
      success: true,
      admin: {
        sl_username: safeText(auth.admin.sl_username),
        display_name: safeText(auth.admin.display_name)
      },
      count: requests.length,
      requests
    });
  } catch (error) {
    return json(500, {
      success: false,
      error: "server_error",
      message: error.message
    });
  }
};