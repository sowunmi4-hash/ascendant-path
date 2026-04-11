const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

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

function cleanText(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function cleanLower(value) {
  return cleanText(value).toLowerCase();
}

function parseNonNegativeInt(value) {
  if (value === undefined || value === null || value === "") return null;

  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 0) return null;

  return parsed;
}

function normalizePathType(value) {
  const raw = cleanLower(value);

  if (!raw) return "";

  if (raw === "single" || raw === "single path") return "single";
  if (raw === "dual" || raw === "dual path") return "dual";
  if (raw === "hybrid" || raw === "hybrid path") return "hybrid";

  if (raw === "both") return "hybrid";

  return "";
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
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

    const sl_avatar_key = cleanText(body.sl_avatar_key);
    const sl_username = cleanLower(body.sl_username);
    const display_name = cleanText(body.display_name);

    const character_name = cleanText(body.character_name);
    const cultivation_path = cleanText(body.cultivation_path);
    const path_type = normalizePathType(body.path_type);

    const character_age = parseNonNegativeInt(body.character_age);
    const mortal_energy = parseNonNegativeInt(body.mortal_energy);

    if (!sl_avatar_key || !sl_username || !display_name) {
      return buildResponse(400, {
        success: false,
        message: "Missing required fields: sl_avatar_key, sl_username, display_name"
      });
    }

    if (cleanText(body.path_type) && !path_type) {
      return buildResponse(400, {
        success: false,
        message: "Invalid path_type. Use single, dual, or hybrid."
      });
    }

    const now = new Date().toISOString();

    const { data: existingMember, error: findError } = await supabase
      .from("cultivation_members")
      .select("*")
      .eq("sl_avatar_key", sl_avatar_key)
      .maybeSingle();

    if (findError) {
      console.error("link-sl-account lookup error:", findError);
      return buildResponse(500, {
        success: false,
        message: "Failed to check cultivation member record.",
        error: findError.message
      });
    }

    if (!existingMember) {
      const starterProfile = {
        sl_avatar_key,
        sl_username,
        display_name,

        character_name: character_name || null,
        character_age,
        cultivation_path: cultivation_path || null,
        path_type: path_type || null,
        mortal_energy: mortal_energy !== null ? mortal_energy : 0,

        realm_index: 1,
        realm_key: "mortal",
        realm_name: "mortal",
        realm_display_name: "Mortal Realm",

        qi_current: 0,
        qi_maximum: 500,
        cultivation_points: 0,

        v2_cultivation_status: "idle",
        v2_active_stage_key: null,
        v2_breakthrough_gate_open: false,

        has_spirit_analysis: false,

        primary_element: "",
        secondary_element: "",
        path_element: "",
        third_element: "",

        spirit_root_values: [0, 0, 0, 0, 0],
        volume_progress: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],

        last_saved: now,
        updated_at: now
      };

      const { data, error } = await supabase
        .from("cultivation_members")
        .insert(starterProfile)
        .select()
        .single();

      if (error) {
        console.error("link-sl-account insert error:", error);
        return buildResponse(500, {
          success: false,
          message: "Failed to create registration record.",
          error: error.message
        });
      }

      return buildResponse(200, {
        success: true,
        message: "Registration created successfully.",
        member: data
      });
    }

    const updatePayload = {
      sl_username,
      display_name,
      character_name: character_name || null,
      character_age,
      cultivation_path: cultivation_path || null,
      path_type: path_type || null,
      mortal_energy:
        mortal_energy !== null
          ? mortal_energy
          : (existingMember.mortal_energy || 0),
      last_saved: now,
      updated_at: now
    };

    const { data, error } = await supabase
      .from("cultivation_members")
      .update(updatePayload)
      .eq("sl_avatar_key", sl_avatar_key)
      .select()
      .single();

    if (error) {
      console.error("link-sl-account update error:", error);
      return buildResponse(500, {
        success: false,
        message: "Failed to update registration record.",
        error: error.message
      });
    }

    return buildResponse(200, {
      success: true,
      message: "Registration updated successfully.",
      member: data
    });
  } catch (error) {
    console.error("link-sl-account server error:", error);

    return buildResponse(500, {
      success: false,
      message: "Server error",
      error: error.message
    });
  }
};