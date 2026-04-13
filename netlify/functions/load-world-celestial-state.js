const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const celestialSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "celestial" }
  }
);

const breakthroughSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "breakthrough" }
  }
);

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
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function monthName(monthNumber) {
  const months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  ];

  const index = Number(monthNumber) - 1;
  return months[index] || "";
}

/**
 * OPTION B:
 * Treat slt_now as already being an SLT wall-clock value.
 * Do NOT convert it again.
 */
function parseSltWallClock(value) {
  if (value == null) return null;

  const raw = String(value).trim();
  if (!raw) return null;

  const match = raw.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?/
  );

  if (match) {
    return {
      raw,
      year: Number(match[1]),
      month: Number(match[2]),
      day: Number(match[3]),
      hour: Number(match[4] ?? 0),
      minute: Number(match[5] ?? 0),
      second: Number(match[6] ?? 0)
    };
  }

  const fallbackDate = new Date(raw);
  if (Number.isNaN(fallbackDate.getTime())) return null;

  return {
    raw,
    year: fallbackDate.getUTCFullYear(),
    month: fallbackDate.getUTCMonth() + 1,
    day: fallbackDate.getUTCDate(),
    hour: fallbackDate.getUTCHours(),
    minute: fallbackDate.getUTCMinutes(),
    second: fallbackDate.getUTCSeconds()
  };
}

function formatSltTimeFromParts(parts) {
  if (!parts) return "--";

  let hour12 = parts.hour % 12;
  if (hour12 === 0) hour12 = 12;

  const meridiem = parts.hour >= 12 ? "PM" : "AM";

  return `${hour12}:${pad2(parts.minute)} ${meridiem}`;
}

function formatSltDateFromParts(parts) {
  if (!parts) return "--";
  return `${monthName(parts.month)} ${parts.day}, ${parts.year}`;
}

function formatSltDateTimeFromParts(parts) {
  if (!parts) return "--";
  return `${formatSltDateFromParts(parts)} ${formatSltTimeFromParts(parts)}`;
}

function buildSltDisplayFields(row) {
  const parts = parseSltWallClock(row?.slt_now);

  return {
    slt_label: "SLT",
    slt_time_text: formatSltTimeFromParts(parts),
    slt_date_text: formatSltDateFromParts(parts),
    slt_datetime_text: formatSltDateTimeFromParts(parts),
    slt_parts: parts
      ? {
          year: parts.year,
          month: parts.month,
          day: parts.day,
          hour: parts.hour,
          minute: parts.minute,
          second: parts.second
        }
      : null
  };
}

function titleize(value, fallback = "") {
  const text = safeText(value);
  if (!text) return fallback;

  return text
    .replace(/_/g, " ")
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function normalizeTribulationState(raw) {
  const row = raw && typeof raw === "object" ? raw : {};

  return {
    success: Boolean(row.success),
    event_name: safeText(row.event_name, "Tribulation Lightning"),
    active_lightning_key: safeText(row.active_lightning_key) || null,
    active_lightning_name: safeText(row.active_lightning_name) || null,
    tribulation_lightning_active: Boolean(row.tribulation_lightning_active),
    active_event_count: safeNumber(row.active_event_count, 0),
    dominant_intensity: safeText(row.dominant_intensity) || null,
    omen_text: safeText(row.omen_text) || null,
    latest_target_id: row.latest_target_id || null,
    latest_started_at: row.latest_started_at || null,
    latest_sl_avatar_key: row.latest_sl_avatar_key || null,
    latest_sl_username: row.latest_sl_username || null,
    active_usernames: Array.isArray(row.active_usernames)
      ? row.active_usernames.filter((item) => safeText(item))
      : []
  };
}

function buildTribulationSummary(tribulation) {
  if (!tribulation?.tribulation_lightning_active) return "";

  const intensity = titleize(tribulation.dominant_intensity, "Major");
  const activeCount = safeNumber(tribulation.active_event_count, 0);
  const lightningName = safeText(tribulation.active_lightning_name);
  const subject =
    activeCount > 1
      ? `${activeCount} breakthrough events are shaking the heavens`
      : "a breakthrough is shaking the heavens";

  if (lightningName) {
    return `Tribulation Lightning — ${lightningName} • ${intensity} intensity • ${subject}.`;
  }

  return `Tribulation Lightning — ${intensity} intensity • ${subject}.`;
}

function buildWorldReportStrip(row, tribulation) {
  if (tribulation?.tribulation_lightning_active) {
    const parts = [];

    parts.push(tribulation.event_name || "Tribulation Lightning");

    if (tribulation.active_lightning_name) {
      parts.push(tribulation.active_lightning_name);
    }

    if (tribulation.dominant_intensity) {
      parts.push(`${titleize(tribulation.dominant_intensity)} Intensity`);
    }

    if (tribulation.omen_text) {
      parts.push(tribulation.omen_text);
    }

    if (row?.phase_name) {
      parts.push(`Beneath ${row.phase_name}`);
    }

    return `Celestial Report — ${parts.join(" • ")}`;
  }

  if (!row) return "Celestial Report — The heavens are unreadable.";

  const parts = [];

  if (row.phase_name) parts.push(row.phase_name);
  if (row.force_name) parts.push(row.force_name);
  if (row.has_active_phenomenon && row.phenomenon_name) parts.push(row.phenomenon_name);
  if (row.dashboard_effect_summary) parts.push(row.dashboard_effect_summary);
  if (row.year_title) parts.push(`Current Celestial Year: ${row.year_title}`);

  return `Celestial Report — ${parts.join(" • ")}`;
}

function applyTribulationOverlay(worldRow, tribulation) {
  const baseWorld = worldRow || {};
  const normalizedTribulation = normalizeTribulationState(tribulation);

  const world = {
    ...baseWorld,

    tribulation: normalizedTribulation,
    tribulation_lightning_active: normalizedTribulation.tribulation_lightning_active,
    tribulation_event_name: normalizedTribulation.event_name,
    tribulation_active_lightning_key: normalizedTribulation.active_lightning_key,
    tribulation_active_lightning_name: normalizedTribulation.active_lightning_name,
    tribulation_active_event_count: normalizedTribulation.active_event_count,
    tribulation_dominant_intensity: normalizedTribulation.dominant_intensity,
    tribulation_omen_text: normalizedTribulation.omen_text,
    tribulation_latest_target_id: normalizedTribulation.latest_target_id,
    tribulation_latest_started_at: normalizedTribulation.latest_started_at,
    tribulation_latest_sl_avatar_key: normalizedTribulation.latest_sl_avatar_key,
    tribulation_latest_sl_username: normalizedTribulation.latest_sl_username,
    tribulation_active_usernames: normalizedTribulation.active_usernames,

    world_event_active: normalizedTribulation.tribulation_lightning_active,
    world_event_key: normalizedTribulation.tribulation_lightning_active
      ? "tribulation_lightning"
      : null,
    world_event_name: normalizedTribulation.tribulation_lightning_active
      ? normalizedTribulation.event_name
      : null,
    world_event_title: normalizedTribulation.tribulation_lightning_active
      ? (normalizedTribulation.active_lightning_name || normalizedTribulation.event_name)
      : null,
    world_event_intensity: normalizedTribulation.tribulation_lightning_active
      ? normalizedTribulation.dominant_intensity
      : null,

    base_has_active_phenomenon: Boolean(baseWorld.has_active_phenomenon),
    base_phenomenon_name: safeText(baseWorld.phenomenon_name) || null,
    base_phenomenon_omen_text: safeText(baseWorld.phenomenon_omen_text) || null,
    base_dashboard_effect_summary: safeText(baseWorld.dashboard_effect_summary) || null
  };

  if (normalizedTribulation.tribulation_lightning_active) {
    world.has_active_phenomenon = true;
    world.phenomenon_name =
      normalizedTribulation.active_lightning_name ||
      normalizedTribulation.event_name;
    world.phenomenon_omen_text =
      normalizedTribulation.omen_text ||
      `${normalizedTribulation.active_lightning_name || "Tribulation Lightning"} is tearing across the heavens.`;
    world.dashboard_effect_summary =
      buildTribulationSummary(normalizedTribulation) ||
      "Tribulation Lightning is active across the world.";
  }

  return world;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const cookieHeader =
      event.headers.cookie ||
      event.headers.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionToken = cookies.ap_session;

    if (!sessionToken) {
      return json(401, {
        success: false,
        error: "not_logged_in"
      });
    }

    const now = new Date().toISOString();

    const { data: sessionRow, error: sessionError } = await supabase
      .from("website_sessions")
      .select("*")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();

    if (sessionError) {
      return json(500, {
        success: false,
        error: "session_lookup_failed",
        details: sessionError.message,
        hint: sessionError.hint || null,
        code: sessionError.code || null
      });
    }

    if (!sessionRow) {
      return json(401, {
        success: false,
        error: "invalid_session"
      });
    }

    const { data: memberRow, error: memberError } = await supabase
      .from("cultivation_members")
      .select("member_id, sl_avatar_key, sl_username, display_name")
      .eq("sl_avatar_key", sessionRow.sl_avatar_key)
      .maybeSingle();

    if (memberError) {
      return json(500, {
        success: false,
        error: "member_lookup_failed",
        details: memberError.message,
        hint: memberError.hint || null,
        code: memberError.code || null
      });
    }

    if (!memberRow) {
      return json(404, {
        success: false,
        error: "member_not_found"
      });
    }

    const [
      { data: celestialData, error: celestialError },
      { data: tribulationData, error: tribulationError }
    ] = await Promise.all([
      celestialSupabase.rpc("load_breath_of_celestial_state"),
      breakthroughSupabase.rpc("get_tribulation_lightning_world_state")
    ]);

    if (celestialError) {
      return json(500, {
        success: false,
        error: "celestial_rpc_failed",
        details: celestialError.message,
        hint: celestialError.hint || null,
        code: celestialError.code || null
      });
    }

    if (tribulationError) {
      return json(500, {
        success: false,
        error: "tribulation_rpc_failed",
        details: tribulationError.message,
        hint: tribulationError.hint || null,
        code: tribulationError.code || null
      });
    }

    const worldRow = Array.isArray(celestialData)
      ? celestialData[0] || null
      : celestialData || null;

    if (!worldRow) {
      return json(404, {
        success: false,
        error: "celestial_state_not_found"
      });
    }

    const overlaidWorld = applyTribulationOverlay(worldRow, tribulationData);

    const [sessionUpdateResult, memberUpdateResult] = await Promise.all([
      supabase
        .from("website_sessions")
        .update({ updated_at: now })
        .eq("session_token", sessionToken),
      supabase
        .from("cultivation_members")
        .update({ last_presence_at: now })
        .eq("sl_avatar_key", memberRow.sl_avatar_key)
    ]);

    if (sessionUpdateResult.error) {
      return json(500, {
        success: false,
        error: "session_update_failed",
        details: sessionUpdateResult.error.message,
        hint: sessionUpdateResult.error.hint || null,
        code: sessionUpdateResult.error.code || null
      });
    }

    if (memberUpdateResult.error) {
      return json(500, {
        success: false,
        error: "member_presence_update_failed",
        details: memberUpdateResult.error.message,
        hint: memberUpdateResult.error.hint || null,
        code: memberUpdateResult.error.code || null
      });
    }

    const world = {
      ...overlaidWorld,
      ...buildSltDisplayFields(overlaidWorld),
      report_strip_label: "Celestial Report",
      report_strip_text: buildWorldReportStrip(overlaidWorld, overlaidWorld.tribulation)
    };

    return json(200, {
      success: true,
      world,
      tribulation: world.tribulation,
      viewer: {
        member_id: safeText(memberRow.member_id) || null,
        sl_avatar_key: safeText(memberRow.sl_avatar_key) || null,
        sl_username: safeText(memberRow.sl_username) || null,
        display_name: safeText(memberRow.display_name) || null
      }
    });
  } catch (err) {
    return json(500, {
      success: false,
      error: "server_error",
      details: err?.message || String(err)
    });
  }
};