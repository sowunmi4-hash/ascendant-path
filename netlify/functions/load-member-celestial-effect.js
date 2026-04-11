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

function buildMemberReportStrip(row) {
  if (!row) return "Celestial Report — Your celestial effect is unreadable.";

  const parts = [];

  if (row.phase_name) parts.push(row.phase_name);
  if (row.force_name) parts.push(row.force_name);
  if (row.has_active_phenomenon && row.phenomenon_name) parts.push(row.phenomenon_name);
  if (row.dashboard_effect_label) parts.push(row.dashboard_effect_label);
  if (row.dashboard_effect_summary) parts.push(row.dashboard_effect_summary);

  return `Celestial Report — ${parts.join(" • ")}`;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
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

    const memberId = safeText(memberRow.member_id);

    if (!memberId) {
      return json(404, {
        success: false,
        error: "member_not_found"
      });
    }

    const { data: celestialData, error: celestialError } =
      await celestialSupabase.rpc("load_member_celestial_effect", {
        p_member_id: memberId
      });

    if (celestialError) {
      return json(500, {
        success: false,
        error: "celestial_rpc_failed",
        details: celestialError.message,
        hint: celestialError.hint || null,
        code: celestialError.code || null
      });
    }

    const effectRow = Array.isArray(celestialData)
      ? celestialData[0] || null
      : celestialData || null;

    if (!effectRow) {
      return json(404, {
        success: false,
        error: "celestial_effect_not_found"
      });
    }

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

    return json(200, {
      success: true,
      effect: {
        ...effectRow,
        report_strip_label: "Celestial Report",
        report_strip_text: buildMemberReportStrip(effectRow)
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