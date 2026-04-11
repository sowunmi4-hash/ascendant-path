const { createClient } = require("@supabase/supabase-js");

const celestialSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "celestial" }
  }
);

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

exports.handler = async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "GET") {
    return buildResponse(405, {
      success: false,
      error: "Method not allowed"
    });
  }

  try {
    const { data, error } = await celestialSupabase.rpc(
      "load_seven_day_celestial_forecast_compact"
    );

    if (error) {
      console.error("Forecast RPC error:", error);
      return buildResponse(500, {
        success: false,
        error: "Failed to load seven-day celestial forecast",
        details: error.message || String(error)
      });
    }

    if (!data || !Array.isArray(data.days)) {
      return buildResponse(500, {
        success: false,
        error: "Forecast payload was empty or invalid"
      });
    }

    return buildResponse(200, {
      success: true,
      forecast: data
    });
  } catch (err) {
    console.error("Unhandled forecast loader error:", err);
    return buildResponse(500, {
      success: false,
      error: "Unexpected server error while loading forecast",
      details: err.message || String(err)
    });
  }
};