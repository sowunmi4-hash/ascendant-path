const { createClient } = require("@supabase/supabase-js");

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
};

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
}

function getSupabaseEnv() {
  const url =
    process.env.SUPABASE_URL ||
    process.env.VITE_SUPABASE_URL ||
    process.env.NEXT_PUBLIC_SUPABASE_URL ||
    "";

  const key =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY ||
    process.env.VITE_SUPABASE_ANON_KEY ||
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    "";

  return { url, key };
}

exports.handler = async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders,
      body: ""
    };
  }

  if (!["GET", "POST"].includes(event.httpMethod)) {
    return json(405, {
      success: false,
      error: "Method not allowed."
    });
  }

  const { url, key } = getSupabaseEnv();

  if (!url || !key) {
    return json(500, {
      success: false,
      error: "Missing Supabase environment variables.",
      expected: {
        url: ["SUPABASE_URL", "VITE_SUPABASE_URL", "NEXT_PUBLIC_SUPABASE_URL"],
        key: [
          "SUPABASE_SERVICE_ROLE_KEY",
          "SUPABASE_SECRET_KEY",
          "SUPABASE_ANON_KEY",
          "VITE_SUPABASE_ANON_KEY",
          "NEXT_PUBLIC_SUPABASE_ANON_KEY"
        ]
      }
    });
  }

  const supabase = createClient(url, key, {
    db: { schema: "glossary" },
    auth: { persistSession: false, autoRefreshToken: false }
  });

  try {
    const { data, error } = await supabase.rpc("load_active_entries_json");

    if (error) {
      console.error("load_active_entries_json error:", error);
      return json(500, {
        success: false,
        error: error.message || "Failed to load glossary entries."
      });
    }

    return json(200, {
      success: true,
      glossary: data || { entries: [] }
    });
  } catch (err) {
    console.error("Unhandled load glossary error:", err);

    return json(500, {
      success: false,
      error: err.message || "Unexpected server error."
    });
  }
};