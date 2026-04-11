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

function parseBody(event) {
  if (!event.body) return {};

  try {
    return JSON.parse(event.body);
  } catch (err) {
    return {};
  }
}

function safeString(value) {
  return String(value ?? "").trim();
}

function getInputFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return safeString(
      event.queryStringParameters?.q ||
      event.queryStringParameters?.query ||
      event.queryStringParameters?.term ||
      event.queryStringParameters?.input ||
      event.queryStringParameters?.question ||
      ""
    );
  }

  return safeString(
    body?.q ||
    body?.query ||
    body?.term ||
    body?.input ||
    body?.question ||
    ""
  );
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

  const body = parseBody(event);
  const input = getInputFromEvent(event, body);

  if (!input) {
    return json(400, {
      success: false,
      error: "Missing glossary input.",
      message: "Provide q, query, term, input, or question."
    });
  }

  const supabase = createClient(url, key, {
    db: { schema: "glossary" },
    auth: { persistSession: false, autoRefreshToken: false }
  });

  try {
    const { data, error } = await supabase.rpc("resolve_entry", {
      p_input: input
    });

    if (error) {
      console.error("resolve_entry error:", error);
      return json(500, {
        success: false,
        error: error.message || "Failed to resolve glossary entry."
      });
    }

    return json(200, {
      success: true,
      input,
      result: data || {
        found: false,
        message: "No official glossary entry was found for that term yet."
      }
    });
  } catch (err) {
    console.error("Unhandled resolve glossary error:", err);
    return json(500, {
      success: false,
      error: err.message || "Unexpected server error."
    });
  }
};