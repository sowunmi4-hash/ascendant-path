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

function safeInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function getActionFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return (
      safeString(
        event.queryStringParameters?.action ||
          event.queryStringParameters?.mode ||
          event.queryStringParameters?.type
      ).toLowerCase() || "resolve"
    );
  }

  return (
    safeString(body?.action || body?.mode || body?.type).toLowerCase() ||
    "resolve"
  );
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
    body?.q || body?.query || body?.term || body?.input || body?.question || ""
  );
}

function getAvatarKeyFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return safeString(
      event.queryStringParameters?.avatarKey ||
        event.queryStringParameters?.member_avatar_key ||
        event.queryStringParameters?.sl_avatar_key ||
        ""
    );
  }

  return safeString(
    body?.avatarKey || body?.member_avatar_key || body?.sl_avatar_key || ""
  );
}

function getUsernameFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return safeString(
      event.queryStringParameters?.username ||
        event.queryStringParameters?.sl_username ||
        ""
    );
  }

  return safeString(body?.username || body?.sl_username || "");
}

function getCategoryFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return safeString(
      event.queryStringParameters?.category ||
        event.queryStringParameters?.category_key ||
        event.queryStringParameters?.categoryKey ||
        ""
    );
  }

  return safeString(
    body?.category || body?.category_key || body?.categoryKey || ""
  );
}

function getQuestionKeyFromEvent(event, body) {
  if (event.httpMethod === "GET") {
    return safeString(
      event.queryStringParameters?.question_key ||
        event.queryStringParameters?.questionKey ||
        event.queryStringParameters?.key ||
        ""
    );
  }

  return safeString(body?.question_key || body?.questionKey || body?.key || "");
}

function getLimitFromEvent(event, body, fallback = 8, min = 1, max = 100) {
  const raw =
    event.httpMethod === "GET"
      ? event.queryStringParameters?.limit
      : body?.limit;

  return clamp(safeInteger(raw, fallback), min, max);
}

function getPerCategoryFromEvent(event, body, fallback = 2, min = 1, max = 10) {
  const raw =
    event.httpMethod === "GET"
      ? event.queryStringParameters?.perCategory ||
        event.queryStringParameters?.per_category
      : body?.perCategory || body?.per_category;

  return clamp(safeInteger(raw, fallback), min, max);
}

function getRuntimeState(body) {
  if (!body || typeof body !== "object") return null;

  const runtime =
    body.runtimeState && typeof body.runtimeState === "object"
      ? { ...body.runtimeState }
      : {};

  const category = safeString(body.category || body.category_key || body.mode);
  const categoryLabel = safeString(
    body.category_label || body.categoryTitle || body.category_title
  );

  if (category) {
    runtime.selected_category = category;
  }

  if (categoryLabel) {
    runtime.selected_category_title = categoryLabel;
  }

  return Object.keys(runtime).length ? runtime : null;
}

function normalizeResolveResult(data) {
  const result =
    data && typeof data === "object"
      ? data
      : {
          mode: "fallback",
          title: "Oracle Response",
          answer: "The Oracle could not produce a response right now."
        };

  return {
    mode: safeString(result.mode) || "fallback",
    intent_key: safeString(result.intent_key) || "",
    title:
      safeString(result.title) ||
      safeString(result.headline) ||
      "Oracle Response",
    answer:
      safeString(result.answer) ||
      safeString(result.full_explanation) ||
      safeString(result.message) ||
      "The Oracle could not produce a response right now.",
    full_explanation:
      safeString(result.full_explanation) ||
      safeString(result.answer) ||
      safeString(result.message) ||
      "",
    short_answer:
      safeString(result.short_answer) ||
      safeString(result.summary) ||
      safeString(result.answer) ||
      "",
    summary:
      safeString(result.summary) ||
      safeString(result.short_answer) ||
      "",
    message:
      safeString(result.message) ||
      safeString(result.answer) ||
      "",
    headline:
      safeString(result.headline) ||
      safeString(result.title) ||
      "Oracle Response",
    glossary:
      result.glossary && typeof result.glossary === "object"
        ? result.glossary
        : null,
    context:
      result.context && typeof result.context === "object"
        ? result.context
        : null,
    capture:
      result.capture && typeof result.capture === "object"
        ? result.capture
        : null,
    raw: result
  };
}

async function callRpc(supabase, name, params = {}) {
  const { data, error } = await supabase.rpc(name, params);

  if (error) {
    throw new Error(error.message || `RPC failed: ${name}`);
  }

  return data;
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
  const action = getActionFromEvent(event, body);
  const input = getInputFromEvent(event, body);
  const avatarKey = getAvatarKeyFromEvent(event, body);
  const username = getUsernameFromEvent(event, body);
  const category = getCategoryFromEvent(event, body);
  const questionKey = getQuestionKeyFromEvent(event, body);
  const limit = getLimitFromEvent(event, body);
  const perCategory = getPerCategoryFromEvent(event, body);
  const runtimeState = getRuntimeState(body);

  const supabase = createClient(url, key, {
    db: { schema: "oracle" },
    auth: { persistSession: false, autoRefreshToken: false }
  });

  try {
    if (action === "resolve") {
      if (!avatarKey) {
        return json(400, {
          success: false,
          error: "Missing avatar key.",
          message: "Provide avatarKey, member_avatar_key, or sl_avatar_key."
        });
      }

      if (!input) {
        return json(400, {
          success: false,
          error: "Missing Oracle input.",
          message: "Provide q, query, term, input, or question."
        });
      }

      if (runtimeState) {
        try {
          await callRpc(supabase, "upsert_member_runtime_state", {
            p_member_avatar_key: avatarKey,
            p_member_username: username || null,
            p_runtime: runtimeState
          });
        } catch (runtimeErr) {
          console.error("upsert_member_runtime_state error:", runtimeErr);
        }
      }

      const data = await callRpc(supabase, "resolve_oracle_query_with_capture", {
        p_member_avatar_key: avatarKey,
        p_input: input,
        p_member_username: username || null
      });

      return json(200, {
        success: true,
        action: "resolve",
        input,
        avatarKey,
        username: username || null,
        result: normalizeResolveResult(data)
      });
    }

    if (action === "featured") {
      const data = await callRpc(supabase, "load_featured_faq_mix", {
        p_per_category: perCategory
      });

      return json(200, {
        success: true,
        action: "featured",
        per_category: perCategory,
        result: data
      });
    }

    if (action === "all") {
      const data = await callRpc(supabase, "load_faq_questions");

      return json(200, {
        success: true,
        action: "all",
        result: data
      });
    }

    if (action === "grouped") {
      const data = await callRpc(supabase, "load_grouped_faq_questions");

      return json(200, {
        success: true,
        action: "grouped",
        result: data
      });
    }

    if (action === "match") {
      if (!input) {
        return json(400, {
          success: false,
          error: "Missing match input.",
          message: "Provide q, query, term, input, or question."
        });
      }

      const data = await callRpc(supabase, "match_faq_questions", {
        p_input: input,
        p_limit: limit
      });

      return json(200, {
        success: true,
        action: "match",
        input,
        limit,
        result: data
      });
    }

    if (action === "category") {
      const data = await callRpc(supabase, "load_faq_questions_by_category", {
        p_category: category || null,
        p_limit: limit
      });

      return json(200, {
        success: true,
        action: "category",
        category: category || null,
        limit,
        result: data
      });
    }

    if (action === "related") {
      if (!questionKey) {
        return json(400, {
          success: false,
          error: "Missing question key.",
          message: "Provide question_key, questionKey, or key."
        });
      }

      const data = await callRpc(supabase, "load_related_faq_questions", {
        p_question_key: questionKey,
        p_limit: limit
      });

      return json(200, {
        success: true,
        action: "related",
        question_key: questionKey,
        limit,
        result: data
      });
    }

    if (action === "random") {
      const data = await callRpc(supabase, "load_random_faq_questions", {
        p_limit: limit,
        p_category: category || null
      });

      return json(200, {
        success: true,
        action: "random",
        category: category || null,
        limit,
        result: data
      });
    }

    return json(400, {
      success: false,
      error: "Unknown action.",
      allowed_actions: [
        "resolve",
        "featured",
        "all",
        "grouped",
        "match",
        "category",
        "related",
        "random"
      ]
    });
  } catch (err) {
    console.error("Oracle endpoint error:", err);
    return json(500, {
      success: false,
      error: err.message || "Unexpected server error."
    });
  }
};