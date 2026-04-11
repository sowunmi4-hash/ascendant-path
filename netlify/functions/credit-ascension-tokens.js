const { createClient } = require("@supabase/supabase-js");

const storeSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  { db: { schema: "store" } }
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

function safeText(value) {
  return String(value || "").trim();
}

function safeLower(value) {
  return safeText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function jsonToObject(value) {
  if (!value) return null;
  if (typeof value === "object") return value;

  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function coerceRpcPayload(data) {
  let payload = data;

  if (Array.isArray(payload)) {
    payload = payload.length ? payload[0] : null;
  }

  if (!payload) return null;

  if (typeof payload === "string") {
    payload = jsonToObject(payload);
  }

  if (!payload || typeof payload !== "object") {
    return null;
  }

  // Common wrappers depending on SQL return style
  if (payload.process_token_purchase !== undefined) {
    payload = payload.process_token_purchase;
  } else if (payload.result !== undefined) {
    payload = payload.result;
  } else if (payload.data !== undefined && payload.success === undefined) {
    payload = payload.data;
  }

  if (typeof payload === "string") {
    payload = jsonToObject(payload);
  }

  return payload && typeof payload === "object" ? payload : null;
}

function pick(obj, keys, fallback = null) {
  if (!obj || typeof obj !== "object") return fallback;

  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) {
      return obj[key];
    }
  }

  return fallback;
}

function pickNumber(obj, keys, fallback = 0) {
  return safeNumber(pick(obj, keys, fallback), fallback);
}

function pickText(obj, keys, fallback = "") {
  return safeText(pick(obj, keys, fallback));
}

function pickBool(obj, keys, fallback = false) {
  const value = pick(obj, keys, fallback);

  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;

  const lowered = safeLower(value);
  if (lowered === "true" || lowered === "1" || lowered === "yes") return true;
  if (lowered === "false" || lowered === "0" || lowered === "no") return false;

  return Boolean(value);
}

function resolveFailureStatus(payload, rpcError) {
  if (rpcError) {
    const message = safeLower(rpcError.message);

    if (
      message.includes("duplicate") ||
      message.includes("already processed") ||
      message.includes("already exists")
    ) {
      return 409;
    }

    if (
      message.includes("not found") ||
      message.includes("member")
    ) {
      return 404;
    }

    if (
      message.includes("machine") &&
      (message.includes("invalid") || message.includes("inactive") || message.includes("not registered"))
    ) {
      return 403;
    }

    if (
      message.includes("invalid") ||
      message.includes("amount") ||
      message.includes("pack")
    ) {
      return 409;
    }

    return 500;
  }

  const errorCode = safeLower(pick(payload, ["error_code", "code", "status_code_key"], ""));
  const message = safeLower(pick(payload, ["message", "error", "status_text"], ""));

  if (
    errorCode.includes("duplicate") ||
    message.includes("already processed") ||
    message.includes("duplicate")
  ) {
    return 409;
  }

  if (
    errorCode.includes("member_not_found") ||
    message.includes("member not found") ||
    message.includes("no cultivation member")
  ) {
    return 404;
  }

  if (
    errorCode.includes("machine") ||
    message.includes("machine not registered") ||
    message.includes("machine inactive") ||
    message.includes("invalid machine")
  ) {
    return 403;
  }

  if (
    errorCode.includes("invalid_amount") ||
    errorCode.includes("invalid_pack") ||
    message.includes("invalid amount") ||
    message.includes("valid token pack")
  ) {
    return 409;
  }

  return 400;
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
    if (
      !process.env.SUPABASE_URL ||
      !process.env.SUPABASE_SECRET_KEY ||
      !process.env.ASCENSION_MACHINE_SECRET
    ) {
      return buildResponse(500, {
        success: false,
        message: "Missing required environment variables."
      });
    }

    const body = parseBody(event);

    const machine_secret = safeText(body.machine_secret);
    const sl_avatar_key = safeText(body.sl_avatar_key);
    const sl_username = safeLower(body.sl_username);
    const linden_amount = safeNumber(body.linden_amount, 0);
    const reference_code = safeText(body.reference_code);
    const source_object_key = safeText(body.source_object_key);
    const source_object_name = safeText(body.source_object_name);

    if (!machine_secret) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: machine_secret."
      });
    }

    if (machine_secret !== safeText(process.env.ASCENSION_MACHINE_SECRET)) {
      return buildResponse(403, {
        success: false,
        message: "Invalid machine authorization."
      });
    }

    if (!sl_avatar_key) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: sl_avatar_key."
      });
    }

    if (linden_amount <= 0) {
      return buildResponse(400, {
        success: false,
        message: "linden_amount must be greater than 0."
      });
    }

    if (!reference_code) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: reference_code."
      });
    }

    if (!source_object_key) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: source_object_key."
      });
    }

    // --------------------------------------------------
    // Single database-owned purchase call
    // --------------------------------------------------
    // Assumes Claude used the argument names exactly as summarized:
    //   avatar_key, username, linden_amount, machine_key, reference_code
    // If Claude used p_ prefixes instead, only change the 5 keys below.
    const { data: rpcData, error: rpcError } = await storeSupabase.rpc("process_token_purchase", {
      avatar_key: sl_avatar_key,
      username: sl_username || null,
      linden_amount,
      machine_key: source_object_key,
      reference_code
    });

    if (rpcError) {
      return buildResponse(resolveFailureStatus(null, rpcError), {
        success: false,
        message: "Failed to process token purchase.",
        error: rpcError.message,
        reference_code,
        source_object_key,
        source_object_name: source_object_name || null
      });
    }

    const payload = coerceRpcPayload(rpcData);

    if (!payload) {
      return buildResponse(500, {
        success: false,
        message: "Purchase RPC returned no usable payload.",
        reference_code,
        source_object_key,
        source_object_name: source_object_name || null
      });
    }

    const success = pickBool(payload, ["success", "ok"], false);
    const message = pickText(
      payload,
      ["message", "status_text", "result_message"],
      success ? "Ascension Tokens credited successfully." : "Purchase failed."
    );

    if (!success) {
      return buildResponse(resolveFailureStatus(payload, null), {
        success: false,
        message,
        error_code: pickText(payload, ["error_code", "code"], "") || null,
        reference_code,
        source_object_key,
        source_object_name: source_object_name || null,
        details: payload
      });
    }

    const baseTokens = pickNumber(payload, ["base_tokens", "base_token_amount"], 0);
    const bonusTokens = pickNumber(payload, ["bonus_tokens", "bonus_token_amount"], 0);
    const finalTokens = pickNumber(
      payload,
      ["final_tokens", "token_amount", "credited_tokens"],
      baseTokens + bonusTokens
    );

    const balanceBefore = pickNumber(payload, ["balance_before"], 0);
    const balanceAfter = pickNumber(
      payload,
      ["balance_after", "new_balance", "ascension_tokens_balance"],
      0
    );

    return buildResponse(200, {
      success: true,
      message,
      user: {
        sl_avatar_key,
        sl_username: pickText(payload, ["sl_username", "username"], sl_username) || null,
        display_name: pickText(payload, ["display_name", "member_display_name"], "") || null,
        current_path: pickText(payload, ["current_path", "path"], "") || null
      },
      payment: {
        linden_amount,
        base_token_amount: baseTokens,
        bonus_token_amount: bonusTokens,
        token_amount: finalTokens,
        reference_code,
        source_object_name: source_object_name || null,
        source_object_key,
        celestial_event: pickText(
          payload,
          ["celestial_event_name", "event_name", "celestial_event"],
          ""
        ) || null,
        bonus_reason: pickText(
          payload,
          ["bonus_reason", "event_bonus_reason", "path_bonus_reason"],
          ""
        ) || null
      },
      wallet: {
        currency_name: "Ascension Tokens",
        balance_before: balanceBefore,
        balance_after: balanceAfter,
        ascension_tokens_balance: balanceAfter,
        total_tokens_credited: pickNumber(payload, ["total_tokens_credited"], 0),
        total_tokens_spent: pickNumber(payload, ["total_tokens_spent"], 0),
        updated_at: pickText(payload, ["updated_at", "wallet_updated_at", "processed_at"], "") || null
      },
      store: {
        pack_key: pickText(payload, ["pack_key"], "") || null,
        machine_key: pickText(payload, ["machine_key"], source_object_key) || null,
        machine_name: pickText(payload, ["machine_name", "display_name"], source_object_name) || null
      }
    });
  } catch (error) {
    return buildResponse(500, {
      success: false,
      message: "Unexpected error while processing token purchase.",
      error: error.message
    });
  }
};