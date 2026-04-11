const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const REFUND_BY_TARGET = {
  primary: 15,
  secondary: 30,
  third: 60,
  root_minor: 15,
  root_major: 30
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

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function getRefundAmount(requestRow) {
  const explicitRefund = Number(requestRow?.refund_if_denied);

  if (Number.isFinite(explicitRefund) && explicitRefund >= 0) {
    return explicitRefund;
  }

  const target = safeLower(requestRow?.request_target);
  return REFUND_BY_TARGET[target] || 0;
}

async function requireSafareehillsAdmin(event) {
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
        error: "admin_not_found"
      })
    };
  }

  const username = safeLower(memberRow.sl_username);

  const { data: adminRow, error: adminError } = await supabase
    .from("library_store_admins")
    .select("*")
    .eq("is_active", true)
    .eq("sl_username", username)
    .maybeSingle();

  if (adminError || !adminRow || username !== "safareehills") {
    return {
      ok: false,
      response: json(403, {
        success: false,
        error: "admin_access_denied"
      })
    };
  }

  return {
    ok: true,
    admin: memberRow
  };
}

async function findPendingRequest(rawRequestId) {
  const textId = safeText(rawRequestId);

  if (!textId) {
    return {
      ok: false,
      response: json(400, {
        success: false,
        error: "invalid_request_id"
      })
    };
  }

  const numericRequestId = Number(textId);

  let requestRow = null;
  let requestError = null;

  if (Number.isFinite(numericRequestId) && numericRequestId > 0) {
    const byRequestId = await supabase
      .schema("elements")
      .from("element_requests")
      .select("*")
      .eq("request_id", numericRequestId)
      .maybeSingle();

    requestRow = byRequestId.data;
    requestError = byRequestId.error;
  }

  if (!requestRow && !requestError) {
    const byId = await supabase
      .schema("elements")
      .from("element_requests")
      .select("*")
      .eq("id", textId)
      .maybeSingle();

    requestRow = byId.data;
    requestError = byId.error;
  }

  if (requestError) {
    return {
      ok: false,
      response: json(500, {
        success: false,
        error: "failed_to_load_request",
        message: requestError.message
      })
    };
  }

  if (!requestRow) {
    return {
      ok: false,
      response: json(404, {
        success: false,
        error: "request_not_found"
      })
    };
  }

  if (safeLower(requestRow.status) !== "pending") {
    return {
      ok: false,
      response: json(409, {
        success: false,
        error: "request_not_pending"
      })
    };
  }

  return {
    ok: true,
    request: requestRow
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
    const auth = await requireSafareehillsAdmin(event);
    if (!auth.ok) return auth.response;

    const body = parseBody(event);
    const rawRequestId = body.request_id || body.id || body.element_request_id;
    const reviewNotes = safeText(body.review_notes);
    const reviewedAt = new Date().toISOString();

    const requestLookup = await findPendingRequest(rawRequestId);
    if (!requestLookup.ok) return requestLookup.response;

    const requestRow = requestLookup.request;

    const { data: walletRow, error: walletError } = await supabase
      .from("member_wallets")
      .select("*")
      .eq("sl_avatar_key", requestRow.sl_avatar_key)
      .maybeSingle();

    if (walletError || !walletRow) {
      return json(404, {
        success: false,
        error: "wallet_not_found"
      });
    }

    const refundAmount = getRefundAmount(requestRow);
    const currentBalance = safeNumber(walletRow.ascension_tokens_balance, 0);
    const refundedBalance = currentBalance + refundAmount;

    const { error: walletUpdateError } = await supabase
      .from("member_wallets")
      .update({
        ascension_tokens_balance: refundedBalance,
        updated_at: reviewedAt
      })
      .eq("sl_avatar_key", requestRow.sl_avatar_key);

    if (walletUpdateError) {
      return json(500, {
        success: false,
        error: "failed_to_refund_tokens",
        message: walletUpdateError.message
      });
    }

    const requestUpdatePayload = {
      status: "denied",
      review_notes: reviewNotes,
      reviewed_by: safeText(auth.admin.sl_username),
      reviewed_at: reviewedAt,
      updated_at: reviewedAt
    };

    if (Object.prototype.hasOwnProperty.call(requestRow, "denied_at")) {
      requestUpdatePayload.denied_at = reviewedAt;
    }

    const requestIdColumn =
      requestRow.request_id != null ? "request_id" : "id";

    const requestIdValue =
      requestRow.request_id != null ? requestRow.request_id : requestRow.id;

    const { data: updatedRequest, error: updateRequestError } = await supabase
      .schema("elements")
      .from("element_requests")
      .update(requestUpdatePayload)
      .eq(requestIdColumn, requestIdValue)
      .select("*")
      .maybeSingle();

    if (updateRequestError) {
      await supabase
        .from("member_wallets")
        .update({
          ascension_tokens_balance: currentBalance,
          updated_at: reviewedAt
        })
        .eq("sl_avatar_key", requestRow.sl_avatar_key);

      return json(500, {
        success: false,
        error: "failed_to_update_request",
        message: updateRequestError.message
      });
    }

    return json(200, {
      success: true,
      message: "Petition denied and refund applied.",
      denied_request: {
        request_id: requestRow.request_id ?? requestRow.id,
        request_target: safeText(requestRow.request_target),
        requested_element: safeText(requestRow.requested_element),
        increase_root: safeText(requestRow.increase_root),
        decrease_root: safeText(requestRow.decrease_root),
        shift_amount: requestRow.shift_amount ?? null,
        reviewed_by: safeText(auth.admin.sl_username),
        reviewed_at: reviewedAt
      },
      wallet: {
        refunded: refundAmount,
        balance_after: refundedBalance
      },
      request: updatedRequest
    });
  } catch (error) {
    return json(500, {
      success: false,
      error: "server_error",
      message: error.message
    });
  }
};