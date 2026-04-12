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

function generateSixDigitPin() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "POST") {
    return buildResponse(405, {
      error: "method_not_allowed",
      message: "Method not allowed. Use POST."
    });
  }

  try {
    const body = JSON.parse(event.body || "{}");

    const sl_avatar_key = (body.sl_avatar_key || "").trim();
    const sl_username = (body.sl_username || "").trim();
    const display_name = (body.display_name || "").trim();

    if (!sl_avatar_key || !sl_username) {
      return buildResponse(400, {
        error: "missing_fields",
        message: "sl_avatar_key and sl_username are required."
      });
    }

    // 1) Deactivate old active pins for this avatar
    const { error: deactivateError } = await supabase
      .from("hud_login_pins")
      .update({
        pin_active: false
      })
      .eq("sl_avatar_key", sl_avatar_key)
      .eq("pin_active", true);

    if (deactivateError) {
      console.error("Deactivate old pins error:", deactivateError);
      return buildResponse(500, {
        error: "pin_cleanup_failed",
        message: "Could not deactivate old login pins."
      });
    }

    // 2) Generate and insert new PIN
    let login_pin = "";
    let insertError = null;
    let insertedRow = null;

    for (let attempt = 0; attempt < 5; attempt++) {
      login_pin = generateSixDigitPin();

      const { data, error } = await supabase
        .from("hud_login_pins")
        .insert([
          {
            sl_avatar_key,
            sl_username,
            display_name: display_name || sl_username,
            login_pin,
            pin_active: true
          }
        ])
        .select()
        .single();

      if (!error) {
        insertedRow = data;
        insertError = null;
        break;
      }

      insertError = error;
    }

    if (insertError) {
      console.error("Supabase insert error:", insertError);
      return buildResponse(500, {
        error: "pin_create_failed",
        message: "Could not generate login pin."
      });
    }

    return buildResponse(200, {
      success: true,
      message: "Login pin generated successfully.",
      login_pin: insertedRow.login_pin,
      sl_avatar_key: insertedRow.sl_avatar_key,
      sl_username: insertedRow.sl_username,
      display_name: insertedRow.display_name,
      created_at: insertedRow.created_at
    });
  } catch (err) {
    console.error("generate-login-pin error:", err);

    return buildResponse(500, {
      error: "server_error",
      message: "An unexpected error occurred."
    });
  }
};