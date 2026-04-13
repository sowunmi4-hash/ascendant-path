const { createClient } = require("@supabase/supabase-js");
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY);
function json(status, body) { return { statusCode: status, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }; }
function parseBody(event) { try { return event.body ? JSON.parse(event.body) : {}; } catch(e) { return {}; } }

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { success: false, message: "POST required." });
  const { sl_avatar_key, session_id } = parseBody(event);
  if (!sl_avatar_key || !session_id) return json(400, { success: false, message: "sl_avatar_key and session_id required." });
  const { data, error } = await supabase.rpc("override_spiritual_pressure", {
    p_overrider_avatar_key: sl_avatar_key,
    p_session_id: session_id
  });
  if (error) return json(500, { success: false, message: error.message });
  const result = Array.isArray(data) ? data[0] : data;
  return json(result?.success ? 200 : 409, result);
};
