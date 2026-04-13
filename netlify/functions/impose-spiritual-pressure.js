const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

function json(status, body) {
  return { statusCode: status, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

function parseBody(event) {
  try { return event.body ? JSON.parse(event.body) : {}; } catch(e) { return {}; }
}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { success: false, message: "POST required." });

  const body = parseBody(event);
  const { sl_avatar_key, range_type, target_avatar_key, parcel_id, include_bond_partner } = body;

  if (!sl_avatar_key) return json(400, { success: false, message: "sl_avatar_key required." });
  if (!range_type || !["targeted","area"].includes(range_type)) return json(400, { success: false, message: "range_type must be targeted or area." });
  if (range_type === "targeted" && !target_avatar_key) return json(400, { success: false, message: "target_avatar_key required for targeted pressure." });

  const { data, error } = await supabase.rpc("impose_spiritual_pressure", {
    p_imposer_avatar_key:  sl_avatar_key,
    p_range_type:          range_type,
    p_target_avatar_key:   target_avatar_key || null,
    p_parcel_id:           parcel_id || null,
    p_include_bond_partner: Boolean(include_bond_partner)
  });

  if (error) return json(500, { success: false, message: error.message });
  const result = Array.isArray(data) ? data[0] : data;
  if (!result?.success) return json(409, result);
  return json(200, result);
};
