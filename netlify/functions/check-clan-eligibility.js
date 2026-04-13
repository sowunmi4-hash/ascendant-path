const { createClient } = require("@supabase/supabase-js");
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY);
function json(s,b){return{statusCode:s,headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  const sl_avatar_key = event.queryStringParameters?.sl_avatar_key || (event.body ? JSON.parse(event.body).sl_avatar_key : null);
  if (!sl_avatar_key) return json(400, { success: false, message: "sl_avatar_key required." });
  const { data, error } = await supabase.rpc("check_clan_founding_eligibility", { p_avatar_key: sl_avatar_key });
  if (error) return json(500, { success: false, message: error.message });
  const result = Array.isArray(data) ? data[0] : data;
  return json(200, result);
};
