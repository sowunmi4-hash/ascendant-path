const { createClient } = require("@supabase/supabase-js");
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY);
function json(s,b){return{statusCode:s,headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}}
function parseBody(e){try{return e.body?JSON.parse(e.body):{}}catch{return{}}}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { success: false, message: "POST required." });
  const { sl_avatar_key } = parseBody(event);
  if (!sl_avatar_key) return json(400, { success: false, message: "sl_avatar_key required." });
  const { data, error } = await supabase.rpc("leave_clan", { p_avatar_key: sl_avatar_key });
  if (error) return json(500, { success: false, message: error.message });
  const result = Array.isArray(data) ? data[0] : data;
  return json(result?.success ? 200 : 409, result);
};
