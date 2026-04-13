const { createClient } = require("@supabase/supabase-js");
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SECRET_KEY);
function json(s,b){return{statusCode:s,headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}}
function parseBody(e){try{return e.body?JSON.parse(e.body):{}}catch{return{}}}

exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { success: false, message: "POST required." });
  const body = parseBody(event);
  const { sl_avatar_key, clan_name, clan_key, parent_clan_key, motto, summary, primary_color, secondary_color } = body;
  if (!sl_avatar_key || !clan_name || !clan_key || !parent_clan_key)
    return json(400, { success: false, message: "sl_avatar_key, clan_name, clan_key and parent_clan_key required." });
  const { data, error } = await supabase.rpc("found_minor_clan", {
    p_founder_avatar_key: sl_avatar_key,
    p_clan_name:          clan_name,
    p_clan_key:           clan_key,
    p_parent_clan_key:    parent_clan_key,
    p_motto:              motto || null,
    p_summary:            summary || null,
    p_primary_color:      primary_color || "#c8a96a",
    p_secondary_color:    secondary_color || "#07070b"
  });
  if (error) return json(500, { success: false, message: error.message });
  const result = Array.isArray(data) ? data[0] : data;
  return json(result?.success ? 200 : 409, result);
};
