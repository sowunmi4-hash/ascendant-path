// soul-sense-log.js — returns usage history for logged-in member
const { createClient } = require("@supabase/supabase-js");
const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();
const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

function parseCookies(h) {
  const c={}; if(!h) return c;
  h.split(";").forEach(p=>{const e=p.trim().indexOf("=");if(e<0)return;const k=p.trim().slice(0,e).trim();const v=p.trim().slice(e+1).trim();try{c[k]=decodeURIComponent(v);}catch{c[k]=v;}});
  return c;
}
function json(s,b){return{statusCode:s,headers:{"Content-Type":"application/json","Cache-Control":"no-store"},body:JSON.stringify(b)};}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") return json(405, { error: "Method not allowed" });
  const cookies = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const { data: session } = await supabase.from("website_sessions").select("sl_avatar_key")
    .eq("session_token", cookies[COOKIE_NAME]||"").eq("is_active", true).maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Not authenticated" });

  const avatarKey = session.sl_avatar_key;

  const { data: logs } = await supabase.from("soul_sense_log")
    .select("realm_index, auric_cost, auric_before, auric_after, cultivators_seen, created_at")
    .eq("sl_avatar_key", avatarKey)
    .order("created_at", { ascending: false })
    .limit(5);

  const total_uses  = (logs || []).length;
  const total_auric = (logs || []).reduce((sum, l) => sum + (l.auric_cost || 0), 0);

  return json(200, { success: true, logs: logs || [], total_uses, total_auric });
};
