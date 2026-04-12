// load-profile-stats.js — returns stats + resources for logged-in member
const { createClient } = require("@supabase/supabase-js");
const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();
const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);

function parseCookies(h) {
  const c = {}; if (!h) return c;
  h.split(";").forEach(p => { const e = p.trim().indexOf("="); if (e < 0) return; const k = p.trim().slice(0,e).trim(); const v = p.trim().slice(e+1).trim(); try { c[k] = decodeURIComponent(v); } catch { c[k] = v; } });
  return c;
}
function json(s, b) { return { statusCode: s, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }, body: JSON.stringify(b) }; }

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") return json(405, { error: "Method not allowed" });

  const cookies = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const { data: session } = await supabase.from("website_sessions").select("sl_avatar_key").eq("session_token", cookies[COOKIE_NAME] || "").eq("is_active", true).maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Not authenticated" });

  const avatarKey = session.sl_avatar_key;

  const [{ data: stats }, { data: member }, { data: wallet }] = await Promise.all([
    supabase.from("cultivator_stats").select("vitality, will, resonance, insight").eq("sl_avatar_key", avatarKey).maybeSingle(),
    supabase.from("cultivation_members").select("auric_current, auric_maximum, vestiges, gender, realm_name, realm_index, v2_active_stage_key, v2_cultivation_status, personal_cultivation_preference").eq("sl_avatar_key", avatarKey).maybeSingle(),
    supabase.from("member_wallets").select("ascension_tokens_balance").eq("sl_avatar_key", avatarKey).maybeSingle()
  ]);

  return json(200, {
    success: true,
    stats:     stats     || { vitality: 0, will: 0, resonance: 0, insight: 0 },
    member:    member    || {},
    tokens:    wallet?.ascension_tokens_balance ?? 0
  });
};
