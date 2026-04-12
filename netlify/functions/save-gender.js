// save-gender.js — saves gender (male/female) for logged-in member
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
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });
  let body = {}; try { body = JSON.parse(event.body || "{}"); } catch {}

  const cookies = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const { data: session } = await supabase.from("website_sessions").select("sl_avatar_key").eq("session_token", cookies[COOKIE_NAME] || "").eq("is_active", true).maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Not authenticated" });

  const gender = (body.gender || "").toLowerCase();
  if (!["male", "female"].includes(gender)) return json(400, { error: "Invalid gender" });

  const { error } = await supabase.from("cultivation_members").update({ gender }).eq("sl_avatar_key", session.sl_avatar_key);
  if (error) return json(500, { error: "Failed to save gender", detail: error.message });

  return json(200, { success: true, gender });
};
