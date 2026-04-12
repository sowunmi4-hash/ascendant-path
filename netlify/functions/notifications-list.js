// notifications-list.js
// Returns unread (and recent read) notifications for the logged-in member.
// Auth: ap_session cookie only — website use only.

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
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  const cookies = parseCookies(event.headers?.cookie || event.headers?.Cookie || "");
  const sessionToken = cookies[COOKIE_NAME] || "";
  if (!sessionToken) return json(401, { error: "Not authenticated" });

  const { data: session } = await supabase.from("website_sessions").select("sl_avatar_key").eq("session_token", sessionToken).eq("is_active", true).maybeSingle();
  if (!session?.sl_avatar_key) return json(401, { error: "Session invalid" });

  const avatarKey = session.sl_avatar_key;

  const { data: notifications, error } = await supabase
    .from("member_notifications")
    .select("id, type, title, message, is_read, metadata, created_at")
    .eq("sl_avatar_key", avatarKey)
    .order("created_at", { ascending: false })
    .limit(50);

  if (error) return json(500, { error: "Failed to load notifications", detail: error.message });

  const unread_count = (notifications || []).filter(n => !n.is_read).length;

  return json(200, { success: true, notifications: notifications || [], unread_count });
};
