// load-elements-state.js
// Returns caller's spiritual traits and root balances flattened onto a single
// member object matching the shape elements.html expects.
// Delegates to public.get_member_elements_state RPC to cross schema boundaries.
// v4 — 2026-04-09
const { createClient } = require("@supabase/supabase-js");
const SUPABASE_URL        = process.env.SUPABASE_URL        || "";
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";
const COOKIE_NAME         = (process.env.SESSION_COOKIE_NAME || "ap_session").trim();
const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY);
function parseCookies(header) {
  const cookies = {};
  if (!header) return cookies;
  header.split(";").forEach((part) => {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    try { cookies[key] = decodeURIComponent(val); } catch { cookies[key] = val; }
  });
  return cookies;
}
function json(status, body) {
  return {
    statusCode: status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    body: JSON.stringify(body),
  };
}
exports.handler = async (event) => {
  if (event.httpMethod !== "GET" && event.httpMethod !== "POST") {
    return json(405, { success: false, message: "Method not allowed." });
  }
  // ── Auth ──────────────────────────────────────────────────────
  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return json(401, { success: false, authenticated: false, message: "No active session." });
  const { data: session } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();
  if (!session?.sl_avatar_key) {
    return json(401, { success: false, authenticated: false, message: "Session not found or inactive." });
  }
  const avatarKey = session.sl_avatar_key;
  // ── Wallet fetch ──────────────────────────────────────────────
  const { data: walletRow } = await supabase
    .from("member_wallets")
    .select("ascension_tokens_balance, total_tokens_credited, total_tokens_spent")
    .eq("sl_avatar_key", avatarKey)
    .maybeSingle();
  // ── Load all elements data via RPC ────────────────────────────
  const { data: result, error } = await supabase
    .rpc("get_member_elements_state", { p_avatar_key: avatarKey });
  if (error) {
    console.error("[load-elements-state] RPC error:", error);
    return json(500, { success: false, message: "Failed to load elements state." });
  }
  if (!result?.success) {
    return json(404, { success: false, message: result?.error_code || "Member not found." });
  }
  const cm = result.member;
  const t  = result.traits;
  const rb = result.root_balances;
  const pd = result.path_deltas || { wood: 0, fire: 0, earth: 0, metal: 0, water: 0 };
  const requests = result.requests || [];
  const pendingRequest = requests.find(r => r.status === "pending") || null;
  // ── Unlock flags ──────────────────────────────────────────────
  const primaryUnlocked      = t?.has_spirit_analysis        || false;
  const secondaryUnlocked    = t?.secondary_element_unlocked || false;
  const thirdUnlocked = t?.third_element_unlocked || false;
  const primaryStatus   = primaryUnlocked   ? "active"  : "sealed";
  const secondaryStatus = secondaryUnlocked ? "active"  : "sealed";
  const thirdStatus     = thirdUnlocked     ? "unlocked" : "sealed";
  // ── Root maps ─────────────────────────────────────────────────
  const spiritRootMap = t ? {
    wood:  t.natural_wood_root,
    fire:  t.natural_fire_root,
    earth: t.natural_earth_root,
    metal: t.natural_metal_root,
    water: t.natural_water_root,
  } : null;
  const baseRootMap = rb ? {
    wood:  rb.base_wood,
    fire:  rb.base_fire,
    earth: rb.base_earth,
    metal: rb.base_metal,
    water: rb.base_water,
  } : null;
  const effectiveRootMap = rb ? {
    wood:  rb.final_wood,
    fire:  rb.final_fire,
    earth: rb.final_earth,
    metal: rb.final_metal,
    water: rb.final_water,
  } : null;
  const modifierBreakdown = rb ? {
    primary: {
      wood:  rb.primary_wood_delta,
      fire:  rb.primary_fire_delta,
      earth: rb.primary_earth_delta,
      metal: rb.primary_metal_delta,
      water: rb.primary_water_delta,
    },
    secondary: {
      wood:  rb.secondary_wood_delta,
      fire:  rb.secondary_fire_delta,
      earth: rb.secondary_earth_delta,
      metal: rb.secondary_metal_delta,
      water: rb.secondary_water_delta,
    },
    third: {
      wood:  rb.third_wood_delta  || 0,
      fire:  rb.third_fire_delta  || 0,
      earth: rb.third_earth_delta || 0,
      metal: rb.third_metal_delta || 0,
      water: rb.third_water_delta || 0,
    },
    path: {
      wood:  pd.wood,
      fire:  pd.fire,
      earth: pd.earth,
      metal: pd.metal,
      water: pd.water,
    },
  } : null;
  const rootBarMax = effectiveRootMap
    ? Math.max(...Object.values(effectiveRootMap), 1)
    : 100;
  // ── Flat member object ────────────────────────────────────────
  const member = {
    sl_username:        cm.sl_username,
    character_name:     cm?.character_name || cm.sl_username,
    realm_index:        cm.realm_index,
    realm_name:         cm.realm_name,
    realm_display_name: cm.realm_display_name,
    mortal_energy: t?.mortal_energy_snapshot || 0,
    primary_element:   t?.natural_primary_element   || null,
    secondary_element: t?.natural_secondary_element || null,
    third_element:     t?.natural_third_element     || null,
    current_primary_element:   t?.current_primary_element   || null,
    current_secondary_element: t?.current_secondary_element || null,
    current_third_element:     t?.current_third_element     || null,
    path_element:         t?.current_path_element || null,
    current_path_element: t?.current_path_element || null,
    current_path_label:   t?.current_path_element || null,
    primary_element_unlocked:   primaryUnlocked,
    secondary_element_unlocked: secondaryUnlocked,
    third_element_unlocked:     thirdUnlocked,
    primary_status:   primaryStatus,
    secondary_status: secondaryStatus,
    third_status:     thirdStatus,
    spirit_root_profile:             t?.current_spirit_root_profile     || null,
    dominant_current_root_element:   t?.dominant_current_root_element   || null,
    supporting_current_root_element: t?.supporting_current_root_element || null,
    spirit_root_map:    spiritRootMap,
    base_root_map:      baseRootMap,
    effective_root_map: effectiveRootMap,
    modifier_breakdown: modifierBreakdown,
    root_balance_meta: rb ? {
      source:             "member_root_balances",
      last_rebalanced_at: rb.last_rebalanced_at,
    } : null,
    root_bar_max: rootBarMax,
    has_spirit_analysis: t?.has_spirit_analysis || false,
    is_element_admin:    cm.sl_username === "safareehills",
  };
  const { data: pricingRows } = await supabase
    .schema("elements")
    .from("petition_pricing")
    .select("request_target, display_name, total_cost, submission_cost, attunement_cost, refund_if_denied, description, cascade_note")
    .eq("is_active", true)
    .order("total_cost", { ascending: true });

  const petition_pricing = {};
  for (const row of (pricingRows || [])) {
    petition_pricing[row.request_target] = row;
  }

  return json(200, {
    success:         true,
    authenticated:   true,
    member,
    requests,
    pending_request: pendingRequest,
    petition_pricing,
    wallet: {
      currency_name:            "Ascension Tokens",
      wallet_found:             !!walletRow,
      ascension_tokens_balance: walletRow?.ascension_tokens_balance ?? 0,
      total_tokens_credited:    walletRow?.total_tokens_credited    ?? 0,
      total_tokens_spent:       walletRow?.total_tokens_spent       ?? 0,
    },
  });
};
