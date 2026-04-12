// =====================================================
// script-config.js
// The Ascendant Path — LSL Script Auto-Update Endpoint
//
// To push a config update to all in-world HUDs:
//   1. Increment CURRENT_VERSION
//   2. Add/change values in CURRENT_CONFIG
//   3. Deploy — every HUD updates itself on next 10s load-resonance poll
// =====================================================

const CURRENT_VERSION = 2;

const CURRENT_CONFIG = {
  // Cultivation resource field names returned by Netlify endpoints
  auric_current:    "auric_current",
  auric_maximum:    "auric_maximum",
  vestiges:         "vestiges",
  vestiges_maximum: "vestiges_maximum",
  vestiges_capped:  "vestiges_capped",

  // Bond / resonance fields
  bond_runtime_active: "bond_runtime_active",
  bond_session_status: "bond_session_status",
  bond_volume_number:  "bond_volume_number",
  bond_book_number:    "bond_book_number",

  // Crystal / weather fields
  repair_auric_cost: "repair_auric_cost",
};

function buildScriptUpdate(clientVersion) {
  const v = parseInt(clientVersion, 10);
  if (!isNaN(v) && v >= CURRENT_VERSION) {
    return { upToDate: true };
  }
  return {
    upToDate: false,
    version:  CURRENT_VERSION,
    config:   CURRENT_CONFIG,
  };
}

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch (_) {}

  const scriptUpdate = buildScriptUpdate(body.version ?? body.script_version);

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(scriptUpdate),
  };
};

exports.buildScriptUpdate = buildScriptUpdate;
exports.CURRENT_VERSION   = CURRENT_VERSION;
