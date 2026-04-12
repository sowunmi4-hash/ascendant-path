// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./clan/load-governance').handler;
