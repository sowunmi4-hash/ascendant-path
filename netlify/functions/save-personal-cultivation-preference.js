// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./cultivation/save-preference').handler;
