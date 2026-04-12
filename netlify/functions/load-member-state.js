// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./profile/load-member-state').handler;
