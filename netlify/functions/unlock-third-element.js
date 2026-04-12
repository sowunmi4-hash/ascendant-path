// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./profile/unlock-third-element').handler;
