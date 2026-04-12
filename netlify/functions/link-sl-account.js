// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./auth/link-sl-account').handler;
