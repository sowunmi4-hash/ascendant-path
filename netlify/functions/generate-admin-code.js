// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./admin/generate-code').handler;
