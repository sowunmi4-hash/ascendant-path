// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./auth/generate-login-pin').handler;
