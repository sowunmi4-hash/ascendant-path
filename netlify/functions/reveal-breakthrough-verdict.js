// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./breakthrough/verdict').handler;
