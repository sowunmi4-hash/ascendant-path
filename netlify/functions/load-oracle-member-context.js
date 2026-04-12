// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./oracle/load-context').handler;
