// Shim — forwards to new location. HUD and legacy callers keep working.
exports.handler = require('./library/load-book-state').handler;
