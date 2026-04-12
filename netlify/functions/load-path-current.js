const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

const alignmentSupabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY,
  {
    db: { schema: "alignment" }
  }
);

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  };
}

function parseCookies(cookieHeader = "") {
  const cookies = {};

  cookieHeader.split(";").forEach((part) => {
    const trimmed = part.trim();
    if (!trimmed) return;

    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) return;

    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();

    cookies[key] = decodeURIComponent(value);
  });

  return cookies;
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function safeLower(value, fallback = "") {
  return safeText(value, fallback).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function safeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;

  const text = safeLower(value);
  if (["true", "1", "yes", "y", "on", "active", "started"].includes(text)) return true;
  if (["false", "0", "no", "n", "off", "inactive", "stopped"].includes(text)) return false;

  return fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function roundNumber(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Number(number.toFixed(digits));
}

function normalizeKey(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function titleize(value, fallback = "Unknown") {
  const text = safeText(value);
  if (!text) return fallback;

  return text
    .replace(/_/g, " ")
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getPathLabel(pathKey) {
  const key = safeLower(pathKey, "unaligned");
  if (key === "yin") return "Yin";
  if (key === "yang") return "Yang";
  if (key === "taiji") return "Taiji";
  return "Unaligned";
}

function getPathTotal(pathKey, totals) {
  const key = safeLower(pathKey, "unaligned");
  if (key === "yin") return safeNumber(totals.yin, 0);
  if (key === "yang") return safeNumber(totals.yang, 0);
  if (key === "taiji") return safeNumber(totals.taiji, 0);
  return 0;
}

function getHourGroupFromPhase(phaseKeyOrName) {
  const normalized = normalizeKey(phaseKeyOrName);

  if (["moon_grave_silence", "night_tide_omen"].includes(normalized)) {
    return "yin";
  }

  if (["veil_of_first_breath", "ashen_descent"].includes(normalized)) {
    return "taiji";
  }

  if (["golden_meridian", "zenith_of_heaven"].includes(normalized)) {
    return "yang";
  }

  return "";
}

function getMultipliers(pathKey, hourGroup) {
  const path = safeLower(pathKey, "unaligned");
  const group = safeLower(hourGroup);

  if (path === "yin") {
    if (group === "yin") return { qi: 1.5, cp: 1.5, state_label: "aligned" };
    if (group === "taiji") return { qi: 0.9, cp: 0.9, state_label: "tempered" };
    if (group === "yang") return { qi: 0.5, cp: 0.5, state_label: "opposed" };
  }

  if (path === "yang") {
    if (group === "yang") return { qi: 1.5, cp: 1.5, state_label: "aligned" };
    if (group === "taiji") return { qi: 0.9, cp: 0.9, state_label: "tempered" };
    if (group === "yin") return { qi: 0.5, cp: 0.5, state_label: "opposed" };
  }

  if (path === "taiji") {
    if (group === "taiji") return { qi: 1.5, cp: 1.5, state_label: "aligned" };
    if (group === "yin" || group === "yang") return { qi: 1.0, cp: 1.0, state_label: "steady" };
  }

  return { qi: 1.0, cp: 1.0, state_label: "forming" };
}

function getAlignedBonusWindowActive(pathKey, hourGroup) {
  const path = safeLower(pathKey, "unaligned");
  const group = safeLower(hourGroup);

  if (!path || path === "unaligned") return false;
  return path === group;
}

function getBiasSpreadFromKey(biasKey) {
  const key = safeLower(biasKey);

  if (key === "yin") {
    return { yin: 12, yang: 2, taiji: 6 };
  }

  if (key === "yang") {
    return { yin: 2, yang: 12, taiji: 6 };
  }

  if (key === "taiji") {
    return { yin: 5, yang: 5, taiji: 10 };
  }

  if (key === "yin_to_taiji") {
    return { yin: 9, yang: 3, taiji: 8 };
  }

  if (key === "yang_to_taiji") {
    return { yin: 3, yang: 9, taiji: 8 };
  }

  return { yin: 0, yang: 0, taiji: 0 };
}

function buildBiasTotals({ phaseBias, forceBias, phenomenonBias, fallbackBiasKey }) {
  const totals = {
    yin: roundNumber(
      safeNumber(phaseBias?.yin_bias, 0) +
      safeNumber(forceBias?.yin_bias, 0) +
      safeNumber(phenomenonBias?.yin_bias, 0),
      2
    ),
    yang: roundNumber(
      safeNumber(phaseBias?.yang_bias, 0) +
      safeNumber(forceBias?.yang_bias, 0) +
      safeNumber(phenomenonBias?.yang_bias, 0),
      2
    ),
    taiji: roundNumber(
      safeNumber(phaseBias?.taiji_bias, 0) +
      safeNumber(forceBias?.taiji_bias, 0) +
      safeNumber(phenomenonBias?.taiji_bias, 0),
      2
    )
  };

  if (totals.yin === 0 && totals.yang === 0 && totals.taiji === 0) {
    const fallback = getBiasSpreadFromKey(fallbackBiasKey);
    return {
      yin: roundNumber(fallback.yin, 2),
      yang: roundNumber(fallback.yang, 2),
      taiji: roundNumber(fallback.taiji, 2)
    };
  }

  return totals;
}

function getLeaderFromTotals(totals) {
  const entries = [
    { key: "yin", label: "Yin", total: safeNumber(totals?.yin, 0) },
    { key: "yang", label: "Yang", total: safeNumber(totals?.yang, 0) },
    { key: "taiji", label: "Taiji", total: safeNumber(totals?.taiji, 0) }
  ].sort((a, b) => b.total - a.total);

  return {
    leader: entries[0],
    second: entries[1],
    third: entries[2]
  };
}

function buildOrbState(totals) {
  const yin = safeNumber(totals.yin, 0);
  const yang = safeNumber(totals.yang, 0);
  const taiji = safeNumber(totals.taiji, 0);
  const total = yin + yang + taiji;

  const { leader, second, third } = getLeaderFromTotals({ yin, yang, taiji });

  return {
    yin_percent: total > 0 ? roundNumber((yin / total) * 100, 2) : 0,
    yang_percent: total > 0 ? roundNumber((yang / total) * 100, 2) : 0,
    taiji_percent: total > 0 ? roundNumber((taiji / total) * 100, 2) : 0,
    leader_key: leader.key,
    leader_label: leader.label,
    leader_total: leader.total,
    second_key: second.key,
    second_label: second.label,
    second_total: second.total,
    third_key: third.key,
    third_label: third.label,
    third_total: third.total,
    lead_margin_over_second: roundNumber(leader.total - second.total, 2),
    total_drift: roundNumber(total, 2)
  };
}

function calculateCandidateProgress(pathKey, totals, settings) {
  const totalDrift =
    safeNumber(totals.yin, 0) +
    safeNumber(totals.yang, 0) +
    safeNumber(totals.taiji, 0);

  const currentValue = getPathTotal(pathKey, totals);

  const totalRequired = safeNumber(settings.first_reveal_total_required, 180);
  const standardLeadRequired = safeNumber(settings.standard_lead_margin, 25);
  const taijiLeadRequired = safeNumber(settings.taiji_lead_margin, 20);

  const otherTotals =
    pathKey === "yin"
      ? [safeNumber(totals.yang, 0), safeNumber(totals.taiji, 0)]
      : pathKey === "yang"
        ? [safeNumber(totals.yin, 0), safeNumber(totals.taiji, 0)]
        : [safeNumber(totals.yin, 0), safeNumber(totals.yang, 0)];

  const secondHighest = Math.max(...otherTotals);
  const leadGap = roundNumber(currentValue - secondHighest, 2);

  let requiredValue = 0;
  let leadRequired = standardLeadRequired;
  let extraProgressRatios = [];
  let extraRequirements = {};

  if (pathKey === "yin") {
    requiredValue = safeNumber(settings.yin_required, 100);
    leadRequired = standardLeadRequired;
  }

  if (pathKey === "yang") {
    requiredValue = safeNumber(settings.yang_required, 100);
    leadRequired = standardLeadRequired;
  }

  if (pathKey === "taiji") {
    requiredValue = safeNumber(settings.taiji_required, 100);
    leadRequired = taijiLeadRequired;

    const taijiMinYin = safeNumber(settings.taiji_min_yin, 40);
    const taijiMinYang = safeNumber(settings.taiji_min_yang, 40);

    const yinRatio = taijiMinYin > 0 ? safeNumber(totals.yin, 0) / taijiMinYin : 1;
    const yangRatio = taijiMinYang > 0 ? safeNumber(totals.yang, 0) / taijiMinYang : 1;

    extraProgressRatios = [yinRatio, yangRatio];

    extraRequirements = {
      taiji_min_yin: taijiMinYin,
      taiji_min_yang: taijiMinYang,
      current_yin_total: roundNumber(safeNumber(totals.yin, 0), 2),
      current_yang_total: roundNumber(safeNumber(totals.yang, 0), 2),
      taiji_min_yin_met: safeNumber(totals.yin, 0) >= taijiMinYin,
      taiji_min_yang_met: safeNumber(totals.yang, 0) >= taijiMinYang,
      taiji_min_yin_remaining: Math.max(0, roundNumber(taijiMinYin - safeNumber(totals.yin, 0), 2)),
      taiji_min_yang_remaining: Math.max(0, roundNumber(taijiMinYang - safeNumber(totals.yang, 0), 2))
    };
  }

  const totalRatio = totalRequired > 0 ? totalDrift / totalRequired : 1;
  const requirementRatio = requiredValue > 0 ? currentValue / requiredValue : 1;
  const leadRatio = leadRequired > 0 ? leadGap / leadRequired : 1;

  const ratios = [totalRatio, requirementRatio, leadRatio, ...extraProgressRatios]
    .filter((value) => Number.isFinite(value));

  const overallProgressPercent = roundNumber(clamp(Math.min(...ratios) * 100, 0, 100), 2);

  const ready =
    totalDrift >= totalRequired &&
    currentValue >= requiredValue &&
    leadGap >= leadRequired &&
    extraProgressRatios.every((ratio) => ratio >= 1);

  return {
    key: pathKey,
    label: getPathLabel(pathKey),
    ready,
    current_total: roundNumber(currentValue, 2),
    total_drift: roundNumber(totalDrift, 2),
    total_required: totalRequired,
    total_remaining: Math.max(0, roundNumber(totalRequired - totalDrift, 2)),
    requirement_required: requiredValue,
    requirement_remaining: Math.max(0, roundNumber(requiredValue - currentValue, 2)),
    lead_gap: leadGap,
    lead_required: leadRequired,
    lead_remaining: Math.max(0, roundNumber(leadRequired - leadGap, 2)),
    total_progress_percent: roundNumber(clamp(totalRatio * 100, 0, 100), 2),
    requirement_progress_percent: roundNumber(clamp(requirementRatio * 100, 0, 100), 2),
    lead_progress_percent: roundNumber(clamp(leadRatio * 100, 0, 100), 2),
    overall_progress_percent: overallProgressPercent,
    extra_requirements: extraRequirements
  };
}

function getBestRevealCandidate(candidates) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return null;
  }

  const sorted = [...candidates].sort((a, b) => {
    if (Boolean(b.ready) !== Boolean(a.ready)) return Number(b.ready) - Number(a.ready);
    if (b.overall_progress_percent !== a.overall_progress_percent) {
      return b.overall_progress_percent - a.overall_progress_percent;
    }
    if (b.current_total !== a.current_total) {
      return b.current_total - a.current_total;
    }
    return 0;
  });

  return sorted[0] || null;
}

function buildRevealSummary(currentPath, candidates, orbState) {
  const normalizedPath = safeLower(currentPath, "unaligned");
  const bestCandidate = getBestRevealCandidate(candidates);

  if (normalizedPath !== "unaligned") {
    return {
      reveal_status: "revealed",
      reveal_label: "Revealed",
      current_path_key: normalizedPath,
      current_path_label: getPathLabel(normalizedPath),
      current_candidate_key: bestCandidate?.key || orbState?.leader_key || normalizedPath,
      current_candidate_label: bestCandidate?.label || orbState?.leader_label || getPathLabel(normalizedPath),
      ready_to_reveal: false,
      summary_line: `${getPathLabel(normalizedPath)} is your revealed path.`
    };
  }

  if (bestCandidate?.ready) {
    return {
      reveal_status: "ready_to_reveal",
      reveal_label: "Ready to Reveal",
      current_path_key: "unaligned",
      current_path_label: "Unaligned",
      current_candidate_key: bestCandidate.key,
      current_candidate_label: bestCandidate.label,
      ready_to_reveal: true,
      summary_line: `${bestCandidate.label} has met the first reveal requirements and is ready to emerge.`
    };
  }

  return {
    reveal_status: "forming",
    reveal_label: "Still Forming",
    current_path_key: "unaligned",
    current_path_label: "Unaligned",
    current_candidate_key: bestCandidate?.key || orbState?.leader_key || "yin",
    current_candidate_label: bestCandidate?.label || orbState?.leader_label || "Yin",
    ready_to_reveal: false,
    summary_line: `${bestCandidate?.label || orbState?.leader_label || "A path"} is currently leading, but your path is still forming.`
  };
}

function buildInfluenceLine({
  phaseName,
  forceName,
  phenomenonName,
  movingTowardLabel,
  hourGroupLabel,
  cultivationActive,
  currentPathLabel,
  alignedBonusWindowActive
}) {
  const phaseText = safeText(phaseName, "the current phase");
  const forceText = safeText(forceName, "the present force");
  const phenomenonText = safeText(phenomenonName);
  const currentText = phenomenonText && safeLower(phenomenonText) !== "none"
    ? `${phaseText}, ${forceText}, and ${phenomenonText}`
    : `${phaseText} and ${forceText}`;

  if (cultivationActive && alignedBonusWindowActive && currentPathLabel !== "Unaligned") {
    return `${currentText} are favoring ${currentPathLabel} during ${hourGroupLabel}, while the live current is pulling toward ${movingTowardLabel}.`;
  }

  if (cultivationActive) {
    return `${currentText} are shaping this session and currently pulling toward ${movingTowardLabel}.`;
  }

  return `${currentText} are currently pulling toward ${movingTowardLabel}.`;
}

function buildConditionCapLine(conditionState, phaseName) {
  const awarded = safeNumber(conditionState?.total_awarded, 0);
  const totalCap = safeNumber(conditionState?.total_cap, 20);
  const remaining = Math.max(0, totalCap - awarded);
  const label = safeText(phaseName, "this celestial window");

  if (remaining <= 0) {
    return `${label} has reached its current Path Drift cap. Qi and CP can continue, but drift will wait for the next celestial shift.`;
  }

  return `${label} has granted ${awarded} of ${totalCap} Path Drift in this current window, with ${remaining} remaining before saturation.`;
}

async function loadPathRuleSettings() {
  let { data, error } = await alignmentSupabase
    .from("path_rule_settings")
    .select("*")
    .eq("settings_key", "default")
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load path_rule_settings: ${error.message}`);
  }

  if (data) return data;

  const fallback = await alignmentSupabase
    .from("path_rule_settings")
    .select("*")
    .limit(1)
    .maybeSingle();

  if (fallback.error) {
    throw new Error(`Failed to load fallback path_rule_settings: ${fallback.error.message}`);
  }

  return fallback.data || null;
}

async function loadMemberPathState(memberAvatarKey) {
  const { data, error } = await alignmentSupabase
    .from("member_path_state")
    .select("*")
    .eq("member_avatar_key", memberAvatarKey)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load member_path_state: ${error.message}`);
  }

  return data || null;
}

async function loadCurrentDriftCondition() {
  try {
    const { data, error } = await supabase.rpc("get_current_drift_condition");

    if (error) {
      throw new Error(`Failed to resolve current drift condition: ${error.message}`);
    }

    if (Array.isArray(data)) {
      return data[0] || null;
    }

    return data || null;
  } catch (error) {
    throw error;
  }
}

async function loadCurrentConditionState(memberAvatarKey, conditionSignature) {
  if (!safeText(memberAvatarKey) || !safeText(conditionSignature)) return null;

  const { data, error } = await alignmentSupabase
    .from("member_path_condition_drift_state")
    .select("*")
    .eq("member_avatar_key", memberAvatarKey)
    .eq("condition_signature", conditionSignature)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to load member_path_condition_drift_state: ${error.message}`);
  }

  return data || null;
}

async function getLiveMeditationPreview(memberAvatarKey) {
  try {
    const { data, error } = await alignmentSupabase.rpc(
      "get_live_member_meditation_preview",
      {
        p_member_avatar_key: memberAvatarKey,
        p_aligned_minute_streak: 1
      }
    );

    if (error) {
      console.error("load-path-current preview rpc error:", error);
      return null;
    }

    if (Array.isArray(data)) {
      return data[0] || null;
    }

    return data || null;
  } catch (error) {
    console.error("load-path-current preview catch error:", error);
    return null;
  }
}

async function loadPhaseBias(phaseKey, hourGroup) {
  if (!safeText(phaseKey) || !safeText(hourGroup)) return null;

  const { data, error } = await alignmentSupabase
    .from("phase_bias")
    .select("*")
    .eq("phase_key", phaseKey)
    .eq("hour_group", hourGroup)
    .maybeSingle();

  if (error) {
    console.error("load-path-current phase_bias error:", error);
    return null;
  }

  return data || null;
}

async function loadForceBias(forceKey) {
  if (!safeText(forceKey)) return null;

  const { data, error } = await alignmentSupabase
    .from("force_bias")
    .select("*")
    .eq("force_key", forceKey)
    .maybeSingle();

  if (error) {
    console.error("load-path-current force_bias error:", error);
    return null;
  }

  return data || null;
}

async function loadPhenomenonBias(phenomenonKey) {
  if (!safeText(phenomenonKey) || safeLower(phenomenonKey) === "none") return null;

  const { data, error } = await alignmentSupabase
    .from("phenomenon_bias")
    .select("*")
    .eq("phenomenon_key", phenomenonKey)
    .maybeSingle();

  if (error) {
    console.error("load-path-current phenomenon_bias error:", error);
    return null;
  }

  return data || null;
}

async function loadConversionMatrix(currentPath, targetPath) {
  if (!safeText(currentPath) || !safeText(targetPath)) return null;
  if (safeLower(currentPath) === safeLower(targetPath)) return null;
  if (safeLower(currentPath) === "unaligned") return null;

  const { data, error } = await alignmentSupabase
    .from("path_conversion_matrix")
    .select("*")
    .eq("current_path", currentPath)
    .eq("target_path", targetPath)
    .maybeSingle();

  if (error) {
    console.error("load-path-current conversion_matrix error:", error);
    return null;
  }

  return data || null;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
    return json(405, {
      success: false,
      error: "method_not_allowed"
    });
  }

  try {
    const cookieHeader =
      event.headers.cookie ||
      event.headers.Cookie ||
      "";

    const cookies = parseCookies(cookieHeader);
    const sessionToken = cookies[process.env.SESSION_COOKIE_NAME || "ap_session"];

    if (!sessionToken) {
      return json(401, {
        success: false,
        error: "not_logged_in"
      });
    }

    const { data: sessionRow, error: sessionError } = await supabase
      .from("website_sessions")
      .select("*")
      .eq("session_token", sessionToken)
      .eq("is_active", true)
      .maybeSingle();

    if (sessionError) {
      console.error("load-path-current session lookup error:", sessionError);
      return json(500, {
        success: false,
        error: "server_error"
      });
    }

    if (!sessionRow) {
      return json(401, {
        success: false,
        error: "invalid_session"
      });
    }

    const { data: memberRow, error: memberError } = await supabase
      .from("cultivation_members")
      .select("*")
      .eq("sl_avatar_key", sessionRow.sl_avatar_key)
      .maybeSingle();

    if (memberError) {
      console.error("load-path-current member lookup error:", memberError);
      return json(500, {
        success: false,
        error: "server_error"
      });
    }

    if (!memberRow) {
      return json(404, {
        success: false,
        error: "member_not_found"
      });
    }

    const memberAvatarKey = safeText(memberRow.sl_avatar_key);
    const memberUsername = safeText(memberRow.sl_username);

    const [settingsRow, pathStateRow, previewPayload, currentDriftCondition] = await Promise.all([
      loadPathRuleSettings(),
      loadMemberPathState(memberAvatarKey),
      getLiveMeditationPreview(memberAvatarKey),
      loadCurrentDriftCondition()
    ]);

    const currentConditionState = await loadCurrentConditionState(
      memberAvatarKey,
      currentDriftCondition?.condition_signature
    );

    const currentPath = safeText(
      pathStateRow?.current_path,
      "unaligned"
    );

    const totals = {
      yin: roundNumber(safeNumber(pathStateRow?.yin_total, 0), 2),
      yang: roundNumber(safeNumber(pathStateRow?.yang_total, 0), 2),
      taiji: roundNumber(safeNumber(pathStateRow?.taiji_total, 0), 2)
    };

    const totalDrift = roundNumber(totals.yin + totals.yang + totals.taiji, 2);
    const orbState = buildOrbState(totals);

    const previewCelestial = previewPayload?.celestial || {};
    const previewRaw = previewPayload?.preview || null;

    const phaseKey = safeText(
      previewCelestial.phase_key,
      currentDriftCondition?.phase_key || ""
    );

    const phaseName = safeText(
      previewCelestial.phase_name,
      currentDriftCondition?.phase_name || ""
    );

    const forceKey = safeText(previewCelestial.force_key);
    const forceName = safeText(previewCelestial.force_name);
    const phenomenonKey = safeText(previewCelestial.phenomenon_key);
    const phenomenonName = safeText(previewCelestial.phenomenon_name, "None");

    const effectiveBias = safeText(
      previewCelestial.effective_bias,
      currentDriftCondition?.phase_bias || ""
    );

    const dashboardEffectLabel = safeText(previewCelestial.dashboard_effect_label);
    const dashboardEffectSummary = safeText(previewCelestial.dashboard_effect_summary);

    const hourGroup = getHourGroupFromPhase(phaseKey || phaseName);
    const hourGroupLabel = titleize(hourGroup, "Unknown Hours");

    const [phaseBias, forceBias, phenomenonBias] = await Promise.all([
      loadPhaseBias(phaseKey, hourGroup),
      loadForceBias(forceKey),
      loadPhenomenonBias(phenomenonKey)
    ]);

    const currentBiasTotals = buildBiasTotals({
      phaseBias,
      forceBias,
      phenomenonBias,
      fallbackBiasKey: effectiveBias
    });

    const biasLeaderState = getLeaderFromTotals(currentBiasTotals);
    const movingTowardKey = biasLeaderState.leader.key;
    const movingTowardLabel = biasLeaderState.leader.label;

    const yinCandidate = calculateCandidateProgress("yin", totals, settingsRow || {});
    const yangCandidate = calculateCandidateProgress("yang", totals, settingsRow || {});
    const taijiCandidate = calculateCandidateProgress("taiji", totals, settingsRow || {});
    const revealCandidates = [yinCandidate, yangCandidate, taijiCandidate];

    const revealSummary = buildRevealSummary(currentPath, revealCandidates, orbState);

    const currentPathLabel = getPathLabel(currentPath);
    const liveMultipliers = getMultipliers(currentPath, hourGroup);
    const alignedBonusWindowActive = getAlignedBonusWindowActive(currentPath, hourGroup);

    const influenceLine = buildInfluenceLine({
      phaseName,
      forceName,
      phenomenonName,
      movingTowardLabel,
      hourGroupLabel,
      cultivationActive: safeLower(memberRow?.v2_cultivation_status) === "cultivating",
      currentPathLabel,
      alignedBonusWindowActive
    });

    const driftShiftTargetKey =
      safeLower(currentPath) !== "unaligned" &&
      orbState.leader_key !== safeLower(currentPath)
        ? orbState.leader_key
        : null;

    const conversionRow = await loadConversionMatrix(currentPath, driftShiftTargetKey);

    const conversionWarningActive =
      Boolean(driftShiftTargetKey) &&
      Boolean(conversionRow) &&
      safeLower(currentPath) !== safeLower(driftShiftTargetKey);

    const conversionWarningLine = conversionWarningActive
      ? `Drifting from ${currentPathLabel} toward ${getPathLabel(driftShiftTargetKey)} would cost ${safeNumber(conversionRow.auric_sacrifice_per_minute, 0)} Auric per minute.`
      : "";

    const firstRevealTotalRequired = safeNumber(settingsRow?.first_reveal_total_required, 180);
    const firstRevealProgressPercent = roundNumber(
      clamp((totalDrift / firstRevealTotalRequired) * 100, 0, 100),
      2
    );

    const conditionCap = {
      condition_signature: safeText(currentConditionState?.condition_signature, currentDriftCondition?.condition_signature || ""),
      phase_key: safeText(currentConditionState?.phase_key, currentDriftCondition?.phase_key || ""),
      phase_name: safeText(currentConditionState?.phase_name, currentDriftCondition?.phase_name || ""),
      phase_bias: safeText(currentConditionState?.phase_bias, currentDriftCondition?.phase_bias || ""),
      total_cap: safeNumber(currentConditionState?.total_cap, currentDriftCondition?.total_cap || 20),
      total_awarded: safeNumber(currentConditionState?.total_awarded, 0),
      total_remaining: Math.max(
        0,
        safeNumber(currentConditionState?.total_cap, currentDriftCondition?.total_cap || 20) -
        safeNumber(currentConditionState?.total_awarded, 0)
      ),
      yin_cap: safeNumber(currentConditionState?.yin_cap, currentDriftCondition?.yin_cap || 0),
      yang_cap: safeNumber(currentConditionState?.yang_cap, currentDriftCondition?.yang_cap || 0),
      taiji_cap: safeNumber(currentConditionState?.taiji_cap, currentDriftCondition?.taiji_cap || 0),
      yin_awarded: safeNumber(currentConditionState?.yin_awarded, 0),
      yang_awarded: safeNumber(currentConditionState?.yang_awarded, 0),
      taiji_awarded: safeNumber(currentConditionState?.taiji_awarded, 0),
      slt_window_start: currentConditionState?.slt_window_start || currentDriftCondition?.slt_window_start || null,
      slt_window_end: currentConditionState?.slt_window_end || currentDriftCondition?.slt_window_end || null,
      exhausted: Boolean(currentConditionState?.exhausted_at),
      exhausted_at: currentConditionState?.exhausted_at || null,
      summary_line: buildConditionCapLine(
        {
          total_awarded: currentConditionState?.total_awarded,
          total_cap: currentConditionState?.total_cap || currentDriftCondition?.total_cap || 20
        },
        phaseName || currentDriftCondition?.phase_name
      )
    };

    return json(200, {
      success: true,
      generated_at: new Date().toISOString(),

      path_current: {
        member_avatar_key: memberAvatarKey,
        member_username: memberUsername,
        character_name: safeText(memberRow.character_name, "Unnamed Cultivator"),

        current_path_key: safeLower(currentPath, "unaligned"),
        current_path_label: currentPathLabel,
        first_revealed_at: pathStateRow?.first_revealed_at || null,
        last_path_shift_at: pathStateRow?.last_path_shift_at || null,
        lifetime_conversion_auric_spent: roundNumber(
          safeNumber(pathStateRow?.lifetime_conversion_auric_spent, 0),
          2
        ),

        totals: {
          yin: totals.yin,
          yang: totals.yang,
          taiji: totals.taiji,
          total_drift: totalDrift
        },

        orb: orbState,

        reveal: {
          settings_key: safeText(settingsRow?.settings_key, "default"),
          reveal_status: revealSummary.reveal_status,
          reveal_label: revealSummary.reveal_label,
          ready_to_reveal: revealSummary.ready_to_reveal,
          summary_line: revealSummary.summary_line,

          first_reveal_total_required: firstRevealTotalRequired,
          first_reveal_total_remaining: Math.max(0, roundNumber(firstRevealTotalRequired - totalDrift, 2)),
          first_reveal_progress_percent: firstRevealProgressPercent,

          current_candidate_key: revealSummary.current_candidate_key,
          current_candidate_label: revealSummary.current_candidate_label,

          candidates: {
            yin: yinCandidate,
            yang: yangCandidate,
            taiji: taijiCandidate
          }
        },

        current: {
          slt_now: previewCelestial.slt_now || null,
          slt_today: previewCelestial.slt_today || null,
          slt_hour: safeNumber(previewCelestial.slt_hour, 0),

          phase_key: phaseKey || null,
          phase_name: phaseName || null,
          force_key: forceKey || null,
          force_name: forceName || null,
          phenomenon_key: phenomenonKey || null,
          phenomenon_name: phenomenonName || null,
          effective_bias: effectiveBias || null,
          hour_group: hourGroup || null,
          hour_group_label: hourGroupLabel,

          dashboard_effect_label: dashboardEffectLabel || null,
          dashboard_effect_summary: dashboardEffectSummary || null,

          phase_bias: phaseBias
            ? {
                yin_bias: roundNumber(safeNumber(phaseBias.yin_bias, 0), 2),
                yang_bias: roundNumber(safeNumber(phaseBias.yang_bias, 0), 2),
                taiji_bias: roundNumber(safeNumber(phaseBias.taiji_bias, 0), 2)
              }
            : null,

          force_bias: forceBias
            ? {
                yin_bias: roundNumber(safeNumber(forceBias.yin_bias, 0), 2),
                yang_bias: roundNumber(safeNumber(forceBias.yang_bias, 0), 2),
                taiji_bias: roundNumber(safeNumber(forceBias.taiji_bias, 0), 2)
              }
            : null,

          phenomenon_bias: phenomenonBias
            ? {
                yin_bias: roundNumber(safeNumber(phenomenonBias.yin_bias, 0), 2),
                yang_bias: roundNumber(safeNumber(phenomenonBias.yang_bias, 0), 2),
                taiji_bias: roundNumber(safeNumber(phenomenonBias.taiji_bias, 0), 2)
              }
            : null,

          total_bias: {
            yin: currentBiasTotals.yin,
            yang: currentBiasTotals.yang,
            taiji: currentBiasTotals.taiji
          },

          moving_toward_key: movingTowardKey,
          moving_toward_label: movingTowardLabel,
          influence_line: influenceLine
        },

        condition_cap: conditionCap,

        live_session: {
          cultivation_active: safeLower(memberRow?.v2_cultivation_status) === "cultivating",
          cultivation_status: safeText(memberRow?.v2_cultivation_status, "idle"),
          accumulated_seconds: safeNumber(memberRow?.v2_accumulated_seconds, 0),
          cultivation_started_at: memberRow?.v2_cultivation_started_at || null,
          sessions_today: safeNumber(memberRow?.v2_sessions_today, 0),
          aligned_bonus_window_active: alignedBonusWindowActive,
          auric_multiplier: liveMultipliers.qi,
          cp_multiplier: liveMultipliers.cp,
          session_state_label: liveMultipliers.state_label,
          summary_line: safeLower(memberRow?.v2_cultivation_status) === "cultivating"
            ? alignedBonusWindowActive
              ? `${currentPathLabel} is in its favored hour window.`
              : `Cultivation is active under ${hourGroupLabel}.`
            : `Cultivation is inactive. The current heavens are leaning toward ${movingTowardLabel}.`
        },

        conversion: {
          warning_active: conversionWarningActive,
          from_path_key: conversionWarningActive ? safeLower(currentPath) : null,
          from_path_label: conversionWarningActive ? currentPathLabel : null,
          target_path_key: conversionWarningActive ? driftShiftTargetKey : null,
          target_path_label: conversionWarningActive ? getPathLabel(driftShiftTargetKey) : null,
          conversion_level: conversionWarningActive ? safeText(conversionRow?.conversion_level) : null,
          auric_sacrifice_per_minute: conversionWarningActive
            ? safeNumber(conversionRow?.auric_sacrifice_per_minute, 0)
            : 0,
          summary_line: conversionWarningLine || null
        },

        raw_preview: previewRaw
      }
    });
  } catch (err) {
    console.error("load-path-current error:", err);
    return json(500, {
      success: false,
      error: "server_error",
      message: err.message || "Unknown error"
    });
  }
};