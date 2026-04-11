const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY
);

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function cleanText(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function cleanLower(value) {
  return cleanText(value).toLowerCase();
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeBoolean(value) {
  return value === true;
}

function parseBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function firstRow(data) {
  return Array.isArray(data) && data.length > 0 ? data[0] : null;
}

function normalizeEvidenceJson(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  return {};
}

function mapTask(row) {
  if (!row) return null;

  const progressCurrent = safeNumber(row.progress_current, 0);
  const progressTarget = safeNumber(row.progress_target, 1);

  return {
    task_instance_id: row.task_instance_id,
    task_id: row.task_id || null,
    task_key: cleanText(row.task_key),
    owner_rank_key: cleanText(row.owner_rank_key),
    task_name: cleanText(row.task_name),
    task_frequency: cleanText(row.task_frequency),
    proof_type: cleanText(row.proof_type),
    requires_approval: safeBoolean(row.requires_approval),
    auto_progress_rule: cleanText(row.auto_progress_rule) || null,
    display_order: safeNumber(row.display_order, 0),
    description: cleanText(row.description),
    cycle_key: cleanText(row.cycle_key),
    cycle_started_at: row.cycle_started_at || null,
    cycle_ends_at: row.cycle_ends_at || null,
    progress_current: progressCurrent,
    progress_target: progressTarget,
    progress_percent:
      progressTarget > 0
        ? Math.min(100, Math.round((progressCurrent / progressTarget) * 100))
        : 0,
    task_status: cleanText(row.task_status),
    proof_status: cleanText(row.proof_status),
    proof_note: cleanText(row.proof_note) || null,
    submitted_at: row.submitted_at || null,
    approved_at: row.approved_at || null,
    claimed_at: row.claimed_at || null,
    reward_tokens: safeNumber(row.reward_tokens, 0),
    reward_loyalty_percent: safeNumber(row.reward_loyalty_percent, 0),
    reward_clan_contribution: safeNumber(row.reward_clan_contribution, 0),
    reward_promotion_credit: safeNumber(row.reward_promotion_credit, 0),
    can_submit_progress:
      ["available", "in_progress"].includes(cleanText(row.task_status)),
    can_claim_reward: cleanText(row.task_status) === "approved",
    is_completed:
      ["approved", "claimed"].includes(cleanText(row.task_status))
  };
}

async function loadMyClanState(sl_avatar_key, sl_username) {
  const { data, error } = await supabase.rpc("get_my_clan_state", {
    p_sl_avatar_key: sl_avatar_key || null,
    p_sl_username: sl_username || null
  });

  if (error) {
    throw new Error(`Failed to load my clan state: ${error.message}`);
  }

  return firstRow(data);
}

async function submitTaskProgress({
  membership_id,
  task_key,
  progress_delta,
  proof_note,
  evidence_json
}) {
  const { data, error } = await supabase.rpc("submit_clan_task_progress", {
    p_membership_id: membership_id,
    p_task_key: task_key,
    p_progress_delta: progress_delta,
    p_proof_note: proof_note || null,
    p_evidence_json: evidence_json || {}
  });

  if (error) {
    throw new Error(`Failed to submit clan task progress: ${error.message}`);
  }

  return firstRow(data);
}

async function loadCurrentTasks(membership_id) {
  const { data, error } = await supabase.rpc("get_current_clan_tasks_for_membership", {
    p_membership_id: membership_id
  });

  if (error) {
    throw new Error(`Failed to reload current clan tasks: ${error.message}`);
  }

  return Array.isArray(data) ? data : [];
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return buildResponse(200, { ok: true });
  }

  if (event.httpMethod !== "POST") {
    return buildResponse(405, {
      success: false,
      message: "Method not allowed. Use POST."
    });
  }

  try {
    const body = parseBody(event);

    const sl_avatar_key = cleanText(body.sl_avatar_key);
    const sl_username = cleanLower(body.sl_username);
    const task_key = cleanLower(body.task_key);
    const progress_delta = Math.max(1, safeNumber(body.progress_delta, 1));
    const proof_note = cleanText(body.proof_note);
    const evidence_json = normalizeEvidenceJson(body.evidence_json);

    if (!sl_avatar_key && !sl_username) {
      return buildResponse(400, {
        success: false,
        message: "Missing required member identity. Provide sl_avatar_key or sl_username."
      });
    }

    if (!task_key) {
      return buildResponse(400, {
        success: false,
        message: "Missing required field: task_key."
      });
    }

    const membership = await loadMyClanState(sl_avatar_key, sl_username);

    if (!membership) {
      return buildResponse(404, {
        success: false,
        message: "No active clan membership found for this user."
      });
    }

    const submitResult = await submitTaskProgress({
      membership_id: membership.membership_id,
      task_key,
      progress_delta,
      proof_note,
      evidence_json
    });

    const reloadedTasks = await loadCurrentTasks(membership.membership_id);
    const updatedTaskRow =
      reloadedTasks.find((row) => cleanLower(row.task_key) === task_key) || submitResult;

    const updatedTask = mapTask(updatedTaskRow);

    return buildResponse(200, {
      success: true,
      message: "Clan task progress submitted successfully.",
      viewer: {
        membership_id: membership.membership_id,
        clan_id: membership.clan_id,
        clan_key: cleanText(membership.clan_key),
        clan_name: cleanText(membership.clan_name),
        sl_avatar_key: cleanText(membership.sl_avatar_key),
        sl_username: cleanText(membership.sl_username),
        display_name: cleanText(membership.display_name) || cleanText(membership.sl_username),
        current_rank_key: cleanText(membership.current_rank_key),
        current_rank_name: cleanText(membership.current_rank_name)
      },
      submission: {
        task_instance_id: submitResult?.task_instance_id || null,
        task_key: cleanText(submitResult?.task_key || task_key),
        task_status: cleanText(submitResult?.task_status),
        progress_current: safeNumber(submitResult?.progress_current, 0),
        progress_target: safeNumber(submitResult?.progress_target, 1),
        progress_delta
      },
      task: updatedTask
    });
  } catch (error) {
    console.error("submit-clan-task-progress error:", error);

    return buildResponse(500, {
      success: false,
      message: "Failed to submit clan task progress.",
      error: error.message || "Unknown error."
    });
  }
};