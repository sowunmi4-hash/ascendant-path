const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SECRET_KEY;
const COOKIE_NAME = process.env.SESSION_COOKIE_NAME || "ap_session";

function json(status, body) {
  return {
    statusCode: status,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;
  for (const part of cookieHeader.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k) cookies[k.trim()] = decodeURIComponent(v.join("=").trim());
  }
  return cookies;
}

function normalize(text) {
  return (text || "").toLowerCase().trim()
    .replace(/[^\w\s]/g, "")
    .replace(/\s+/g, " ");
}

const STOP_WORDS = new Set([
  "what","is","are","a","an","the","how","why","do","does","i","my","me",
  "can","could","should","tell","about","explain","mean","means","there",
  "did","will","would","when","where","who","which","it","in","of","to",
  "and","or","for","on","at","by","from","with","that","this","be","has",
  "have","had","was","were","been","get","got","give","make","take","go",
  "im","ive","id","its","am","us","we","they","them","their","our","your"
]);

function extractKeywords(text) {
  return normalize(text).split(" ")
    .filter(w => w.length > 1 && !STOP_WORDS.has(w));
}

function slugify(text) {
  return (text || "").toLowerCase().trim()
    .replace(/[^\w\s]/g, "").replace(/\s+/g, "_");
}

async function resolveSession(supabase, cookieHeader) {
  const token = parseCookies(cookieHeader)[COOKIE_NAME] || "";
  if (!token) return null;
  const { data: session } = await supabase
    .from("website_sessions")
    .select("sl_avatar_key, sl_username")
    .eq("session_token", token)
    .eq("is_active", true)
    .maybeSingle();
  return session || null;
}

// Score how well a candidate string matches the input keywords
function scoreMatch(inputKeywords, candidateText) {
  if (!candidateText) return 0;
  const candidateKeywords = extractKeywords(candidateText);
  if (candidateKeywords.length === 0) return 0;
  const inputSet = new Set(inputKeywords);
  const matches = candidateKeywords.filter(k => inputSet.has(k)).length;
  // Score = matched / total candidate keywords (how much of term is covered)
  return matches / candidateKeywords.length;
}

async function searchGlossary(supabase, inputKeywords, normalizedInput) {
  const { data: terms } = await supabase
    .schema("glossary")
    .from("entries")
    .select("term_key, term_name, category, short_definition, full_explanation, mechanics_notes, aliases")
    .eq("is_active", true);

  if (!terms?.length) return null;

  let bestMatch = null;
  let bestScore = 0;

  for (const t of terms) {
    const candidates = [t.term_name, t.term_key, ...(t.aliases || [])];

    // Exact substring match — highest priority
    for (const c of candidates) {
      const nc = normalize(c);
      if (nc && (normalizedInput.includes(nc) || nc.includes(normalizedInput))) {
        return t; // instant return on exact match
      }
    }

    // Keyword score match
    for (const c of candidates) {
      const score = scoreMatch(inputKeywords, c);
      if (score > bestScore) {
        bestScore = score;
        bestMatch = t;
      }
    }
  }

  // Only return if score is strong enough (full keyword coverage)
  return bestScore >= 1.0 ? bestMatch : null;
}

async function searchFaq(supabase, inputKeywords, normalizedInput, intentKey) {
  // Intent match first
  if (intentKey) {
    const { data: byIntent } = await supabase
      .schema("oracle")
      .from("faq_questions")
      .select("*")
      .eq("live_intent_key", intentKey)
      .eq("is_active", true)
      .maybeSingle();
    if (byIntent) return byIntent;

    const { data: byKey } = await supabase
      .schema("oracle")
      .from("faq_questions")
      .select("*")
      .eq("question_key", intentKey)
      .eq("is_active", true)
      .maybeSingle();
    if (byKey) return byKey;
  }

  const { data: allQuestions } = await supabase
    .schema("oracle")
    .from("faq_questions")
    .select("*")
    .eq("is_active", true);

  if (!allQuestions?.length) return null;

  let bestMatch = null;
  let bestScore = 0;

  for (const q of allQuestions) {
    const candidates = [q.question_text, ...(q.question_aliases || [])];

    // Exact substring
    for (const c of candidates) {
      const nc = normalize(c);
      if (nc && (normalizedInput.includes(nc) || nc.includes(normalizedInput))) {
        return q;
      }
    }

    // Keyword score — 70% threshold for FAQ (more lenient than glossary)
    for (const c of candidates) {
      const score = scoreMatch(inputKeywords, c);
      if (score > bestScore) {
        bestScore = score;
        bestMatch = q;
      }
    }
  }

  return bestScore >= 0.7 ? bestMatch : null;
}

async function matchIntent(supabase, normalizedInput) {
  const { data: patterns } = await supabase
    .schema("oracle")
    .from("intent_patterns")
    .select("intent_key, pattern_regex, priority")
    .eq("is_active", true)
    .order("priority", { ascending: false });

  if (!patterns?.length) return null;
  for (const p of patterns) {
    try {
      if (new RegExp(p.pattern_regex, "i").test(normalizedInput)) return p.intent_key;
    } catch (_) {}
  }
  return null;
}

// Classify unknown input — is it system-related or completely foreign?
function classifyUnknown(inputKeywords, allTermNames) {
  if (!inputKeywords.length) return "unknown";

  // Build a set of all known system words from term names
  const systemWords = new Set();
  for (const name of allTermNames) {
    for (const word of extractKeywords(name)) {
      systemWords.add(word);
    }
  }

  // Also add core cultivation vocabulary
  const coreWords = new Set([
    "qi","cultivation","realm","stage","breakthrough","bond","path","yin","yang",
    "taiji","element","root","mortal","soul","core","foundation","nascent",
    "void","body","mahayana","tribulation","celestial","phase","resonance",
    "meditation","spirit","energy","force","phenomenon","alignment","drift",
    "library","volume","book","scripture","ascension","token","store","clan",
    "partner","bond","seal","comprehend","cultivate","advance","attune",
    "drain","draining","drained","sacrifice","sealed","comprehension","cultivator",
    "heaven","heavens","verdict","setback","cooldown","bottleneck","affinity",
    "attunement","refine","refinement"
  ]);

  const matchedSystem = inputKeywords.filter(w => systemWords.has(w) || coreWords.has(w));
  const ratio = matchedSystem.length / inputKeywords.length;

  if (ratio >= 0.5) return "system_related"; // at least half the words are system terms
  return "not_system";
}

async function resolveFaqAnswer(supabase, faqRow, avatarKey) {
  if (faqRow.answer_mode === "glossary" && faqRow.glossary_term_key) {
    const { data: term } = await supabase
      .schema("glossary").from("entries")
      .select("term_name, short_definition, full_explanation, mechanics_notes")
      .eq("term_key", faqRow.glossary_term_key)
      .eq("is_active", true)
      .maybeSingle();
    if (term) return { mode: "glossary", title: term.term_name, summary: term.short_definition, body: term.full_explanation, notes: term.mechanics_notes || null };
  }
  if (faqRow.answer_mode === "static" && faqRow.answer_text) {
    return { mode: "static", title: faqRow.answer_title || null, summary: faqRow.answer_summary || null, body: faqRow.answer_text };
  }
  if (faqRow.answer_mode === "live_intent") {
    const { data: ms } = await supabase.schema("oracle").from("member_runtime_state")
      .select("*").eq("member_avatar_key", avatarKey).maybeSingle();
    return { mode: "live_intent", title: faqRow.answer_title || null, summary: faqRow.answer_summary || faqRow.answer_text || null, body: faqRow.answer_text || null, member_state: ms || null };
  }
  return null;
}

async function logQuestion(supabase, session, rawInput, normalizedInput, matchedKey, matchedTermKey, resolvedMode, answered) {
  await supabase.schema("oracle").from("question_log").insert({
    member_avatar_key: session.sl_avatar_key,
    member_username: session.sl_username,
    raw_input: rawInput,
    normalized_input: normalizedInput,
    matched_question_key: matchedKey || null,
    matched_term_key: matchedTermKey || null,
    resolved_mode: resolvedMode || null,
    was_answered: answered,
    was_fallback: false,
  });
}

async function logUnanswered(supabase, session, rawInput, normalizedInput) {
  const { data: existing } = await supabase
    .schema("oracle").from("unanswered_question_candidates")
    .select("id, times_asked").eq("normalized_question", normalizedInput).maybeSingle();

  if (existing) {
    await supabase.schema("oracle").from("unanswered_question_candidates")
      .update({ times_asked: (existing.times_asked || 1) + 1, last_seen_at: new Date().toISOString(), last_member_avatar_key: session.sl_avatar_key, last_member_username: session.sl_username })
      .eq("id", existing.id);
  } else {
    await supabase.schema("oracle").from("unanswered_question_candidates").insert({
      canonical_question: rawInput, normalized_question: normalizedInput,
      times_asked: 1, first_seen_at: new Date().toISOString(),
      last_seen_at: new Date().toISOString(),
      last_member_avatar_key: session.sl_avatar_key,
      last_member_username: session.sl_username, status: "pending",
    });
  }
}

exports.handler = async (event) => {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const cookieHeader = event.headers?.cookie || event.headers?.Cookie || "";
  const session = await resolveSession(supabase, cookieHeader);
  if (!session) return json(401, { success: false, error: "Unauthorized" });

  let rawInput = "";
  try {
    const body = JSON.parse(event.body || "{}");
    rawInput = (body.question || "").trim();
  } catch (_) {}

  if (!rawInput) return json(400, { success: false, error: "No question provided" });

  const normalizedInput = normalize(rawInput);
  const inputKeywords = extractKeywords(rawInput);

  // Step 1 — Intent pattern match
  const intentKey = await matchIntent(supabase, normalizedInput);

  // Step 2 — FAQ search
  const faqRow = await searchFaq(supabase, inputKeywords, normalizedInput, intentKey);
  if (faqRow) {
    const answer = await resolveFaqAnswer(supabase, faqRow, session.sl_avatar_key);
    if (answer) {
      await logQuestion(supabase, session, rawInput, normalizedInput, faqRow.question_key, faqRow.glossary_term_key, faqRow.answer_mode, true);
      return json(200, { success: true, answered: true, source: "faq", question_key: faqRow.question_key, answer });
    }
  }

  // Step 3 — Glossary direct search
  const glossaryTerm = await searchGlossary(supabase, inputKeywords, normalizedInput);
  if (glossaryTerm) {
    const answer = { mode: "glossary", title: glossaryTerm.term_name, summary: glossaryTerm.short_definition, body: glossaryTerm.full_explanation, notes: glossaryTerm.mechanics_notes || null };
    await logQuestion(supabase, session, rawInput, normalizedInput, null, glossaryTerm.term_key, "glossary", true);
    return json(200, { success: true, answered: true, source: "glossary", answer });
  }

  // Step 4 — Classify the unknown input
  const { data: allTermsData } = await supabase.schema("glossary").from("entries").select("term_name").eq("is_active", true);
  const allTermNames = (allTermsData || []).map(t => t.term_name);
  const classification = classifyUnknown(inputKeywords, allTermNames);

  if (classification === "system_related") {
    // Keywords sound like system terms but we don't have a match yet — log for me to fill in
    await logQuestion(supabase, session, rawInput, normalizedInput, null, null, "pending_definition", false);
    await logUnanswered(supabase, session, rawInput, normalizedInput);
    return json(200, {
      success: true,
      answered: false,
      source: "pending_definition",
      message: "The Oracle recognises this may be part of your cultivation path but does not yet have a recorded answer. Your question has been captured and a definition will be provided soon.",
    });
  }

  // Step 5 — Clearly not part of the system
  await logQuestion(supabase, session, rawInput, normalizedInput, null, null, "not_system", false);
  return json(200, {
    success: true,
    answered: false,
    source: "not_system",
    message: "That concept is not part of the Ascendant Path system. The Oracle can only answer questions about cultivation, realms, breakthroughs, bonds, elements, paths, and other system mechanics.",
  });
};
