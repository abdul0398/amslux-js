const fs = require("fs");
const path = require("path");

/**
 * Tracks AI API token usage and cost per operation, writing to daily log files.
 *
 * Log files are stored in logs/ai-costs/
 *   - YYYY-MM-DD.log          → one JSON line per API call
 *   - YYYY-MM-DD-summary.json → running daily totals by model and operation
 *
 * Usage:
 *   // OpenAI (response from openai.chat.completions.create)
 *   AICostTrackerService.trackOpenAI('gpt-4.1-mini', 'validate_product', completion);
 *
 *   // Gemini (raw response object with json() method)
 *   AICostTrackerService.trackGemini('gemini-2.5-flash', 'brand_analysis', responseJson);
 *
 *   // Manual (if you have token counts from elsewhere)
 *   AICostTrackerService.track('gpt-4o', 'labeling', inputTokens, outputTokens);
 */

/**
 * Pricing per 1,000,000 tokens in USD.
 * Update these values when OpenAI / Google change their pricing.
 *
 * Models marked [estimated] are not in the primary pricing sheet —
 * verify against official pricing pages before relying on them.
 */
const PRICING = {
  // ── OpenAI ──────────────────────────────────────────────────────────
  "gpt-4o":        { input: 5.00,  output: 15.00 },  // confirmed
  "gpt-4.1":       { input: 2.00,  output: 8.00  },  // confirmed
  "gpt-4.1-mini":  { input: 0.40,  output: 1.60  },  // confirmed
  "gpt-4.1-nano":  { input: 0.10,  output: 0.40  },  // [estimated]
  "gpt-4o-mini":   { input: 0.15,  output: 0.60  },  // [estimated]
  "gpt-4-turbo":   { input: 10.00, output: 30.00 },  // [estimated]
  "gpt-4":         { input: 30.00, output: 60.00 },  // [estimated] legacy
  "gpt-3.5-turbo": { input: 0.50,  output: 1.50  },  // [estimated]

  // ── Google Gemini ────────────────────────────────────────────────────
  "gemini-2.5-flash":       { input: 0.30, output: 2.50 }, // confirmed
  "gemini-2.5-flash-image": { input: 0.30, output: 2.50 }, // same tier as flash
  "gemini-2.0-flash":       { input: 0.10, output: 0.40 }, // [estimated]
};

const LOG_DIR = path.join(__dirname, "../logs/ai-costs");

class AICostTrackerService {
  /**
   * Track an OpenAI API call using the response object from the openai SDK.
   *
   * @param {string} model     e.g. 'gpt-4.1-mini'
   * @param {string} operation Short descriptive name e.g. 'validate_product'
   * @param {object} response  The object returned by openai.chat.completions.create()
   */
  static trackOpenAI(model, operation, response) {
    const inputTokens  = response?.usage?.prompt_tokens     ?? 0;
    const outputTokens = response?.usage?.completion_tokens ?? 0;
    this.track(model, operation, inputTokens, outputTokens);
  }

  /**
   * Track a Gemini API call using the decoded JSON response.
   *
   * @param {string} model        e.g. 'gemini-2.5-flash'
   * @param {string} operation    Short descriptive name e.g. 'brand_analysis'
   * @param {object} responseData The parsed JSON from the Gemini response
   */
  static trackGemini(model, operation, responseData) {
    const inputTokens  = responseData?.usageMetadata?.promptTokenCount     ?? 0;
    const outputTokens = responseData?.usageMetadata?.candidatesTokenCount ?? 0;
    this.track(model, operation, inputTokens, outputTokens);
  }

  /**
   * Track any AI API call when you have raw token counts.
   *
   * @param {string} model
   * @param {string} operation
   * @param {number} inputTokens
   * @param {number} outputTokens
   */
  static track(model, operation, inputTokens, outputTokens) {
    const costUsd = this._calculateCost(model, inputTokens, outputTokens);

    const entry = {
      timestamp:     new Date().toISOString(),
      model,
      operation,
      input_tokens:  inputTokens,
      output_tokens: outputTokens,
      total_tokens:  inputTokens + outputTokens,
      cost_usd:      costUsd,
    };

    this._writeCallLog(entry);
    this._updateDailySummary(model, operation, inputTokens, outputTokens, costUsd);
  }

  // ─────────────────────────────────────────────────────────────────────────

  static _calculateCost(model, inputTokens, outputTokens) {
    const price = PRICING[model];

    if (!price) {
      console.warn(`AICostTracker: no pricing found for model '${model}'. Cost recorded as 0.`, {
        model,
        input_tokens: inputTokens,
        output_tokens: outputTokens,
      });
      return 0.0;
    }

    const inputCost  = (inputTokens  / 1_000_000) * price.input;
    const outputCost = (outputTokens / 1_000_000) * price.output;

    return parseFloat((inputCost + outputCost).toFixed(8));
  }

  /**
   * Append one JSON line to the daily call log.
   */
  static _writeCallLog(entry) {
    this._ensureDirectory(LOG_DIR);
    const file = path.join(LOG_DIR, `${this._today()}.log`);
    fs.appendFileSync(file, JSON.stringify(entry) + "\n");
  }

  /**
   * Keep a running JSON summary for the day (totals by model and by operation).
   */
  static _updateDailySummary(model, operation, inputTokens, outputTokens, costUsd) {
    this._ensureDirectory(LOG_DIR);
    const file = path.join(LOG_DIR, `${this._today()}-summary.json`);

    // Load existing summary or start fresh
    let summary = {};
    if (fs.existsSync(file)) {
      try {
        summary = JSON.parse(fs.readFileSync(file, "utf8")) ?? {};
      } catch {
        summary = {};
      }
    }

    // ── Global day totals ────────────────────────────────────────────────
    summary.date                = this._today();
    summary.last_updated        = new Date().toISOString();
    summary.total_calls         = (summary.total_calls         ?? 0) + 1;
    summary.total_input_tokens  = (summary.total_input_tokens  ?? 0) + inputTokens;
    summary.total_output_tokens = (summary.total_output_tokens ?? 0) + outputTokens;
    summary.total_cost_usd      = parseFloat(((summary.total_cost_usd ?? 0) + costUsd).toFixed(8));

    // ── Per-model breakdown ──────────────────────────────────────────────
    summary.by_model ??= {};
    summary.by_model[model] ??= this._emptyBucket();
    summary.by_model[model] = this._addToBucket(summary.by_model[model], inputTokens, outputTokens, costUsd);

    // ── Per-operation breakdown ──────────────────────────────────────────
    summary.by_operation ??= {};
    summary.by_operation[operation] ??= this._emptyBucket();
    summary.by_operation[operation] = this._addToBucket(summary.by_operation[operation], inputTokens, outputTokens, costUsd);

    // Sort by cost descending so the most expensive shows first
    summary.by_model     = this._sortByCostDesc(summary.by_model);
    summary.by_operation = this._sortByCostDesc(summary.by_operation);

    fs.writeFileSync(file, JSON.stringify(summary, null, 2) + "\n");
  }

  static _emptyBucket() {
    return { calls: 0, input_tokens: 0, output_tokens: 0, cost_usd: 0.0 };
  }

  static _addToBucket(bucket, inputTokens, outputTokens, costUsd) {
    return {
      calls:         bucket.calls         + 1,
      input_tokens:  bucket.input_tokens  + inputTokens,
      output_tokens: bucket.output_tokens + outputTokens,
      cost_usd:      parseFloat((bucket.cost_usd + costUsd).toFixed(8)),
    };
  }

  static _sortByCostDesc(obj) {
    return Object.fromEntries(
      Object.entries(obj).sort(([, a], [, b]) => b.cost_usd - a.cost_usd)
    );
  }

  static _today() {
    return new Date().toISOString().split("T")[0];
  }

  static _ensureDirectory(dir) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }
}

module.exports = AICostTrackerService;
