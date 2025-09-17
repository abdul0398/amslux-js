require("dotenv").config();
const puppeteer = require("puppeteer");
const cheerio = require("cheerio");
const { encoding_for_model } = require("tiktoken");
const OpenAI = require("openai");
const mysql = require("mysql2/promise");
const fs = require("fs").promises;
const path = require("path");
const https = require("https");
const http = require("http");
const { EventEmitter } = require("events");

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Database configuration with connection pooling
const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_DATABASE || "amslux",
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 5, // Reduced from 10
  queueLimit: 0,
  idleTimeout: 300000, // 5 minutes
};

class URLProcessor extends EventEmitter {
  constructor() {
    super();
    this.db = null;
    this.browser = null;
    this.isProcessing = false;
    this.processInterval = 45000; // Increased to 45 seconds
    this.maxConcurrentJobs = 2; // Reduced from 3
    this.currentJobs = 0;
    this.maxRetries = 2; // Reduced from 3
    this.retryDelay = 10000; // Increased to 10 seconds
    this.imagesDir = path.join(__dirname, "../", "service_images");

    // Memory management
    this.pagePool = [];
    this.maxPagePool = 2; // Maximum pages to keep in pool
    this.processedCount = 0;
    this.restartThreshold = 100; // Restart browser after processing 100 URLs

    // Pipeline stages
    this.pipeline = new URLProcessingPipeline();

    // Memory monitoring
    this.memoryCheckInterval = 60000; // Check memory every minute
    this.maxMemoryUsage = 512 * 1024 * 1024; // 512MB limit

    this.startMemoryMonitoring();
  }

  async initialize() {
    try {
      // Initialize database connection
      this.db = await mysql.createPool(dbConfig);
      console.log("Database connected successfully");

      // Test database connection
      const connection = await this.db.getConnection();
      await connection.ping();
      connection.release();
      console.log("Database connection verified");

      // Create images directory
      await this.createImagesDirectory();

      // Initialize browser with memory-optimized settings
      await this.initializeBrowser();

      // Initialize pipeline
      await this.pipeline.initialize(this.db, this.browser);

      console.log("✅ URLProcessor initialized successfully");
      return true;
    } catch (error) {
      console.error("❌ Failed to initialize:", error.message);
      return false;
    }
  }

  async initializeBrowser() {
    if (this.browser) {
      try {
        await this.browser.close();
      } catch (error) {
        console.error("Error closing existing browser:", error.message);
      }
    }

    this.browser = await puppeteer.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--no-first-run",
        "--no-zygote",
        "--single-process",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=TranslateUI,VizDisplayCompositor",
        "--disable-blink-features=AutomationControlled",
        "--memory-pressure-off",
        `--max-old-space-size=256`, // Reduced memory limit
        "--js-flags=--max-old-space-size=256",
      ],
      ignoreDefaultArgs: ["--disable-extensions"],
    });

    console.log("🚀 Browser initialized with memory optimization");
  }

  startMemoryMonitoring() {
    setInterval(() => {
      const memUsage = process.memoryUsage();
      const memUsageMB = Math.round(memUsage.heapUsed / 1024 / 1024);

      console.log(`📊 Memory usage: ${memUsageMB}MB`);

      if (memUsage.heapUsed > this.maxMemoryUsage) {
        console.warn(
          "⚠️ High memory usage detected, forcing garbage collection"
        );
        if (global.gc) {
          global.gc();
        }

        // If still high memory, restart browser
        if (process.memoryUsage().heapUsed > this.maxMemoryUsage) {
          this.emit("restartBrowser");
        }
      }
    }, this.memoryCheckInterval);

    // Listen for restart browser event
    this.on("restartBrowser", async () => {
      if (this.currentJobs === 0) {
        await this.restartBrowser();
      }
    });
  }

  async restartBrowser() {
    console.log("🔄 Restarting browser for memory cleanup");
    try {
      if (this.browser) {
        await this.browser.close();
      }
      await this.initializeBrowser();
      await this.pipeline.updateBrowser(this.browser);
      this.processedCount = 0;
      console.log("✅ Browser restarted successfully");
    } catch (error) {
      console.error("❌ Error restarting browser:", error.message);
    }
  }

  async createImagesDirectory() {
    try {
      await fs.mkdir(this.imagesDir, { recursive: true });
      console.log("📁 Images directory ready:", this.imagesDir);
    } catch (error) {
      console.error("Error creating images directory:", error.message);
    }
  }

  async startProcessing() {
    if (this.isProcessing) {
      console.log("⚠️ Processing already running");
      return;
    }

    this.isProcessing = true;
    console.log("🚀 URL Queue Processor started with pipeline");

    while (this.isProcessing) {
      try {
        await this.processPendingUrls();

        // Memory cleanup after each cycle
        if (global.gc) {
          global.gc();
        }

        // Check if browser needs restart
        if (
          this.processedCount >= this.restartThreshold &&
          this.currentJobs === 0
        ) {
          await this.restartBrowser();
        }

        await this.sleep(this.processInterval);
      } catch (error) {
        console.error("❌ Error in processing cycle:", error.message);
        await this.sleep(this.processInterval * 2); // Longer delay on error
      }
    }
  }

  async processPendingUrls() {
    try {
      const limit = Math.min(this.maxConcurrentJobs - this.currentJobs, 5);

      if (limit <= 0) {
        console.log("⏳ Max concurrent jobs reached, waiting...");
        return;
      }

      const [rows] = await this.db.execute(
        `SELECT id, url, category, priority, subcategory_id
   FROM product_urls 
   WHERE status = 'pending'
   ORDER BY 
     CASE priority 
       WHEN 'high' THEN 1 
       WHEN 'normal' THEN 2 
       WHEN 'low' THEN 3 
     END,
     created_at ASC
   LIMIT ${this.db.escape(limit)}`
      );

      if (rows.length === 0) {
        console.log("📭 No pending URLs found");
        return;
      }

      console.log(`📋 Found ${rows.length} URLs to process`);

      // Process URLs through pipeline
      const promises = rows.map((row) => this.pipeline.processUrl(row));
      await Promise.allSettled(promises);
    } catch (error) {
      console.error("❌ Error getting pending URLs:", error.message);
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async stop() {
    console.log("🛑 Stopping URL Queue Processor...");
    this.isProcessing = false;

    // Wait for current jobs to complete
    while (this.currentJobs > 0) {
      console.log(`⏳ Waiting for ${this.currentJobs} jobs to complete...`);
      await this.sleep(1000);
    }

    if (this.pipeline) {
      await this.pipeline.cleanup();
    }

    if (this.browser) {
      await this.browser.close();
    }

    if (this.db) {
      await this.db.end();
    }

    console.log("✅ URL Queue Processor stopped cleanly");
  }
}

// Pipeline Implementation
class URLProcessingPipeline {
  constructor() {
    this.db = null;
    this.browser = null;
    this.stages = [
      new FetchStage(),
      new ValidationStage(),
      new ExtractionStage(),
      new ImageProcessingStage(),
      new StorageStage(),
    ];
  }

  async initialize(db, browser) {
    this.db = db;
    this.browser = browser;

    for (const stage of this.stages) {
      await stage.initialize(db, browser);
    }
  }

  async updateBrowser(browser) {
    this.browser = browser;
    for (const stage of this.stages) {
      if (stage.updateBrowser) {
        await stage.updateBrowser(browser);
      }
    }
  }

  async processUrl(urlData) {
    let context = {
      urlData,
      page: null,
      html: null,
      productData: null,
      images: [],
      serviceId: null,
      startTime: Date.now(),
    };

    try {
      // Update status to processing
      await this.updateUrlStatus(urlData.id, "processing");

      // Process through each stage
      for (const stage of this.stages) {
        context = await stage.process(context);

        if (context.shouldStop) {
          console.log(
            `⏹️ Pipeline stopped at ${stage.constructor.name} for URL ${urlData.id}`
          );
          break;
        }

        // Memory cleanup between stages
        if (global.gc && Math.random() < 0.3) {
          global.gc();
        }
      }

      // Final status update
      if (context.productData && context.serviceId) {
        await this.updateUrlStatus(urlData.id, "scraped", {
          is_crawled: true,
          is_parsed: true,
          is_valid: 1,
          last_scrapped: new Date().toISOString().split("T")[0],
        });

        const processingTime = Date.now() - context.startTime;
        console.log(
          `✅ Successfully processed URL ${urlData.id} in ${processingTime}ms`
        );
      } else {
        await this.updateUrlStatus(urlData.id, "failed", { is_valid: 0 });
        console.log(`❌ Failed to process URL ${urlData.id}`);
      }
    } catch (error) {
      console.error(`❌ Pipeline error for URL ${urlData.id}:`, error.message);
      await this.updateUrlStatus(urlData.id, "failed");
    } finally {
      // Always cleanup page
      if (context.page) {
        try {
          await context.page.close();
        } catch (error) {
          console.error("Error closing page:", error.message);
        }
      }
    }

    return context;
  }

  async updateUrlStatus(id, status, additionalFields = {}) {
    try {
      let query = "UPDATE product_urls SET status = ?, updated_at = NOW()";
      let params = [status];

      Object.keys(additionalFields).forEach((field) => {
        query += `, ${field} = ?`;
        params.push(additionalFields[field]);
      });

      query += " WHERE id = ?";
      params.push(id);

      await this.db.execute(query, params);
    } catch (error) {
      console.error("❌ Error updating URL status:", error.message);
    }
  }

  async cleanup() {
    for (const stage of this.stages) {
      if (stage.cleanup) {
        await stage.cleanup();
      }
    }
  }
}

// Pipeline Stages
class FetchStage {
  constructor() {
    this.name = "Fetch";
  }

  async initialize(db, browser) {
    this.browser = browser;
  }

  async updateBrowser(browser) {
    this.browser = browser;
  }

  async process(context) {
    try {
      console.log(`🌐 Fetching URL: ${context.urlData.url}`);

      // Create page with optimized settings
      context.page = await this.browser.newPage();

      // Set minimal resources loading - but allow images for better extraction
      await context.page.setRequestInterception(true);
      context.page.on("request", (request) => {
        const resourceType = request.resourceType();
        const url = request.url();

        // Block unnecessary resources but allow images for better extraction
        if (["font", "stylesheet", "media"].includes(resourceType)) {
          request.abort();
        } else if (resourceType === "image") {
          // Allow images but limit size to prevent memory issues
          const isLikelyLargeImage =
            url.includes("original") ||
            url.includes("full") ||
            url.includes("raw") ||
            url.includes("maximum");

          if (isLikelyLargeImage) {
            // Allow but we'll handle size limits in download
            request.continue();
          } else {
            request.continue();
          }
        } else {
          request.continue();
        }
      });

      await context.page.setUserAgent(
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
      );

      // Set viewport to reduce memory usage
      await context.page.setViewport({ width: 1024, height: 768 });

      // Navigate with timeout
      const response = await Promise.race([
        context.page.goto(context.urlData.url, {
          waitUntil: "domcontentloaded", // Faster than "load"
          timeout: 25000,
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Navigation timeout")), 25000)
        ),
      ]);

      if (!response || !response.ok()) {
        throw new Error(`HTTP ${response?.status() || "Unknown"}`);
      }

      // Get HTML content
      context.html = await context.page.content();

      if (!context.html || context.html.length < 500) {
        context.shouldStop = true;
        console.log(
          `⚠️ Invalid or minimal HTML content for URL ${context.urlData.id}`
        );
        return context;
      }

      console.log(`✅ Successfully fetched ${context.html.length} characters`);
    } catch (error) {
      console.error(
        `❌ Fetch failed for URL ${context.urlData.id}:`,
        error.message
      );
      context.shouldStop = true;
    }

    return context;
  }
}

class ValidationStage {
  constructor() {
    this.name = "Validation";
  }

  async initialize(db, browser) {
    // No initialization needed
  }

  async process(context) {
    try {
      console.log(`✔️ Validating URL ${context.urlData.id}`);

      // Quick validation checks
      if (this.isBlockedPage(context.html)) {
        context.shouldStop = true;
        console.log(
          `⚠️ Blocked or error page detected for URL ${context.urlData.id}`
        );
        return context;
      }

      if (!this.hasProductIndicators(context.html)) {
        context.shouldStop = true;
        console.log(
          `⚠️ No product indicators found for URL ${context.urlData.id}`
        );
        return context;
      }

      console.log(`✅ URL ${context.urlData.id} passed validation`);
    } catch (error) {
      console.error(
        `❌ Validation failed for URL ${context.urlData.id}:`,
        error.message
      );
      context.shouldStop = true;
    }

    return context;
  }

  isBlockedPage(html) {
    const blockedIndicators = [
      "cloudflare",
      "access denied",
      "503 service",
      "404 not found",
      "500 internal server",
      "rate limit",
      "blocked",
      "captcha",
    ];

    const htmlLower = html.toLowerCase();
    return blockedIndicators.some((indicator) => htmlLower.includes(indicator));
  }

  hasProductIndicators(html) {
    const productIndicators = [
      "price",
      "buy now",
      "add to cart",
      "product",
      "description",
      "specifications",
      "$",
      "€",
      "£",
      "₹",
      "rating",
      "reviews",
    ];

    const htmlLower = html.toLowerCase();
    const foundIndicators = productIndicators.filter((indicator) =>
      htmlLower.includes(indicator)
    );

    return foundIndicators.length >= 2; // At least 2 indicators required
  }
}

class ExtractionStage {
  constructor() {
    this.name = "Extraction";
  }

  async initialize(db, browser) {
    // No initialization needed
  }

  async process(context) {
    try {
      console.log(`🔍 Extracting data from URL ${context.urlData.id}`);

      // Optimize HTML for AI processing
      const optimizedHtml = this.optimizeHtmlForAI(context.html);

      // Extract product data using AI
      const aiResult = await this.validateAndExtractProductData(optimizedHtml);

      if (!aiResult.isValid) {
        context.shouldStop = true;
        console.log(
          `⚠️ AI validation failed for URL ${context.urlData.id}: ${
            aiResult.reason || "Unknown"
          }`
        );
        return context;
      }

      context.productData = aiResult.productData;
      console.log(
        `✅ Extracted product data for: ${
          context.productData?.title || "Unknown"
        }`
      );
    } catch (error) {
      console.error(
        `❌ Extraction failed for URL ${context.urlData.id}:`,
        error.message
      );
      context.shouldStop = true;
    }

    return context;
  }

  // Replace the optimizeHtmlForAI method in ExtractionStage class
  optimizeHtmlForAI(html) {
    try {
      const $ = cheerio.load(html, {
        withDomLvl1: true,
        normalizeWhitespace: true,
        xmlMode: false,
        decodeEntities: true,
      });

      // Extract images FIRST before any modifications
      const images = [];

      // Get high-resolution images from img elements
      $("img").each((i, el) => {
        const $el = $(el);
        let src = null;

        // Priority order: try to get highest quality source
        const possibleSources = [
          $el.attr("data-original"), // Original full-size
          $el.attr("data-full"), // Full resolution
          $el.attr("data-large"), // Large version
          $el.attr("data-zoom"), // Zoom version
          $el.attr("data-hires"), // High resolution
          this.getBestFromSrcset($el.attr("srcset")), // Best from srcset
          $el.attr("src"), // Standard src
          $el.attr("data-src"), // Lazy loaded
          $el.attr("data-lazy"), // Lazy loaded alternative
        ].filter(Boolean);

        src = possibleSources[0]; // Take the first (highest priority) available

        if (src && this.isValidImageUrl(src)) {
          // Convert relative URLs to absolute
          if (src.startsWith("//")) {
            src = "https:" + src;
          } else if (src.startsWith("/")) {
            // Extract domain from current page URL if available
            // For now, skip relative URLs or you can pass base URL to this method
            return;
          }
          images.push(src);
        }
      });

      // Check for background images in style attributes
      $("[style*='background-image']").each((i, el) => {
        const style = $(el).attr("style");
        const matches = style.match(
          /background-image:\s*url\(['"]?([^'")]+)['"]?\)/
        );
        if (matches && matches[1] && this.isValidImageUrl(matches[1])) {
          let src = matches[1];
          if (src.startsWith("//")) {
            src = "https:" + src;
          }
          images.push(src);
        }
      });

      // Look for high-res data attributes in any element
      $(
        "[data-hires], [data-full], [data-zoom], [data-large], [data-original]"
      ).each((i, el) => {
        const $el = $(el);
        const possibleSources = [
          $el.attr("data-original"),
          $el.attr("data-full"),
          $el.attr("data-large"),
          $el.attr("data-zoom"),
          $el.attr("data-hires"),
        ].filter(Boolean);

        possibleSources.forEach((src) => {
          if (src && this.isValidImageUrl(src)) {
            if (src.startsWith("//")) {
              src = "https:" + src;
            }
            images.push(src);
          }
        });
      });

      // Remove duplicates while preserving order (highest quality first)
      const uniqueImages = [...new Set(images)];

      // NOW remove unwanted elements (after image extraction)
      $(
        "script, style, noscript, iframe, embed, object, " +
          'link[rel="stylesheet"], meta, ' +
          ".cookie-banner, .cookie-notice, .gdpr, " +
          ".newsletter, .popup, .modal, .overlay, " +
          ".advertisement, .ads, .ad-banner, " +
          ".comments-section, .user-comments, " +
          ".social-share, .social-media, " +
          "footer, nav:not([class*='breadcrumb'])"
      ).remove();

      // Replace images with placeholders AFTER extraction
      $("img").replaceWith("[IMAGE_PLACEHOLDER]");

      // Add extracted images info at the beginning
      if (uniqueImages.length > 0) {
        $("body").prepend(
          `<div class="extracted-images">${uniqueImages.join("|")}</div>`
        );
      }

      // Clean up attributes
      $("*").each((i, el) => {
        const $el = $(el);
        const keepAttrs = ["class", "id", "href", "alt", "title"];
        const attrs = Object.keys(el.attribs || {});

        attrs.forEach((attr) => {
          if (!keepAttrs.includes(attr)) {
            $el.removeAttr(attr);
          }
        });
      });

      // Minify output
      let cleanedHtml = $.html();
      cleanedHtml = cleanedHtml
        .replace(/\s+/g, " ")
        .replace(/>\s+</g, "><")
        .trim();

      // Truncate if too large
      const maxLength = 15000;
      if (cleanedHtml.length > maxLength) {
        cleanedHtml = cleanedHtml.substring(0, maxLength) + "...";
      }

      return cleanedHtml;
    } catch (error) {
      console.error("HTML optimization error:", error.message);
      return html.substring(0, 10000);
    }
  }

  // Add this new helper method to ExtractionStage class
  getBestFromSrcset(srcset) {
    if (!srcset) return null;

    try {
      // Parse srcset to get the largest/highest quality image
      const sources = srcset.split(",").map((s) => s.trim().split(" "));

      // Sort by width (highest first) or take the last one if no width specified
      const sortedSources = sources.sort((a, b) => {
        const widthA = parseInt(a[1]) || 0;
        const widthB = parseInt(b[1]) || 0;
        return widthB - widthA; // Descending order
      });

      return sortedSources[0] && sortedSources[0][0]
        ? sortedSources[0][0]
        : null;
    } catch (error) {
      console.error("Error parsing srcset:", error.message);
      return null;
    }
  }

  // Update the isValidImageUrl method to be more lenient for high-res images
  isValidImageUrl(url) {
    if (!url || typeof url !== "string") return false;

    try {
      // Handle relative URLs
      if (url.startsWith("//")) {
        url = "https:" + url;
      }

      new URL(url);

      // More comprehensive image format detection
      const imageFormats =
        /\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff|tif|avif|heic|ico)(\?.*)?$/i;
      const isImageFormat = imageFormats.test(url);

      // Additional checks for high-quality images
      const hasImageIndicators =
        url.includes("/images/") ||
        url.includes("/photos/") ||
        url.includes("/media/") ||
        url.includes("image") ||
        url.includes("photo") ||
        isImageFormat;

      return (
        hasImageIndicators &&
        !url.includes("data:image") && // Skip base64 images
        !url.includes("placeholder") && // Skip placeholder images
        !url.includes("loading") && // Skip loading gifs
        !url.includes("spinner") && // Skip loading spinners
        !url.includes("blank") && // Skip blank images
        url.length < 2000 // Reasonable URL length
      );
    } catch {
      return false;
    }
  }

  getBestFromSrcset(srcset) {
    if (!srcset) return null;

    try {
      // Parse srcset to get the largest/highest quality image
      const sources = srcset.split(",").map((s) => s.trim().split(" "));

      // Sort by width (highest first) or take the last one if no width specified
      const sortedSources = sources.sort((a, b) => {
        const widthA = parseInt(a[1]) || 0;
        const widthB = parseInt(b[1]) || 0;
        return widthB - widthA; // Descending order
      });

      return sortedSources[0] && sortedSources[0][0]
        ? sortedSources[0][0]
        : null;
    } catch (error) {
      console.error("Error parsing srcset:", error.message);
      return null;
    }
  }

  isValidImageUrl(url) {
    if (!url || typeof url !== "string") return false;

    try {
      // Handle relative URLs
      if (url.startsWith("//")) {
        url = "https:" + url;
      }

      new URL(url);

      // More comprehensive image format detection
      const imageFormats =
        /\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff|tif|avif|heic|ico)(\?.*)?$/i;
      const isImageFormat = imageFormats.test(url);

      // Additional checks for high-quality images
      const hasImageIndicators =
        url.includes("/images/") ||
        url.includes("/photos/") ||
        url.includes("/media/") ||
        url.includes("image") ||
        url.includes("photo") ||
        isImageFormat;

      return (
        hasImageIndicators &&
        !url.includes("data:image") && // Skip base64 images
        !url.includes("placeholder") && // Skip placeholder images
        !url.includes("loading") && // Skip loading gifs
        !url.includes("spinner") && // Skip loading spinners
        !url.includes("blank") && // Skip blank images
        url.length < 2000 // Reasonable URL length
      );
    } catch {
      return false;
    }
  }

  async validateAndExtractProductData(html) {
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `Extract product information from this webpage. 
            
Return JSON only in this format:
{
  "isValid": boolean,
  "productData": {
    "title": "string",
    "price": "string",
    "location": "string", 
    "description": "string",
    "images": ["array of image URLs from extracted-images div"],
    "tag": "single word category"
  }
}

If not a product page, return {"isValid": false, "reason": "explanation"}

Rules:
- Look for extracted-images div for image URLs (separated by |)
- Translate non-English text to English
- Keep descriptions under 200 words
- Use single word tags like: watch, shirt, car, house
- Prioritize high-quality, full-resolution image URLs
- Include up to 5 best product images`,
          },
          {
            role: "user",
            content: html,
          },
        ],
        temperature: 0.1,
        max_tokens: 800,
      });

      let content = completion.choices[0].message.content.trim();

      // Clean up response
      content = content.replace(/```json\s*|\s*```/g, "");

      const result = JSON.parse(content);

      // Validate structure
      if (typeof result.isValid !== "boolean") {
        throw new Error("Invalid response structure");
      }

      // Parse images from extracted-images div
      if (result.isValid && result.productData) {
        const imageDiv = html.match(
          /<div class="extracted-images">([^<]+)<\/div>/
        );
        if (imageDiv && imageDiv[1]) {
          result.productData.images = imageDiv[1]
            .split("|")
            .filter((url) => url.trim() && this.isValidImageUrl(url.trim()));
        }
      }

      return result;
    } catch (error) {
      console.error("AI extraction error:", error.message);
      return {
        isValid: false,
        reason: "AI processing failed",
        productData: null,
      };
    }
  }
}

class ImageProcessingStage {
  constructor() {
    this.name = "ImageProcessing";
    this.imagesDir = path.join(__dirname, "../", "service_images");
  }

  async initialize(db, browser) {
    await fs.mkdir(this.imagesDir, { recursive: true });
  }

  async process(context) {
    try {
      if (
        !context.productData?.images ||
        context.productData.images.length === 0
      ) {
        console.log(`⚠️ No images to process for URL ${context.urlData.id}`);
        return context;
      }

      console.log(
        `🖼️ Processing ${context.productData.images.length} images for URL ${context.urlData.id}`
      );

      // Limit number of images to download (save bandwidth and storage)
      const maxImages = 5;
      const imagesToProcess = context.productData.images.slice(0, maxImages);

      context.images = await this.downloadImages(
        imagesToProcess,
        context.urlData.id
      );

      console.log(`✅ Downloaded ${context.images.length} images`);
    } catch (error) {
      console.error(
        `❌ Image processing failed for URL ${context.urlData.id}:`,
        error.message
      );
      // Don't stop pipeline for image failures
      context.images = [];
    }

    return context;
  }

  async downloadImages(imageUrls, urlId) {
    const downloadedImages = [];

    // Remove duplicates while preserving order
    const uniqueUrls = [...new Set(imageUrls)];

    console.log(
      `🖼️ Attempting to download ${uniqueUrls.length} unique images...`
    );

    for (let i = 0; i < uniqueUrls.length; i++) {
      try {
        const imageUrl = uniqueUrls[i];

        // Generate filename with original extension if possible
        const urlPath = new URL(imageUrl).pathname;
        const originalExt = path.extname(urlPath).toLowerCase() || ".jpg";
        const fileName = `service_${urlId}_${
          i + 1
        }_${Date.now()}${originalExt}`;
        const localPath = path.join(this.imagesDir, fileName);

        console.log(
          `📥 Downloading image ${i + 1}/${uniqueUrls.length}: ${imageUrl}`
        );
        const success = await this.downloadImage(imageUrl, localPath);

        if (success) {
          downloadedImages.push({
            originalUrl: imageUrl,
            localPath: `service_images/${fileName}`,
            fileName: fileName,
          });

          console.log(`✅ Successfully downloaded: ${fileName}`);
        } else {
          console.log(`❌ Failed to download image ${i + 1}`);
        }
      } catch (error) {
        console.error(`❌ Error downloading image ${i + 1}:`, error.message);
      }
    }

    console.log(
      `📊 Downloaded ${downloadedImages.length}/${uniqueUrls.length} images successfully`
    );
    return downloadedImages;
  }

  downloadImage(url, localPath) {
    return new Promise((resolve) => {
      try {
        const protocol = url.startsWith("https:") ? https : http;

        const request = protocol.get(url, (response) => {
          // Handle redirects
          if (
            response.statusCode >= 300 &&
            response.statusCode < 400 &&
            response.headers.location
          ) {
            console.log(
              `🔄 Following redirect for image: ${response.headers.location}`
            );
            return this.downloadImage(
              response.headers.location,
              localPath
            ).then(resolve);
          }

          if (response.statusCode === 200) {
            const fileStream = require("fs").createWriteStream(localPath);

            // Get full size image without compression
            response.pipe(fileStream);

            fileStream.on("finish", () => {
              fileStream.close();

              // Verify file was downloaded and has reasonable size
              require("fs").stat(localPath, (err, stats) => {
                if (err || stats.size < 1024) {
                  // Less than 1KB probably failed
                  console.log(
                    `⚠️ Downloaded image too small or corrupted: ${
                      stats?.size || 0
                    } bytes`
                  );
                  resolve(false);
                } else {
                  console.log(
                    `✅ Downloaded full-size image: ${Math.round(
                      stats.size / 1024
                    )}KB`
                  );
                  resolve(true);
                }
              });
            });

            fileStream.on("error", (err) => {
              console.error("File write error:", err.message);
              resolve(false);
            });
          } else {
            console.error(
              `Failed to download image: HTTP ${response.statusCode}`
            );
            resolve(false);
          }
        });

        request.on("error", (err) => {
          console.error("Download error:", err.message);
          resolve(false);
        });

        // Increased timeout for larger images
        request.setTimeout(30000, () => {
          request.destroy();
          console.error("Download timeout (30s) - image might be very large");
          resolve(false);
        });
      } catch (error) {
        console.error("Download setup error:", error.message);
        resolve(false);
      }
    });
  }
}

class StorageStage {
  constructor() {
    this.name = "Storage";
  }

  async initialize(db, browser) {
    this.db = db;
  }

  async process(context) {
    try {
      console.log(`💾 Storing data for URL ${context.urlData.id}`);

      const serviceId = await this.createCategoryService(context);
      context.serviceId = serviceId;

      if (context.images.length > 0) {
        await this.saveImageGallery(serviceId, context.images);
      }

      console.log(`✅ Data stored successfully with service ID: ${serviceId}`);
    } catch (error) {
      console.error(
        `❌ Storage failed for URL ${context.urlData.id}:`,
        error.message
      );
      context.shouldStop = true;
    }

    return context;
  }

  async createCategoryService(context) {
    const { urlData, productData, images } = context;

    try {
      // Extract price
      const extractedPrice = this.extractPrice(productData.price);

      // Use first downloaded image or fallback to original
      const primaryImageUrl =
        images.length > 0
          ? images[0].localPath
          : productData.images?.[0] || null;

      const aboutDescription =
        productData.description?.length > 100
          ? productData.description.slice(0, 100) + "..."
          : productData.description || `Quality ${productData.tag} product`;

      const [result] = await this.db.execute(
        `INSERT INTO category_services (
          category_id, sub_category_id, service_name, title, about_description,
          description, imageUrl, tag, service_type, startingPrice, location,
          source_url, stock_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'normal', ?, ?, ?, 'in_stock', NOW(), NOW())`,
        [
          urlData.category,
          urlData.subcategory_id,
          productData.title,
          productData.title,
          aboutDescription,
          productData.description || `Premium ${productData.tag} available`,
          primaryImageUrl,
          productData.tag,
          extractedPrice,
          productData.location,
          urlData.url,
        ]
      );

      return result.insertId;
    } catch (error) {
      console.error("Error creating category service:", error.message);
      throw error;
    }
  }

  async saveImageGallery(serviceId, downloadedImages) {
    try {
      for (const image of downloadedImages) {
        await this.db.execute(
          `INSERT INTO service_image_galleries (service_id, image_url, created_at, updated_at) 
           VALUES (?, ?, NOW(), NOW())`,
          [serviceId, image.localPath]
        );
      }
    } catch (error) {
      console.error("Error saving image gallery:", error.message);
    }
  }

  extractPrice(priceString) {
    if (!priceString) return null;

    const matches = priceString.match(/[\d,]+\.?\d*/);
    return matches ? parseFloat(matches[0].replace(/,/g, "")) : null;
  }
}

// Main execution with enhanced error handling
const processor = new URLProcessor();

async function main() {
  // Handle uncaught exceptions
  process.on("uncaughtException", (error) => {
    console.error("💥 Uncaught Exception:", error.message);
    process.exit(1);
  });

  process.on("unhandledRejection", (reason, promise) => {
    console.error("💥 Unhandled Rejection at:", promise, "reason:", reason);
    process.exit(1);
  });

  const initialized = await processor.initialize();

  if (!initialized) {
    console.error("❌ Failed to initialize processor");
    process.exit(1);
  }

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\n🛑 Received shutdown signal");
    await processor.stop();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  // Start processing
  console.log("🚀 Starting URL processing with optimized pipeline");
  await processor.startProcessing();
}

// Additional utility functions and optimizations
class MemoryManager {
  static forceGarbageCollection() {
    if (global.gc) {
      global.gc();
    }
  }

  static getMemoryUsage() {
    const usage = process.memoryUsage();
    return {
      heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
      external: Math.round(usage.external / 1024 / 1024),
      rss: Math.round(usage.rss / 1024 / 1024),
    };
  }

  static isMemoryHigh(threshold = 400) {
    const usage = this.getMemoryUsage();
    return usage.heapUsed > threshold;
  }
}

// Database connection manager with retry logic
class DatabaseManager {
  constructor(config) {
    this.config = config;
    this.pool = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
  }

  async createPool() {
    try {
      this.pool = await mysql.createPool({
        ...this.config,
        reconnect: true,
        acquireTimeout: 60000,
        timeout: 60000,
      });

      // Test connection
      const connection = await this.pool.getConnection();
      await connection.ping();
      connection.release();

      console.log("✅ Database pool created successfully");
      this.reconnectAttempts = 0;
      return this.pool;
    } catch (error) {
      console.error("❌ Database pool creation failed:", error.message);

      if (this.reconnectAttempts < this.maxReconnectAttempts) {
        this.reconnectAttempts++;
        console.log(
          `🔄 Retrying database connection (${this.reconnectAttempts}/${this.maxReconnectAttempts})`
        );
        await this.sleep(5000 * this.reconnectAttempts);
        return this.createPool();
      }

      throw error;
    }
  }

  async execute(query, params) {
    try {
      return await this.pool.execute(query, params);
    } catch (error) {
      if (error.code === "PROTOCOL_CONNECTION_LOST") {
        console.log("🔄 Database connection lost, recreating pool...");
        await this.createPool();
        return await this.pool.execute(query, params);
      }
      throw error;
    }
  }

  async end() {
    if (this.pool) {
      await this.pool.end();
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Rate limiter for API calls
class RateLimiter {
  constructor(maxCalls = 10, timeWindow = 60000) {
    this.maxCalls = maxCalls;
    this.timeWindow = timeWindow;
    this.calls = [];
  }

  async waitIfNeeded() {
    const now = Date.now();

    // Remove old calls outside the time window
    this.calls = this.calls.filter((call) => now - call < this.timeWindow);

    if (this.calls.length >= this.maxCalls) {
      const oldestCall = Math.min(...this.calls);
      const waitTime = this.timeWindow - (now - oldestCall);

      if (waitTime > 0) {
        console.log(`⏳ Rate limit reached, waiting ${waitTime}ms`);
        await this.sleep(waitTime);
      }
    }

    this.calls.push(now);
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Enhanced error handling and retry mechanism
class RetryManager {
  static async withRetry(fn, maxRetries = 3, baseDelay = 1000, context = "") {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await fn();
      } catch (error) {
        const isLastAttempt = attempt === maxRetries;

        if (isLastAttempt) {
          console.error(
            `❌ ${context} failed after ${maxRetries} attempts:`,
            error.message
          );
          throw error;
        }

        const delay = baseDelay * Math.pow(2, attempt - 1); // Exponential backoff
        console.log(
          `⚠️ ${context} failed (attempt ${attempt}/${maxRetries}), retrying in ${delay}ms...`
        );

        await this.sleep(delay);
      }
    }
  }

  static sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Health check endpoint for monitoring
class HealthMonitor {
  constructor(processor) {
    this.processor = processor;
    this.startTime = Date.now();
    this.stats = {
      processed: 0,
      failed: 0,
      errors: [],
    };
  }

  getHealthStatus() {
    const memUsage = MemoryManager.getMemoryUsage();
    const uptime = Date.now() - this.startTime;

    return {
      status: "healthy",
      uptime: Math.floor(uptime / 1000),
      memory: memUsage,
      processed: this.stats.processed,
      failed: this.stats.failed,
      isProcessing: this.processor.isProcessing,
      currentJobs: this.processor.currentJobs,
      browserConnected: !!this.processor.browser,
      lastError: this.stats.errors[this.stats.errors.length - 1],
    };
  }

  recordSuccess() {
    this.stats.processed++;
  }

  recordFailure(error) {
    this.stats.failed++;
    this.stats.errors.push({
      message: error.message,
      timestamp: new Date().toISOString(),
    });

    // Keep only last 10 errors
    if (this.stats.errors.length > 10) {
      this.stats.errors = this.stats.errors.slice(-10);
    }
  }
}

main().catch(console.error);
