require("dotenv").config();
const puppeteer = require("puppeteer");
const cheerio = require("cheerio");
const OpenAI = require("openai");
const mysql = require("mysql2/promise");
const fs = require("fs").promises;
const fsSync = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");
const AICostTrackerService = require("./services/AICostTrackerService");

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_DATABASE || "amslux",
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
};

class URLProcessor {
  constructor() {
    this.db = null;
    this.browser = null;
    this.isProcessing = false;
    this.processInterval = 30000; // 30 seconds between cycles
    this.maxConcurrentJobs = 2;
    this.currentJobs = 0;
    this.maxRetries = 3;
    this.retryDelay = 5000; // 5 seconds
    this.imagesDir = path.join(__dirname, "../", "service_images");
    this.apiPaused = false;
    this.apiPauseUntil = null;
    this.apiRetryDelay = 60000; // 1 minute initial delay
    this.maxApiRetryDelay = 3600000;

    // Browser restart configuration
    this.browserStartTime = null;
    this.maxBrowserUptime = 1 * 60 * 60 * 1000; // 2 hours in milliseconds
    this.urlsProcessedSinceRestart = 0;
    this.maxUrlsBeforeRestart = 50; // Restart after processing 100 URLs
    this.restartingBrowser = false;
  }

  async initialize() {
    try {
      // Initialize database connection
      this.db = mysql.createPool(dbConfig);
      console.log("Database connected successfully");

      // Create images directory if it doesn't exist
      await this.createImagesDirectory();

      // Initialize browser
      await this.initializeBrowser();

      console.log("Processor initialized successfully");
      return true;
    } catch (error) {
      console.error("Failed to initialize:", error.message);
      return false;
    }
  }

  async initializeBrowser() {
    if (this.browser) {
      try {
        await this.browser.close();
      } catch (error) {
        console.log("Error closing existing browser:", error.message);
      }
    }

    this.browser = await puppeteer.launch({
      headless: true,
      args: [
        "--disable-gpu",
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--disable-default-apps",
        "--disable-dev-shm-usage",
        "--disable-setuid-sandbox",
        "--single-process",
        "--no-zygote",
        "--memory-pressure-off", // Disable memory pressure notifications
        "--max_old_space_size=4096", // Increase memory limit
      ],
    });

    this.browserStartTime = Date.now();
    this.urlsProcessedSinceRestart = 0;
    console.log("🚀 Browser initialized successfully");
  }

  async shouldRestartBrowser() {
    const now = Date.now();
    const uptime = now - this.browserStartTime;

    // Restart conditions:
    // 1. Browser has been running for more than maxBrowserUptime
    // 2. Processed more than maxUrlsBeforeRestart URLs
    // 3. No active jobs (to avoid interrupting ongoing processes)

    const uptimeExceeded = uptime > this.maxBrowserUptime;
    const urlCountExceeded =
      this.urlsProcessedSinceRestart >= this.maxUrlsBeforeRestart;
    const noActiveJobs = this.currentJobs === 0;

    return (
      (uptimeExceeded || urlCountExceeded) &&
      noActiveJobs &&
      !this.restartingBrowser
    );
  }

  async restartBrowserIfNeeded() {
    if (await this.shouldRestartBrowser()) {
      this.restartingBrowser = true;

      const uptime = Math.round(
        (Date.now() - this.browserStartTime) / 1000 / 60,
      ); // minutes
      console.log(
        `🔄 Restarting browser after ${uptime} minutes uptime and ${this.urlsProcessedSinceRestart} URLs processed`,
      );

      try {
        await this.initializeBrowser();
        console.log("✅ Browser restarted successfully");
      } catch (error) {
        console.error("❌ Failed to restart browser:", error.message);
        // Try to continue with existing browser if restart fails
      }

      this.restartingBrowser = false;
    }
  }

  async createImagesDirectory() {
    try {
      await fs.mkdir(this.imagesDir, { recursive: true });
      console.log("Images directory created:", this.imagesDir);
    } catch (error) {
      console.error("Error creating images directory:", error.message);
    }
  }

  async startProcessing() {
    if (this.isProcessing) {
      console.log("Processing already running");
      return;
    }

    this.isProcessing = true;
    console.log("🚀 URL Queue Processor started");

    while (this.isProcessing) {
      try {
        // Check if browser needs restart before processing
        await this.restartBrowserIfNeeded();

        await this.processPendingUrls();
        await this.sleep(this.processInterval);
      } catch (error) {
        console.error("Error in processing cycle:", error.message);
        if (
          error.message.includes("Browser") ||
          error.message.includes("Target closed")
        ) {
          console.log("🔄 Browser error detected, attempting restart...");
          try {
            await this.initializeBrowser();
          } catch (restartError) {
            console.error("Failed to restart browser:", restartError.message);
          }
        }

        await this.sleep(this.processInterval);
      }
    }
  }

  async processPendingUrls() {
    try {
      if (this.restartingBrowser) {
        console.log("⏸️ Skipping cycle - browser is restarting");
        return;
      }

      // Check if API is paused
      const canProceed = this.checkApiStatus();
      if (!canProceed) {
        return; // Skip this cycle
      }
      const limit = this.maxConcurrentJobs - this.currentJobs;
      if (limit <= 0) return;

      const [rows] = await this.db.execute(
        `
      SELECT id, url, category, priority, manual_html
      FROM product_urls 
      WHERE status = 'pending'
      ORDER BY 
        CASE priority 
          WHEN 'high' THEN 1 
          WHEN 'normal' THEN 2 
          WHEN 'low' THEN 3 
        END,
        created_at ASC
      LIMIT ${Number(limit)}
      `,
      );

      if (rows.length === 0) {
        console.log("No pending URLs found");
        return;
      }

      console.log(`Found ${rows.length} URLs to process`);

      await Promise.all(rows.map((row) => this.processUrl(row)));
    } catch (error) {
      console.error("Error getting pending URLs:", error.message);
    }
  }

  async handleOpenAIError(error) {
    const errorMessage = error.message || "";

    if (
      error.status === 429 ||
      errorMessage.includes("rate_limit_exceeded") ||
      errorMessage.includes("quota_exceeded") ||
      errorMessage.includes("insufficient_quota")
    ) {
      console.log("🚨 OpenAI API limit exceeded - pausing processing");
      this.apiPaused = true;

      // Extract retry delay from headers or use default
      let retryAfter = this.apiRetryDelay;
      if (error.headers && error.headers["retry-after"]) {
        retryAfter = parseInt(error.headers["retry-after"]) * 1000;
      }

      // Cap the retry delay
      retryAfter = Math.min(retryAfter, this.maxApiRetryDelay);

      this.apiPauseUntil = Date.now() + retryAfter;
      console.log(
        `⏸️ API paused until: ${new Date(this.apiPauseUntil).toLocaleString()}`,
      );

      // Double the delay for next time (exponential backoff)
      this.apiRetryDelay = Math.min(
        this.apiRetryDelay * 2,
        this.maxApiRetryDelay,
      );

      return false;
    }

    return true; // Continue processing for other errors
  }

  checkApiStatus() {
    if (this.apiPaused) {
      if (Date.now() >= this.apiPauseUntil) {
        console.log("✅ API pause period ended - resuming processing");
        this.apiPaused = false;
        this.apiPauseUntil = null;
        // Reset delay on successful resume
        this.apiRetryDelay = 60000;
        return true;
      }

      const remainingTime = Math.ceil((this.apiPauseUntil - Date.now()) / 1000);
      console.log(`⏸️ API still paused - ${remainingTime}s remaining`);
      return false;
    }

    return true;
  }

  async processUrl(urlData, retryCount = 0) {
    this.currentJobs++;
    const { id, url, manual_html } = urlData;
    let page = null;
    let shouldRetry = false;

    try {
      console.log(
        `🔄 Processing URL ${id}: ${url} (Attempt ${retryCount + 1})`,
      );
      await this.updateUrlStatus(id, "processing");

      let result;

      if (manual_html && manual_html.trim().length > 0) {
        console.log(`📋 Using manual HTML for URL ${id} - skipping Puppeteer`);
        result = await this.scrapeUrlWithManualHtml(manual_html, id);
      } else {
        page = await this.browser.newPage();
        page.setDefaultTimeout(30000);
        page.setDefaultNavigationTimeout(30000);
        await page.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) " +
            "Chrome/124.0.0.0 Safari/537.36",
        );
        result = await this.scrapeUrl(page, url, id);
      }

      if (result && result.apiPaused) {
        await this.updateUrlStatus(id, "pending");
        console.log(`⏸️ URL ${id} returned to pending due to API pause`);
        return;
      }

      if (result && result.isValid) {
        // Throws on DB failure — won't mark scraped if save failed
        const saved = await this.saveScrapedData(id, result.productData);
        if (saved) {
          await this.updateUrlStatus(id, "scraped", {
            is_crawled: true,
            is_parsed: true,
            is_valid: 1,
            last_scrapped: new Date().toISOString().split("T")[0],
          });
          console.log(`✅ Successfully processed URL ${id}`);
          this.urlsProcessedSinceRestart++;
        } else {
          await this.updateUrlStatus(id, "failed");
          console.log(
            `❌ URL ${id}: save returned false (missing title or data)`,
          );
        }
      } else {
        await this.updateUrlStatus(id, "failed", { is_valid: 0 });
        console.log(`❌ URL ${id} is not a valid product page`);
      }
    } catch (error) {
      console.error(
        `Error processing URL ${id} (Attempt ${retryCount + 1}):`,
        error.message,
      );

      if (retryCount < this.maxRetries) {
        // Signal finally to skip currentJobs-- so the retry call can claim the slot
        shouldRetry = true;
        console.log(
          `🔄 Retrying URL ${id} in ${this.retryDelay / 1000} seconds...`,
        );
      } else {
        await this.updateUrlStatus(id, "failed");
        console.log(
          `❌ Failed to process URL ${id} after ${retryCount + 1} attempts`,
        );
      }
    } finally {
      if (page) {
        try {
          await page.close();
        } catch (e) {
          console.log("Error closing page:", e.message);
        }
      }
      // Only release the job slot when NOT handing off to a retry.
      // The retry call will manage its own slot via currentJobs++ at its start.
      if (!shouldRetry) {
        this.currentJobs--;
      }
    }

    // After finally — retry here so currentJobs is still 1 (this call's slot)
    // then the recursive call increments to 1 and decrements back cleanly on finish.
    if (shouldRetry) {
      await this.sleep(this.retryDelay);
      // Hand off: decrement this call's slot, recursive call will increment its own
      this.currentJobs--;
      return this.processUrl(urlData, retryCount + 1);
    }
  }

  async getBrowserMemoryUsage() {
    try {
      if (!this.browser) return null;

      const pages = await this.browser.pages();
      let totalMemory = 0;

      for (const page of pages) {
        try {
          const metrics = await page.metrics();
          totalMemory += metrics.JSHeapUsedSize || 0;
        } catch (error) {
          // Ignore errors for closed pages
        }
      }

      return {
        totalMemoryMB: Math.round(totalMemory / 1024 / 1024),
        pageCount: pages.length,
        uptimeMinutes: Math.round(
          (Date.now() - this.browserStartTime) / 1000 / 60,
        ),
      };
    } catch (error) {
      console.log("Error getting browser memory usage:", error.message);
      return null;
    }
  }
  async logBrowserStats() {
    const stats = await this.getBrowserMemoryUsage();
    if (stats) {
      console.log(
        `📊 Browser Stats: ${stats.totalMemoryMB}MB memory, ${stats.pageCount} pages, ${stats.uptimeMinutes}min uptime, ${this.urlsProcessedSinceRestart} URLs processed`,
      );
    }
  }

  // Add this to your processPendingUrls method to log stats periodically
  async processPendingUrlsWithStats() {
    // Log browser stats every 10 cycles (approximately every 5 minutes)
    if (this.urlsProcessedSinceRestart % 10 === 0) {
      await this.logBrowserStats();
    }

    return this.processPendingUrls();
  }

  async scrapeUrlWithManualHtml(manual_html, urlId) {
    try {
      console.log(`🔄 Processing manual HTML for URL ${urlId}`);

      const optimizedHtml = this.optimizeHtmlForAI(manual_html);

      // Single AI call that both validates and extracts data
      const aiResult = await this.validateAndExtractProductData(optimizedHtml);

      return aiResult;
    } catch (error) {
      console.error("Manual HTML processing error:", error.message);
      return { isValid: false, productData: null };
    }
  }

  async scrapeUrl(page, url, urlId) {
    // Block images, fonts, stylesheets — not needed for text extraction
    // This speeds up page load and reduces noise in the HTML sent to AI
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const type = req.resourceType();
      if (["image", "stylesheet", "font", "media"].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    // Let navigation errors throw — outer processUrl catch handles retries
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    const html = await page.content();

    // Basic check for blocked/error pages — these are not retryable, return invalid
    if (
      !html ||
      html.length < 1000 ||
      html.includes("Cloudflare") ||
      html.includes("Access denied")
    ) {
      console.log(`❌ Blocked or empty page for URL ${urlId}`);
      return { isValid: false, productData: null };
    }

    const optimizedHtml = this.optimizeHtmlForAI(html);
    return await this.validateAndExtractProductData(optimizedHtml);
  }

  // OPTIMIZED: Single AI call for both validation and extraction
  async validateAndExtractProductData(html) {
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4.1-mini",
        messages: [
          {
            role: "system",
            content: `
              You are an expert at identifying and extracting product data from website content.
              
              First, determine if this page contains a single identifiable listing with a title and some details — this includes products, services, rentals, restaurants, hotels, experiences, events, or anything that can be booked, purchased, or enquired about.

              If it's NOT a valid listing (e.g. generic homepage, pure category/search results page with no single item focus, 404 error page, or plain blog article with nothing to purchase/book), return:
              {"isValid": false, "reason": "brief reason why not valid"}
              
              If it IS a valid product page, extract the data and return:
              {
                "isValid": true,
                "productData": {
                  "title": "product title",
                  "price": 1500.00,
                  "location": "location details",
                  "size":"s,m,l (like this comma seperated if multiple)",
                  "description": "product description",
                  "images": ["list", "of", "image", "urls"],
                  "tag": "single product type like: watch, shirt, sneaker, etc"
                }
              }

              - Always output valid JSON only
              - "price" must be a plain numeric value in USD with no currency symbols or text (e.g. 1500.00 not "$1,500").
                Step 1: Find the price and identify its currency (USD, EUR, GBP, AED, SAR, etc.)
                Step 2: If not already USD, convert to USD using approximate exchange rates:
                  EUR → multiply by 1.08
                  GBP → multiply by 1.27
                  AED → multiply by 0.27
                  SAR → multiply by 0.27
                  CAD → multiply by 0.74
                  AUD → multiply by 0.65
                  CHF → multiply by 1.13
                  JPY → multiply by 0.0067
                  CNY → multiply by 0.14
                  INR → multiply by 0.012
                  For any other currency, use your best estimate of the USD exchange rate.
                Step 3: Return only the final USD number. If no price found, use null.
              - If text is not in English, translate it to natural English
              - Ensure all productData fields are in English
              - Only respond with the JSON object, no additional text
            `,
          },
          {
            role: "user",
            content: html,
          },
        ],
        temperature: 0.1,
      });
      AICostTrackerService.trackOpenAI(
        "gpt-4.1-nano",
        "validate_and_extract",
        completion,
      );
      console.log(completion.choices[0].message);
      let content = completion.choices[0].message.content.trim();

      // Clean up markdown code blocks if present
      if (content.startsWith("```json")) {
        content = content.replace(/```json\s*/i, "").replace(/```\s*$/, "");
      } else if (content.startsWith("```")) {
        content = content.replace(/```\s*/, "").replace(/```\s*$/, "");
      }

      const result = JSON.parse(content);

      // Validate the response structure
      if (typeof result.isValid !== "boolean") {
        throw new Error("Invalid AI response structure");
      }

      return result;
    } catch (error) {
      console.error("Error in AI validation and extraction:", error.message);

      // Handle API rate limiting
      const shouldContinue = await this.handleOpenAIError(error);
      if (!shouldContinue) {
        return {
          isValid: false,
          productData: null,
          reason: "API rate limit - processing paused",
          apiPaused: true,
        };
      }

      // For all other errors (JSON parse, network, etc.) — throw so processUrl retries
      throw error;
    }
  }

  async saveScrapedData(urlId, productData) {
    if (!productData || !productData.title) {
      console.warn(`⚠️ URL ${urlId}: productData missing title, skipping save`);
      return false;
    }

    // Download images locally first
    const downloadedImages = await this.downloadImages(
      productData.images,
      urlId,
    );
    productData.downloadedImages = downloadedImages;

    return await this.createCategoryService(urlId, productData);
  }

  async downloadImages(imageUrls, urlId) {
    if (!imageUrls || imageUrls.length === 0) return [];

    const downloadedImages = [];

    for (let i = 0; i < imageUrls.length && i < 10; i++) {
      try {
        const imageUrl = imageUrls[i];
        if (!this.isValidImageUrl(imageUrl)) continue;

        const fileName = `service_${urlId}_${i + 1}_${Date.now()}.jpg`;
        const localPath = path.join(this.imagesDir, fileName);

        const success = await this.downloadImage(imageUrl, localPath);
        if (success) {
          const relativePath = `service_images/${fileName}`;
          downloadedImages.push({
            originalUrl: imageUrl,
            localPath: relativePath,
            fileName: fileName,
          });
          console.log(
            `📥 Downloaded image ${i + 1}/${imageUrls.length}: ${fileName}`,
          );
        }
      } catch (error) {
        console.error(`Error downloading image ${i + 1}:`, error.message);
      }
    }

    return downloadedImages;
  }

  isValidImageUrl(url) {
    if (!url || typeof url !== "string") return false;

    // Convert relative URLs to absolute
    if (url.startsWith("//")) {
      url = "https:" + url;
    } else if (url.startsWith("/")) {
      return false; // Skip relative URLs without domain
    }

    try {
      new URL(url);
      return url.match(/\.(jpg|jpeg|png|gif|webp)(\?.*)?$/i) !== null;
    } catch {
      return false;
    }
  }

  async downloadImage(url, localPath) {
    return new Promise((resolve) => {
      const protocol = url.startsWith("https:") ? https : http;

      const request = protocol.get(url, (response) => {
        if (response.statusCode === 200) {
          const fileStream = fsSync.createWriteStream(localPath);
          response.pipe(fileStream);

          fileStream.on("finish", () => {
            fileStream.close();
            resolve(true);
          });

          fileStream.on("error", (err) => {
            console.error("File write error:", err.message);
            resolve(false);
          });
        } else {
          console.error(`Failed to download image: ${response.statusCode}`);
          resolve(false);
        }
      });

      request.on("error", (err) => {
        console.error("Download error:", err.message);
        resolve(false);
      });

      request.setTimeout(10000, () => {
        request.destroy();
        console.error("Download timeout");
        resolve(false);
      });
    });
  }

  async createCategoryService(urlId, productData) {
    const { title, price, location, size, description, tag, downloadedImages } =
      productData;

    // Extract price information
    const extractedPrice = this.extractPrice(price);

    // Get the source URL and category info from product_urls table
    const [urlRow] = await this.db.execute(
      "SELECT url, category, subcategory_id FROM product_urls WHERE id = ?",
      [urlId],
    );
    const sourceUrl = urlRow.length > 0 ? urlRow[0].url : null;
    const categoryId = urlRow.length > 0 ? urlRow[0].category : null;
    const subCategoryId = urlRow.length > 0 ? urlRow[0].subcategory_id : null;

    // Check if similar service already exists
    const [existing] = await this.db.execute(
      "SELECT id FROM category_services WHERE tag = ? AND title = ?",
      [tag, title],
    );

    if (existing.length > 0) {
      console.log(`🔄 Service already exists for: ${title}`);
      return true;
    }

    // Use first downloaded image as primary image, fallback to original URL if no downloads
    const primaryImageUrl =
      downloadedImages && downloadedImages.length > 0
        ? downloadedImages[0].localPath
        : productData.images && productData.images.length > 0
          ? productData.images[0]
          : null;

    const safeDescription = description || `High-quality ${tag} product`;
    const about_description =
      safeDescription.length > 100
        ? safeDescription.slice(0, 100) + "..."
        : safeDescription;

    // Create new category service
    const [result] = await this.db.execute(
      `
      INSERT INTO category_services (
        category_id, sub_category_id, service_name, title, about_description,
        description, imageUrl, tag, service_type, startingPrice, location,
        sizes, source_url, stock_status, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'normal', ?, ?, ?, ?, 'in_stock', NOW(), NOW())
    `,
      [
        categoryId,
        subCategoryId,
        title,
        title,
        about_description,
        safeDescription,
        primaryImageUrl,
        tag,
        extractedPrice,
        location,
        size,
        sourceUrl,
      ],
    );

    const serviceId = result.insertId;

    // Store additional images in service_image_galleries table
    if (downloadedImages && downloadedImages.length > 0) {
      await this.saveImageGallery(serviceId, downloadedImages);
    }

    console.log(
      `📦 Created category service for: ${title} (${tag}) with ${
        downloadedImages ? downloadedImages.length : 0
      } images`,
    );

    return true;
  }

  async saveImageGallery(serviceId, downloadedImages) {
    try {
      for (const image of downloadedImages) {
        await this.db.execute(
          `
          INSERT INTO service_image_galleries (
            service_id, image_url, created_at, updated_at
          ) VALUES (?, ?, NOW(), NOW())
        `,
          [serviceId.toString(), image.localPath],
        );
      }

      console.log(
        `🖼️  Saved ${downloadedImages.length} images to gallery for service ${serviceId}`,
      );
    } catch (error) {
      console.error("Error saving image gallery:", error.message);
    }
  }

  extractPrice(price) {
    if (price === null || price === undefined || price === "") return null;

    // AI now returns a number directly
    if (typeof price === "number") {
      return isFinite(price) && price >= 0 ? price : null;
    }

    const str = String(price).trim();

    // Strip currency symbols and whitespace
    const cleaned = str.replace(/[^0-9.,]/g, "");
    if (!cleaned) return null;

    // Detect European format: "1.500,50" (dot=thousands, comma=decimal)
    // vs standard format: "1,500.50" (comma=thousands, dot=decimal)
    const lastDot = cleaned.lastIndexOf(".");
    const lastComma = cleaned.lastIndexOf(",");

    let normalized;
    if (lastComma > lastDot) {
      // European: "1.500,50" → remove dots, replace comma with dot
      normalized = cleaned.replace(/\./g, "").replace(",", ".");
    } else {
      // Standard: "1,500.50" → remove commas
      normalized = cleaned.replace(/,/g, "");
    }

    const value = parseFloat(normalized);
    return isFinite(value) && value >= 0 ? value : null;
  }

  async updateUrlStatus(id, status, additionalFields = {}) {
    try {
      let query = "UPDATE product_urls SET status = ?, updated_at = NOW()";
      let params = [status];

      // Add additional fields to update
      Object.keys(additionalFields).forEach((field) => {
        query += `, ${field} = ?`;
        params.push(additionalFields[field]);
      });

      query += " WHERE id = ?";
      params.push(id);

      await this.db.execute(query, params);
    } catch (error) {
      console.error("Error updating URL status:", error.message);
    }
  }

  // Include the existing optimization and extraction methods
  optimizeHtmlForAI(html) {
    const $ = cheerio.load(html);
    const extractedImages = [];

    this.extractBackgroundImages($, extractedImages);

    $(
      "script, style, noscript, iframe, embed, object, svg, " +
        'link[rel="stylesheet"], meta, head, ' +
        ".cookie-banner, .cookie-notice, .gdpr, " +
        ".newsletter, .popup, .modal, .overlay, " +
        ".social-share, .social-media, .social-icons, " +
        ".advertisement, .ads, .ad-banner, .google-ads, " +
        ".tracking, .analytics, " +
        "nav.breadcrumb, .breadcrumbs, " +
        "footer, .footer-content, " +
        ".sidebar-ads, .recommended, .suggestions, " +
        ".comments-section, .user-comments",
    ).remove();

    $("img").each((_, el) => {
      const $el = $(el);
      let src =
        $el.attr("src") || $el.attr("data-src") || $el.attr("data-lazy");

      if (src && src.startsWith("data:image/")) {
        // base64 image → remove it
        $el.remove();
        return;
      }

      const alt = $el.attr("alt") || "";
      if (src) {
        extractedImages.push({
          type: "img",
          url: src,
          alt: alt,
          context: "Regular image tag",
        });

        $el.replaceWith(`[IMAGE: ${alt || "Product image"} - ${src}]`);
      } else {
        $el.remove();
      }
    });

    $(
      '[style*="display:none"], [style*="display: none"], [hidden], .hidden, .sr-only, .visually-hidden',
    ).remove();

    $("nav").each((_, el) => {
      const navText = $(el).text().toLowerCase();
      if (
        !navText.includes("watch") &&
        !navText.includes("product") &&
        !navText.includes("category") &&
        !navText.includes("property") &&
        !navText.includes("villa") &&
        !navText.includes("house")
      ) {
        $(el).remove();
      }
    });

    $("*").each((_, el) => {
      const $el = $(el);
      const importantAttrs = [
        "href",
        "src",
        "alt",
        "title",
        "data-price",
        "data-value",
        "class",
        "id",
      ];
      const attrs = Object.keys(el.attribs || {});

      attrs.forEach((attr) => {
        if (
          !importantAttrs.includes(attr) &&
          !attr.startsWith("data-") &&
          !attr.startsWith("aria-")
        ) {
          $el.removeAttr(attr);
        }
      });

      const classValue = $el.attr("class");
      if (classValue) {
        const relevantClasses = classValue
          .split(" ")
          .filter(
            (cls) =>
              cls.includes("price") ||
              cls.includes("product") ||
              cls.includes("property") ||
              cls.includes("title") ||
              cls.includes("desc") ||
              cls.includes("spec") ||
              cls.includes("detail") ||
              cls.includes("image") ||
              cls.includes("photo") ||
              cls.includes("gallery") ||
              cls.includes("slide") ||
              cls.includes("content") ||
              cls.includes("info") ||
              cls.includes("data") ||
              cls.includes("value") ||
              cls.includes("villa") ||
              cls.includes("house"),
          );

        if (relevantClasses.length > 0) {
          $el.attr("class", relevantClasses.join(" "));
        } else {
          $el.removeAttr("class");
        }
      }
    });

    if (extractedImages.length > 0) {
      const imagesSummary = `
        <div class="extracted-images-summary">
          <h3>EXTRACTED IMAGES:</h3>
          ${extractedImages
            .map(
              (img, index) =>
                `<div class="image-${index}">[${img.type.toUpperCase()}] ${
                  img.url
                } (${img.alt || "No alt text"}) - Context: ${img.context}</div>`,
            )
            .join("\n")}
        </div>
      `;
      $("body").prepend(imagesSummary);
    }

    $("table").each((_, el) => {
      const $table = $(el);
      $table
        .find("*")
        .removeAttr("style")
        .removeAttr("bgcolor")
        .removeAttr("width")
        .removeAttr("height");
    });

    $("*").each((_, el) => {
      const $el = $(el);
      if ($el.children().length === 0 && $el.text().trim() === "") {
        $el.remove();
      }
    });

    let cleanedHtml = $.html();

    cleanedHtml = cleanedHtml
      .replace(/\s+/g, " ")
      .replace(/>\s+</g, "><")
      .replace(/\n\s*\n/g, "\n")
      .replace(/<!--[\s\S]*?-->/g, "")
      .replace(/\s*(function\s*\([^)]*\)[^}]*})\s*/g, "")
      .replace(/\s*{\s*}/g, "")
      .trim();

    return cleanedHtml;
  }

  extractBackgroundImages($, extractedImages) {
    $('[style*="background"]').each((_, el) => {
      const $el = $(el);
      const styleAttr = $el.attr("style");

      if (styleAttr) {
        const bgImages = this.extractUrlsFromCss(styleAttr);
        bgImages.forEach((url) => {
          extractedImages.push({
            type: "background-inline",
            url: url,
            alt: $el.attr("alt") || $el.attr("title") || "",
            context: `Inline style on ${el.tagName} with classes: ${
              $el.attr("class") || "none"
            }`,
          });
        });
      }
    });

    $("style").each((i, el) => {
      const cssContent = $(el).html();
      if (cssContent) {
        const bgImages = this.extractUrlsFromCss(cssContent);
        bgImages.forEach((url) => {
          extractedImages.push({
            type: "background-css",
            url: url,
            alt: "",
            context: `CSS style block ${i + 1}`,
          });
        });
      }
    });

    $(
      "[data-bg], [data-background], [data-src], [data-image], [data-lazy]",
    ).each((_, el) => {
      const $el = $(el);
      const dataBg =
        $el.attr("data-bg") ||
        $el.attr("data-background") ||
        $el.attr("data-src") ||
        $el.attr("data-image") ||
        $el.attr("data-lazy");

      if (
        dataBg &&
        (dataBg.includes(".jpg") ||
          dataBg.includes(".jpeg") ||
          dataBg.includes(".png") ||
          dataBg.includes(".webp") ||
          dataBg.includes(".gif"))
      ) {
        extractedImages.push({
          type: "data-attribute",
          url: dataBg,
          alt: $el.attr("alt") || $el.attr("title") || "",
          context: `Data attribute on ${el.tagName} with classes: ${
            $el.attr("class") || "none"
          }`,
        });
      }
    });
  }

  extractUrlsFromCss(cssText) {
    const urls = [];
    const urlPattern =
      /background(?:-image)?\s*:\s*url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/gi;

    let match;
    while ((match = urlPattern.exec(cssText)) !== null) {
      let url = match[1];
      url = url.trim();

      if (
        !url.startsWith("data:") &&
        !url.includes("gradient") &&
        !url.includes("linear-gradient") &&
        !url.includes("radial-gradient") &&
        (url.includes(".jpg") ||
          url.includes(".jpeg") ||
          url.includes(".png") ||
          url.includes(".webp") ||
          url.includes(".gif") ||
          url.includes(".svg"))
      ) {
        urls.push(url);
      }
    }

    return urls;
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async stop() {
    console.log("🛑 Stopping URL Queue Processor...");
    this.isProcessing = false;

    if (this.browser) {
      await this.browser.close();
    }

    if (this.db) {
      await this.db.end();
    }

    console.log("✅ URL Queue Processor stopped");
  }
}

// Main execution
const processor = new URLProcessor();

async function main() {
  const initialized = await processor.initialize();

  if (!initialized) {
    console.error("Failed to initialize processor");
    process.exit(1);
  }

  // Graceful shutdown
  process.on("SIGINT", async () => {
    await processor.stop();
    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    await processor.stop();
    process.exit(0);
  });

  // Start processing
  await processor.startProcessing();
}

module.exports = { main };
main().catch(console.error);
