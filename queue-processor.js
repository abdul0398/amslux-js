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
    this.maxConcurrentJobs = 3;
    this.currentJobs = 0;
    this.maxRetries = 3;
    this.retryDelay = 5000; // 5 seconds
    this.imagesDir = path.join(__dirname, "../", "service_images");
  }

  async initialize() {
    try {
      // Initialize database connection
      this.db = await mysql.createPool(dbConfig);
      console.log("Database connected successfully");

      // Create images directory if it doesn't exist
      await this.createImagesDirectory();

      // Initialize browser
      this.browser = await puppeteer.launch({
        headless: true,
        args: [
          "--disable-gpu",
          "--no-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--no-first-run",
          "--disable-default-apps",
        ],
      });

      console.log("Browser initialized successfully");

      return true;
    } catch (error) {
      console.error("Failed to initialize:", error.message);
      return false;
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
        await this.processPendingUrls();
        await this.sleep(this.processInterval);
      } catch (error) {
        console.error("Error in processing cycle:", error.message);
        await this.sleep(this.processInterval);
      }
    }
  }

  async processPendingUrls() {
    try {
      const limit = this.maxConcurrentJobs - this.currentJobs;

      const [rows] = await this.db.execute(
        `
      SELECT id, url, category, priority 
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
      `
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

  async processUrl(urlData, retryCount = 0) {
    this.currentJobs++;
    const { id, url, category } = urlData;
    let page = null;

    try {
      console.log(
        `🔄 Processing URL ${id}: ${url} (Attempt ${retryCount + 1})`
      );

      // Mark as processing
      await this.updateUrlStatus(id, "processing");

      // Create new page for this URL
      page = await this.browser.newPage();
      await page.setUserAgent(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
          "AppleWebKit/537.36 (KHTML, like Gecko) " +
          "Chrome/115.0 Safari/537.36"
      );

      const result = await this.scrapeUrl(page, url, id);

      if (result && result.isValid) {
        // Save scraped data
        await this.saveScrapedData(id, result.productData);

        // Update URL status
        await this.updateUrlStatus(id, "scraped", {
          is_crawled: true,
          is_parsed: true,
          is_valid: 1,
          last_scrapped: new Date().toISOString().split("T")[0],
        });

        console.log(`✅ Successfully processed URL ${id}`);
      } else {
        // Mark as invalid
        await this.updateUrlStatus(id, "failed", { is_valid: 0 });
        console.log(`❌ URL ${id} is not a valid product page`);
      }
    } catch (error) {
      console.error(
        `Error processing URL ${id} (Attempt ${retryCount + 1}):`,
        error.message
      );

      // Retry logic
      if (retryCount < this.maxRetries) {
        console.log(
          `🔄 Retrying URL ${id} in ${this.retryDelay / 1000} seconds...`
        );
        await this.sleep(this.retryDelay);

        // Close current page before retry
        if (page) {
          await page.close();
          page = null;
        }

        this.currentJobs--;
        return this.processUrl(urlData, retryCount + 1);
      } else {
        // Mark as failed after all retries exhausted
        await this.updateUrlStatus(id, "failed");
        console.log(
          `❌ Failed to process URL ${id} after ${this.maxRetries + 1} attempts`
        );
      }
    } finally {
      if (page) {
        await page.close();
      }
      this.currentJobs--;
    }
  }

  async scrapeUrl(page, url, urlId) {
    try {
      const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

      await Promise.race([
        page.goto(url, { waitUntil: "load", timeout: 30000 }),
        delay(30000),
      ]);

      const html = await page.content();

      // Basic check for blocked/error pages
      if (
        !html ||
        html.length < 1000 ||
        html.includes("Cloudflare") ||
        html.includes("Access denied")
      ) {
        console.log(
          `❌ Failed to extract valid HTML for URL ${urlId} - likely blocked or error page`
        );
        return { isValid: false, productData: null };
      }

      const optimizedHtml = this.optimizeHtmlForAI(html);

      // Single AI call that both validates and extracts data
      const aiResult = await this.validateAndExtractProductData(optimizedHtml);

      return aiResult;
    } catch (error) {
      console.error("Scraping error:", error.message);
      return { isValid: false, productData: null };
    }
  }

  // OPTIMIZED: Single AI call for both validation and extraction
  async validateAndExtractProductData(html) {
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `
              You are an expert at identifying and extracting product data from website content.
              
              First, determine if this is a legitimate product page (for items like watches, jewelry, clothing, shoes, cars, real estate, etc.).
              
              If it's NOT a valid product page (error page, category listing, blog, etc.), return:
              {"isValid": false, "reason": "brief reason why not valid"}
              
              If it IS a valid product page, extract the data and return:
              {
                "isValid": true,
                "productData": {
                  "title": "product title",
                  "price": "price information", 
                  "location": "location details",
                  "description": "product description",
                  "images": ["list", "of", "image", "urls"],
                  "tag": "single product type like: watch, shirt, sneaker, etc"
                }
              }
              
              - Always output valid JSON only
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
        max_tokens: 1000,
      });

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
      // Fallback to basic validation if AI fails
      const isValid =
        html &&
        html.length > 1000 &&
        !html.includes("404") &&
        !html.includes("error") &&
        !html.includes("Access denied");

      return {
        isValid,
        productData: null,
        reason: isValid ? "AI extraction failed" : "Basic validation failed",
      };
    }
  }

  async saveScrapedData(urlId, productData) {
    try {
      // Create category service directly - no need for intermediate table
      if (productData && productData.title) {
        // Download images locally first
        const downloadedImages = await this.downloadImages(
          productData.images,
          urlId
        );
        productData.downloadedImages = downloadedImages;

        await this.createCategoryService(urlId, productData);
      }
    } catch (error) {
      console.error("Error saving scraped data:", error.message);
    }
  }

  async downloadImages(imageUrls, urlId) {
    if (!imageUrls || imageUrls.length === 0) return [];

    const downloadedImages = [];

    for (let i = 0; i < imageUrls.length; i++) {
      try {
        const imageUrl = imageUrls[i];
        if (!this.isValidImageUrl(imageUrl)) continue;

        const fileName = `service_${urlId}_${i + 1}_${Date.now()}.jpg`;
        const localPath = path.join(this.imagesDir, fileName);

        const success = await this.downloadImage(imageUrl, localPath);
        if (success) {
          const relativePath = `/images/services/${fileName}`;
          downloadedImages.push({
            originalUrl: imageUrl,
            localPath: relativePath,
            fileName: fileName,
          });
          console.log(
            `📥 Downloaded image ${i + 1}/${imageUrls.length}: ${fileName}`
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
          const fileStream = require("fs").createWriteStream(localPath);
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
    try {
      const { title, price, location, description, tag, downloadedImages } =
        productData;

      // Extract price information
      const extractedPrice = this.extractPrice(price);

      // Get the source URL and category info from product_urls table
      const [urlRow] = await this.db.execute(
        "SELECT url, category, subcategory_id FROM product_urls WHERE id = ?",
        [urlId]
      );
      const sourceUrl = urlRow.length > 0 ? urlRow[0].url : null;
      const categoryId = urlRow.length > 0 ? urlRow[0].category : null;
      const subCategoryId = urlRow.length > 0 ? urlRow[0].subcategory_id : null;

      // Check if similar service already exists
      const [existing] = await this.db.execute(
        "SELECT id FROM category_services WHERE tag = ? AND title = ?",
        [tag, title]
      );

      if (existing.length === 0) {
        // Use first downloaded image as primary image, fallback to original URL if no downloads
        const primaryImageUrl =
          downloadedImages && downloadedImages.length > 0
            ? downloadedImages[0].localPath
            : productData.images && productData.images.length > 0
            ? productData.images[0]
            : null;

        const about_description =
          description.length > 100
            ? description.slice(0, 100) + "..."
            : description || `High-quality ${tag} product`;
        // Create new category service
        const [result] = await this.db.execute(
          `
          INSERT INTO category_services (
            category_id, sub_category_id, service_name, title, about_description,
            description, imageUrl, tag, service_type, startingPrice, location,
            source_url, stock_status, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'normal', ?, ?, ?, 'in_stock', NOW(), NOW())
        `,
          [
            categoryId,
            subCategoryId,
            title,
            title,
            about_description,
            description || `Premium ${tag} available for purchase`,
            primaryImageUrl,
            tag,
            extractedPrice,
            location,
            sourceUrl,
          ]
        );

        const serviceId = result.insertId;

        // Store additional images in service_image_galleries table
        if (downloadedImages && downloadedImages.length > 0) {
          await this.saveImageGallery(serviceId, downloadedImages);
        }

        console.log(
          `📦 Created category service for: ${title} (${tag}) with ${
            downloadedImages ? downloadedImages.length : 0
          } images`
        );
      } else {
        console.log(`🔄 Service already exists for: ${title}`);
      }
    } catch (error) {
      console.error("Error creating category service:", error.message);
    }
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
          [serviceId.toString(), image.localPath]
        );
      }

      console.log(
        `🖼️  Saved ${downloadedImages.length} images to gallery for service ${serviceId}`
      );
    } catch (error) {
      console.error("Error saving image gallery:", error.message);
    }
  }

  generateCategoryId(tag) {
    // Map common tags to category IDs
    const categoryMap = {
      watch: "luxury-watches",
      watches: "luxury-watches",
      jewelry: "jewelry",
      ring: "jewelry",
      necklace: "jewelry",
      bracelet: "jewelry",
      earrings: "jewelry",
      handbag: "fashion-accessories",
      bag: "fashion-accessories",
      shoes: "footwear",
      sneakers: "footwear",
      boots: "footwear",
      shirt: "clothing",
      dress: "clothing",
      jacket: "clothing",
      coat: "clothing",
      car: "vehicles",
      motorcycle: "vehicles",
      house: "real-estate",
      villa: "real-estate",
      apartment: "real-estate",
    };

    return categoryMap[tag.toLowerCase()] || `category-${tag.toLowerCase()}`;
  }

  generateSubCategoryId(tag) {
    // Generate subcategory based on tag
    return `sub-${tag.toLowerCase()}`;
  }

  extractPrice(priceString) {
    if (!priceString) return null;

    // Extract numeric value from price string
    const matches = priceString.match(/[\d,]+\.?\d*/);
    if (matches) {
      return parseFloat(matches[0].replace(/,/g, ""));
    }

    return null;
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
      "script, style, noscript, iframe, embed, object, " +
        'link[rel="stylesheet"], meta, head, ' +
        ".cookie-banner, .cookie-notice, .gdpr, " +
        ".newsletter, .popup, .modal, .overlay, " +
        ".social-share, .social-media, .social-icons, " +
        ".advertisement, .ads, .ad-banner, .google-ads, " +
        ".tracking, .analytics, " +
        "nav.breadcrumb, .breadcrumbs, " +
        "footer, .footer-content, " +
        ".sidebar-ads, .recommended, .suggestions, " +
        ".comments-section, .user-comments"
    ).remove();

    $(
      '[style*="display:none"], [style*="display: none"], [hidden], .hidden, .sr-only, .visually-hidden'
    ).remove();

    $("nav").each((i, el) => {
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

    $("*").each((i, el) => {
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
              cls.includes("house")
          );

        if (relevantClasses.length > 0) {
          $el.attr("class", relevantClasses.join(" "));
        } else {
          $el.removeAttr("class");
        }
      }
    });

    $("img").each((i, el) => {
      const $el = $(el);
      const src =
        $el.attr("src") || $el.attr("data-src") || $el.attr("data-lazy");
      const alt = $el.attr("alt") || "";

      if (src) {
        extractedImages.push({
          type: "img",
          url: src,
          alt: alt,
          context: "Regular image tag",
        });

        $el.replaceWith(`[IMAGE: ${alt || "Property image"} - ${src}]`);
      } else {
        $el.remove();
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
                } (${img.alt || "No alt text"}) - Context: ${img.context}</div>`
            )
            .join("\n")}
        </div>
      `;
      $("body").prepend(imagesSummary);
    }

    $("table").each((i, el) => {
      const $table = $(el);
      $table
        .find("*")
        .removeAttr("style")
        .removeAttr("bgcolor")
        .removeAttr("width")
        .removeAttr("height");
    });

    $("*").each((i, el) => {
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
    $('[style*="background"]').each((i, el) => {
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
      "[data-bg], [data-background], [data-src], [data-image], [data-lazy]"
    ).each((i, el) => {
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

main().catch(console.error);
