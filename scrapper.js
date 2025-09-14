require("dotenv").config();
const puppeteer = require("puppeteer");
const cheerio = require("cheerio");
const { encoding_for_model } = require("tiktoken");
const OpenAI = require("openai");

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY, // Make sure to set this environment variable
});

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    executablePath:
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    args: [
      "--disable-gpu",
      "--no-sandbox",
      "--disable-blink-features=AutomationControlled",
      "--no-first-run",
      "--disable-default-apps",
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) " +
      "Chrome/115.0 Safari/537.36"
  );

  const url =
    "https://www.chrono24.nl/audemarspiguet/royal-oak-41-selfwinding-khaki-green-dial-15510st-new--id40961260.htm";

  try {
    const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    await Promise.race([
      page.goto(url, { waitUntil: "load", timeout: 0 }),
      delay(10000), // will just resolve silently after 10s
    ]);

    // Small grace period for late DOM updates (optional)

    const html = await page.content();
    const optimizedHtml = optimizeHtmlForAI(html);

    const productData = await extractProductData(optimizedHtml);
    console.log(
      "Extracted Product Data:",
      JSON.stringify(productData, null, 2)
    );
    return { optimizedHtml, productData };
  } catch (error) {
    console.error("Error:", error.message);
  } finally {
    await browser.close();
  }
})();

function optimizeHtmlForAI(html) {
  const $ = cheerio.load(html);

  // Array to store all extracted images
  const extractedImages = [];

  // 1. Extract background images from CSS before removing styles
  extractBackgroundImages($, extractedImages);

  // 2. Remove completely unnecessary elements
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

  // 3. Remove hidden or visually hidden content
  $(
    '[style*="display:none"], [style*="display: none"], [hidden], .hidden, .sr-only, .visually-hidden'
  ).remove();

  // 4. Clean up navigation (keep only if it contains product categories)
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

  // 5. Remove excessive attributes while keeping important ones
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
      // Keep important attributes and data attributes that might contain product info
      if (
        !importantAttrs.includes(attr) &&
        !attr.startsWith("data-") &&
        !attr.startsWith("aria-")
      ) {
        $el.removeAttr(attr);
      }
    });

    // Simplify class names (keep only if they seem content-related)
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

  // 6. Convert regular images to text representation and collect URLs
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

      // Replace img tag with a simple text representation
      $el.replaceWith(`[IMAGE: ${alt || "Property image"} - ${src}]`);
    } else {
      $el.remove();
    }
  });

  // 7. Add extracted images summary to the beginning of HTML
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

  // 8. Simplify tables while preserving structure
  $("table").each((i, el) => {
    const $table = $(el);
    // Remove styling attributes from table elements
    $table
      .find("*")
      .removeAttr("style")
      .removeAttr("bgcolor")
      .removeAttr("width")
      .removeAttr("height");
  });

  // 9. Remove empty elements and consolidate whitespace
  $("*").each((i, el) => {
    const $el = $(el);
    if ($el.children().length === 0 && $el.text().trim() === "") {
      $el.remove();
    }
  });

  // 10. Get the cleaned HTML
  let cleanedHtml = $.html();

  // 11. Final text cleanup
  cleanedHtml = cleanedHtml
    // Remove excessive whitespace
    .replace(/\s+/g, " ")
    // Remove whitespace around tags
    .replace(/>\s+</g, "><")
    // Remove empty lines
    .replace(/\n\s*\n/g, "\n")
    // Remove HTML comments
    .replace(/<!--[\s\S]*?-->/g, "")
    // Clean up common noise patterns
    .replace(/\s*(function\s*\([^)]*\)[^}]*})\s*/g, "")
    .replace(/\s*{\s*}/g, "")
    .trim();

  return cleanedHtml;
}

function extractBackgroundImages($, extractedImages) {
  // 1. Extract from inline style attributes
  $('[style*="background"]').each((i, el) => {
    const $el = $(el);
    const styleAttr = $el.attr("style");

    if (styleAttr) {
      const bgImages = extractUrlsFromCss(styleAttr);
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

  // 2. Extract from style tags
  $("style").each((i, el) => {
    const cssContent = $(el).html();
    if (cssContent) {
      const bgImages = extractUrlsFromCss(cssContent);
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

  // 3. Look for data attributes that might contain image URLs
  $("[data-bg], [data-background], [data-src], [data-image], [data-lazy]").each(
    (i, el) => {
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
    }
  );
}

function extractUrlsFromCss(cssText) {
  const urls = [];
  // Match background-image: url(...) patterns
  const urlPattern =
    /background(?:-image)?\s*:\s*url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/gi;

  let match;
  while ((match = urlPattern.exec(cssText)) !== null) {
    let url = match[1];

    // Clean up the URL
    url = url.trim();

    // Skip data URLs, gradients, and other non-image URLs
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
      // Convert relative URLs to absolute if needed
      if (
        url.startsWith("/") ||
        url.startsWith("./") ||
        url.startsWith("../")
      ) {
        // You might want to prepend the base URL here
        // url = baseUrl + url;
      }

      urls.push(url);
    }
  }

  return urls;
}

async function extractProductData(html) {
  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [
        {
          role: "system",
          content: `
    You are an expert at extracting structured product or property data from raw HTML. 
    - Always output valid JSON only, matching the exact schema provided.
    - If the extracted text is not in English, translate it fully into natural English before inserting into JSON.
    - Do not include any extra commentary, only return the JSON object.
    `,
        },
        {
          role: "user",
          content: `
    Extract all product/property information from this HTML and return it strictly as JSON in the following structure:
    
    {
      "title": "property title",
      "price": "price information",
      "location": "location details",
      "description": "property description",
      "images": ["list", "of", "image", "urls"],
      "tag": "only one what of product is this, example watch, shirt, sneaker etc"
    }
    
    Ensure all fields are in English.
    
    HTML content:
    ${html}
    `,
        },
      ],
      temperature: 0.1,
    });

    return JSON.parse(completion.choices[0].message.content);
  } catch (error) {
    console.error("Error extracting product data:", error.message);
    return null;
  }
}

async function countTokens(text) {
  // Pick the model you are targeting
  const encoder = encoding_for_model("gpt-4.1-nano");

  // Encode the text into tokens
  const tokens = encoder.encode(text);

  // Return number of tokens
  return tokens.length;
}
