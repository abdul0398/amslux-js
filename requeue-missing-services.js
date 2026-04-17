require("dotenv").config();
const mysql = require("mysql2/promise");

const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_DATABASE || "amslux",
  port: process.env.DB_PORT || 3306,
};

async function requeueMissingServices() {
  const db = await mysql.createConnection(dbConfig);

  try {
    // Find scraped URLs from last 5 days that have no matching category_service
    const [rows] = await db.execute(`
     SELECT pu.id, pu.url, pu.status
    FROM product_urls pu
    LEFT JOIN category_services cs ON cs.source_url = pu.url
    WHERE pu.created_at >= DATE_SUB(NOW(), INTERVAL 1 MONTH)
    AND pu.status IN ('scraped', 'failed')
    AND cs.id IS NULL;
    `);

    if (rows.length === 0) {
      console.log("No URLs to requeue — all have matching category_services.");
      return;
    }

    console.log(`Found ${rows.length} URLs without a category_service:`);
    rows.forEach((r) => console.log(`  [${r.status}] ID ${r.id}: ${r.url}`));

    // Requeue them as pending
    const ids = rows.map((r) => r.id);
    const placeholders = ids.map(() => "?").join(",");

    const [result] = await db.execute(
      `UPDATE product_urls
       SET status = 'pending',
           is_crawled = 0,
           is_parsed = 0,
           is_valid = 0,
           last_scrapped = NULL,
           updated_at = NOW()
       WHERE id IN (${placeholders})`,
      ids,
    );

    console.log(`Requeued ${result.affectedRows} URLs for re-processing.`);
  } catch (error) {
    console.error("Error:", error.message);
  } finally {
    await db.end();
  }
}

requeueMissingServices().catch(console.error);
