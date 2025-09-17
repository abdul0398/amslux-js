const admin = require("firebase-admin");
const { OpenAI } = require("openai");
const dotenv = require("dotenv");
const mysql = require("mysql2/promise"); // Added for MySQL async support
dotenv.config();

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY }); // Set your API key in .env

const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// Create MySQL connection pool (using .env vars for credentials)
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

async function getUserFCMToken() {
  const connection = await pool.getConnection();
  try {
    console.log("🚀 Starting FCM token sync for all users...");

    // Get all users from MySQL database
    const [users] = await connection.query("SELECT id FROM users");
    console.log(`📊 Found ${users.length} users in MySQL database`);

    let updatedCount = 0;
    let notFoundCount = 0;
    let errorCount = 0;

    for (const user of users) {
      const userId = user.id;

      try {
        console.log(`🔍 Processing user: ${userId}`);

        // Get user document from Firebase 'users' collection
        const userDoc = await db
          .collection("users")
          .doc(userId.toString())
          .get();

        if (!userDoc.exists) {
          console.log(
            `❌ User ${userId} not found in Firebase users collection`
          );
          notFoundCount++;
          continue;
        }

        const userData = userDoc.data();
        const fcmToken =
          userData.fcmToken ||
          userData.FCMToken ||
          userData.token ||
          userData.deviceToken;

        if (fcmToken) {
          console.log(`✅ Found FCM token for user ${userId}`);

          // Update FCM token in MySQL database
          await connection.execute(
            "UPDATE users SET fcmToken = ?, updated_at = NOW() WHERE id = ?",
            [fcmToken, userId]
          );

          console.log(`💾 Updated FCM token for user ${userId} in database`);
          updatedCount++;
        } else {
          console.log(`⚠️ No FCM token found for user ${userId}`);
          console.log("Available Firebase fields:", Object.keys(userData));
          notFoundCount++;
        }
      } catch (userError) {
        console.error(`❌ Error processing user ${userId}:`, userError);
        errorCount++;
      }
    }

    console.log("\n📈 FCM Token Sync Summary:");
    console.log(`✅ Successfully updated: ${updatedCount} users`);
    console.log(`⚠️ No token found: ${notFoundCount} users`);
    console.log(`❌ Errors: ${errorCount} users`);
    console.log(`📊 Total processed: ${users.length} users`);

    return {
      totalProcessed: users.length,
      updated: updatedCount,
      notFound: notFoundCount,
      errors: errorCount,
    };
  } catch (error) {
    console.error("❌ Error in FCM token sync process:", error);
    throw error;
  } finally {
    connection.release();
  }
}

// New function to extract service-specific data for serviceId 4802
async function extractServiceSpecificData(userId, userChatrooms) {
  const serviceSpecificData = {};

  // Check if user has chatrooms with serviceId 4802
  const service4802Chatrooms = {};
  for (const [chatroomId, messages] of Object.entries(userChatrooms)) {
    const hasService4802 = messages.some((msg) => msg.serviceId === "4802");
    if (hasService4802) {
      service4802Chatrooms[chatroomId] = messages.filter(
        (msg) => msg.serviceId === "4802"
      );
    }
  }

  if (Object.keys(service4802Chatrooms).length > 0) {
    console.log(
      `📱 Extracting service-specific data for user ${userId} - Service 4802`
    );

    // Flatten all service 4802 messages and sort by time
    let allService4802Messages = [];
    for (const messages of Object.values(service4802Chatrooms)) {
      allService4802Messages = allService4802Messages.concat(messages);
    }
    allService4802Messages.sort((a, b) => (a.time || 0) - (b.time || 0));

    // Build context for service 4802 messages
    const chatContext = allService4802Messages
      .filter((m) => m.text && m.text.trim())
      .map((m) => `Sender ${m.senderId}: ${m.text} (Time: ${m.time})`)
      .join("\n")
      .slice(0, 6000); // Limit context

    if (chatContext.trim()) {
      const prompt = `
IMPORTANT: Respond ONLY with valid JSON in the exact format below. No explanations, no extra text, no markdown code blocks.

Analyze this post-match celebration chat conversation to extract specific event details:

${chatContext}

Extract the following information if present in the conversation. If any information is not found or unclear, use the default values provided:

OUTPUT FORMAT (valid JSON only):
{
  "showtime_summary": {
    "title": "Game night in paris",
    "summary": "Media attention . Full setup ready"
  },
  "tonight_setup": {
    "arrival_time": "10:30 PM",
    "arrival_method": "Black Car", 
    "venue": "L'Avenue",
    "venue_details": "VIP Table",
    "style_code": "Smart Casual"
  }
}

EXTRACTION RULES:
- showtime_summary.title: Look for event names, game references, location mentions, or celebration titles. If found, use that. Default: "Game night in paris"
- showtime_summary.summary: Look for setup status, media mentions, preparation details, or event descriptions. Keep concise (under 50 chars). Default: "Media attention . Full setup ready"
- tonight_setup.arrival_time: Look for time mentions (e.g., "10:30", "at 8", "around 9 PM"). Default: "10:30 PM"
- tonight_setup.arrival_method: Look for transport mentions (car, taxi, uber, walking, etc.). Default: "Black Car"
- tonight_setup.venue: Look for restaurant names, club names, venue names, location names. Default: "L'Avenue" 
- tonight_setup.venue_details: Look for table type, reservation details, seating arrangements. Default: "VIP Table"
- tonight_setup.style_code: Look for dress code mentions, outfit requirements, style guidelines. Default: "Smart Casual"

Only extract information that is explicitly mentioned in the conversation. Use defaults for missing information.
      `;

      try {
        const completion = await openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content:
                "You extract specific event details from post-match celebration chats. Always respond with valid JSON only, no code blocks or explanations.",
            },
            { role: "user", content: prompt },
          ],
          temperature: 0.3,
        });

        const response = completion.choices[0].message.content.trim();
        // Clean the response to remove any markdown code blocks
        let cleanedResponse = response
          .replace(/```json\n?/g, "")
          .replace(/```\n?/g, "")
          .trim();

        try {
          const parsedData = JSON.parse(cleanedResponse);
          serviceSpecificData.service_4802 = parsedData;
          console.log(`✅ Extracted service 4802 data for user ${userId}`);
        } catch (parseError) {
          console.warn(
            `⚠️ Failed to parse service 4802 data for user ${userId}. Using defaults.`
          );
          serviceSpecificData.service_4802 = getDefaultService4802Data();
        }
      } catch (error) {
        console.error(
          `❌ Error extracting service 4802 data for user ${userId}:`,
          error
        );
        serviceSpecificData.service_4802 = getDefaultService4802Data();
      }
    } else {
      serviceSpecificData.service_4802 = getDefaultService4802Data();
    }
  }

  return serviceSpecificData;
}

// Default data for service 4802
function getDefaultService4802Data() {
  return {
    showtime_summary: {
      title: "Game night in paris",
      summary: "Media attention . Full setup ready",
    },
    tonight_setup: {
      arrival_time: "10:30 PM",
      arrival_method: "Black Car",
      venue: "L'Avenue",
      venue_details: "VIP Table",
      style_code: "Smart Casual",
    },
  };
}

// Combined function: Fetch, group, summarize, and save to MySQL
async function processAndSaveUserSummaries() {
  try {
    console.log("⏳ Fetching all services...");
    const servicesSnap = await db.collection("services").get();
    const groupedConversations = {};

    console.log(`📦 Found ${servicesSnap.size} services.`);

    for (const serviceDoc of servicesSnap.docs) {
      const serviceId = serviceDoc.id;
      const serviceData = serviceDoc.data();
      const serviceName = serviceData.serviceName || "Unnamed Service";
      const serviceCategory = serviceData.category || "Unknown";
      console.log(`➡️ Processing service: ${serviceId}`);

      const chatroomsSnap = await db
        .collection(`services/${serviceId}/chatrooms`)
        .get();

      console.log(
        `   💬 Found ${chatroomsSnap.size} chatrooms in service ${serviceId}`
      );

      for (const chatroomDoc of chatroomsSnap.docs) {
        const chatroomId = chatroomDoc.id;
        console.log(`     🗂️ Chatroom: ${chatroomId}`);

        const messagesSnap = await db
          .collection(`services/${serviceId}/chatrooms/${chatroomId}/messages`)
          .get();

        console.log(`✉️ ${messagesSnap.size} messages found.`);

        // Temporary array to collect all messages in this chatroom
        const chatroomMessages = [];

        messagesSnap.forEach((msgDoc) => {
          const data = msgDoc.data();
          chatroomMessages.push({
            serviceId,
            chatroomId,
            messageId: msgDoc.id,
            senderId: data.senderId, // Add senderId for clarity in conversations
            text: data.text || "",
            serviceName,
            serviceCategory,
            time: data.time || null,
            seenBy: data.seenBy || [],
          });
        });

        // Sort messages by time (ascending)
        chatroomMessages.sort((a, b) => (a.time || 0) - (b.time || 0));

        // Parse participants from chatroomId (assuming format: chatroom-userA-userB-serviceId)
        const parts = chatroomId.split("-");
        if (parts.length >= 4 && parts[0] === "chatroom") {
          const userA = parseInt(parts[1], 10);
          const userB = parseInt(parts[2], 10);
          const participants = [userA, userB].filter((id) => !isNaN(id));

          // Assign the full sorted conversation to each participant's group
          participants.forEach((userId) => {
            if (!groupedConversations[userId]) {
              groupedConversations[userId] = {};
            }
            groupedConversations[userId][chatroomId] = chatroomMessages;
          });
        } else {
          console.warn(
            `⚠️ Skipping chatroom ${chatroomId}: Invalid ID format for parsing participants.`
          );
        }
      }
    }

    // Now summarize using GPT (in memory, no JSON file)
    const summaries = {};
    const serviceSpecificDataAll = {};

    for (const [userId, chatrooms] of Object.entries(groupedConversations)) {
      console.log(
        `📝 Summarizing conversations for user: ${userId} (${
          Object.keys(chatrooms).length
        } chatrooms)`
      );

      // Extract service-specific data
      serviceSpecificDataAll[userId] = await extractServiceSpecificData(
        userId,
        chatrooms
      );
      const businessPulseData = await extractBusinessPulseData(
        userId,
        chatrooms
      );
      serviceSpecificDataAll[userId] = {
        ...serviceSpecificDataAll[userId],
        ...businessPulseData,
      };

      // Flatten all messages across chatrooms and sort by time
      let allMessages = [];
      for (const messages of Object.values(chatrooms)) {
        allMessages = allMessages.concat(messages);
      }
      allMessages.sort((a, b) => (a.time || 0) - (b.time || 0));

      // Build context: Include sender, text, service ID, name, category, time (skip empty texts)
      const chatContext = allMessages
        .filter((m) => m.text && m.text.trim()) // Skip empty messages
        .map(
          (m) =>
            `Sender ${m.senderId}: ${m.text} (Service ID: ${m.serviceId}, Name: ${m.serviceName} - ${m.serviceCategory}, Time: ${m.time})`
        )
        .join("\n")
        .slice(0, 8000); // Limit to avoid token limits

      // Safeguard: If no messages, assign default and skip GPT call
      if (allMessages.length === 0 || !chatContext.trim()) {
        console.log(
          `   ⚠️ No valid messages for user ${userId}. Using default summary.`
        );
        summaries[userId] =
          "summary=No activity||trips=0||brands=0||products=0||bought_services=||";
        continue;
      }

      const prompt = `
IMPORTANT: Respond ONLY with the exact format below. No explanations, no extra text, no questions. EVEN IF NO DATA or insufficient evidence, output exactly in the format with summary=No activity|| and all counts=0||bought_services=||trip_details=||. Base output STRICTLY on the provided conversations ONLY—do not invent, assume, or add any messages.


Analyze these full customer conversations (including back-and-forth messages with senders identified) to create a widget summary and transaction data:


${chatContext}


OUTPUT FORMAT:
summary=Travel booking inquiry • Fashion item interest||trips=1||brands=2||products=3||bought_services=serviceId1,serviceId2||||trip_details=Trip1: From 2025-07-01 to 2025-07-05, Destination: Paris, serviceId: 2322; Trip2: From 2025-08-01 to 2025-08-10, Destination: Tokyo, serviceId: 2322||


RULES:
- summary: Concise overview of ACTUAL user actions/interests from the full conversations (max 60 chars total). Use • to separate key points. Focus on verified intents (e.g., request + positive response). If no evidence, use "No activity".
- trips: Count unique travel bookings/requests that show strong intent AND evidence of progress/confirmation (look for: "book", "booked", "booking", "get this booked" followed by affirmative replies like "confirmed" or "done"). Count per unique service/chatroom. If none, 0.
- brands: Count unique fashion/product brands where user showed purchase intent with evidence (e.g., specific request + confirmation). Deduplicate across conversations. If none, 0.
- products: Count total unique products/services with verified purchase intent (e.g., requests like "get me this", "order", "buy", "in medium please" that lead to positive outcomes in the conversation). Deduplicate across chats. If none, 0.
- bought_services: Comma-separated list of UNIQUE service IDs (from the context) where a purchase was CONFIRMED (e.g., based on full conversation showing request + confirmation like "booked" or "purchased"). Deduplicate across all chats. Only include if there's strong evidence of completion. If none, empty (just ||).
- trip_details: Semicolon-separated list of trip details for each counted trip (in the order they appear in conversations). Format each as "TripN: From YYYY-MM-DD to YYYY-MM-DD, Destination: Place, serviceId: NNN" (use exact dates, destinations mentioned and serviceId; if any missing, use "N/A" for that field). Only include for trips that meet the 'trips' criteria with strong evidence. Deduplicate and base strictly on chat mentions. If no trips or details, empty (just ||). Use ISO format for dates (YYYY-MM-DD).
- Purchase indicators: "book", "get me this", "order", "buy", "purchase", "get this booked", "in medium please". Only count if conversation shows follow-through (e.g., thanks/confirmation after request, or positive reply).
- Consider full context: Use timestamps for sequence. Ignore vague interest without action. Do not overcount—focus on confirmed or strongly indicated transactions.
- Treat chat-based requests with positive responses as transactions.
- If no evidence of any activity, set summary=No activity|| with counts=0 and bought_services=||||trip_details=||.
`;

      const completion = await openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          {
            role: "system",
            content:
              "You create concise widget summaries for customer dashboards. Always use • to separate items. Keep under 60 characters total. Analyze full conversations for accuracy. STRICTLY follow the output format—no deviations, even for edge cases.",
          },
          { role: "user", content: prompt },
        ],
      });

      const response = completion.choices[0].message.content.trim();
      // Validate response format (basic regex check)
      const formatRegex =
        /^summary=.*\|\|trips=\d+\|\|brands=\d+\|\|products=\d+\|\|bought_services=.*\|\|$/;
      if (!formatRegex.test(response)) {
        console.warn(
          `   ⚠️ Invalid GPT response for user ${userId}. Using default.`
        );
        summaries[userId] =
          "summary=No activity||trips=0||brands=0||products=0||bought_services=||";
      } else {
        summaries[userId] = response;
      }
    }

    // Now save summaries to MySQL
    console.log("💾 Saving summaries to MySQL database...");
    const connection = await pool.getConnection();
    try {
      for (const [userId, rawSummary] of Object.entries(summaries)) {
        // Parse the raw summary string
        const parts = rawSummary.split("||");
        const summaryText =
          parts[0]?.replace("summary=", "").trim() || "No activity";
        const trips = parseInt(
          parts[1]?.replace("trips=", "").trim() || "0",
          10
        );
        const brands = parseInt(
          parts[2]?.replace("brands=", "").trim() || "0",
          10
        );
        const products = parseInt(
          parts[3]?.replace("products=", "").trim() || "0",
          10
        );
        const boughtServices =
          parts[4]?.replace("bought_services=", "").trim() || "";

        const tripDetails = parts[5]?.replace("trip_details=", "").trim() || "";

        const userPreference = getUserPreference(userId);

        const completion = await openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content:
                "you give percentage matching between the userPreference and the user activity summary, only give number nothing else",
            },
            {
              role: "user",
              content: `summary:${rawSummary}, userPreference:${userPreference}`,
            },
          ],
        });
        const styleMatch = parseInt(completion.choices[0].message.content);

        let relatedProductsJson = "";
        if (trips > 0) {
          const relatedProducts = await getRelatedProductsBasedOnTrip(
            tripDetails
          );
          relatedProductsJson = JSON.stringify(relatedProducts);
        }

        // Get service-specific data for this user (only service 4802)
        const post_match_summary = JSON.stringify(
          serviceSpecificDataAll[userId]?.service_4802 ||
            getDefaultService4802Data()
        );

        const businessPulseJson = JSON.stringify(
          serviceSpecificDataAll[userId]?.business_pulse || null
        );

        // UPSERT query (insert or update if user_id exists)
        const query = `
      INSERT INTO user_summaries 
  (user_id, summary_text, trips, brands, products, raw_summary, bought_services, style_match, trip_details, related_products, post_match_summary, business_pulse_data, created_at, updated_at) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) 
ON DUPLICATE KEY UPDATE 
  summary_text = VALUES(summary_text), 
  trips = VALUES(trips), 
  brands = VALUES(brands), 
  products = VALUES(products), 
  raw_summary = VALUES(raw_summary), 
  bought_services = VALUES(bought_services), 
  style_match = VALUES(style_match), 
  trip_details = VALUES(trip_details), 
  related_products = VALUES(related_products), 
  post_match_summary = VALUES(post_match_summary),
  business_pulse_data = VALUES(business_pulse_data),
  updated_at = NOW();
    `;

        await connection.execute(query, [
          userId,
          summaryText,
          trips,
          brands,
          products,
          rawSummary,
          boughtServices ?? "",
          styleMatch ?? 0,
          tripDetails ?? "",
          relatedProductsJson ?? "",
          post_match_summary,
          businessPulseJson,
        ]);
        console.log(`✅ Saved/Updated summary for user ${userId}`);
      }
    } catch (error) {
      console.error("Error processing and saving user summaries:", error);
      // Additional error handling logic can be added here
    } finally {
      connection.release();
    }

    console.log("✅ Processing and saving complete!");
  } catch (error) {
    console.error("❌ Error processing and saving user summaries:", error);
  }
}

async function getRelatedProductsBasedOnTrip(tripDetails) {
  // Step 0: Parse trip_details to extract serviceIds and fetch details from category_services
  let dynamicTripDetail = "";
  if (tripDetails && tripDetails.trim()) {
    const trips = tripDetails.split(";").map((trip) => trip.trim());
    const serviceIds = trips
      .map((trip) => {
        const match = trip.match(/serviceId:\s*(\d+)/);
        return match ? match[1] : null;
      })
      .filter((id) => id !== null);

    if (serviceIds.length > 0) {
      // Fetch details for each unique serviceId
      const uniqueServiceIds = [...new Set(serviceIds)];
      const placeholders = uniqueServiceIds.map(() => "?").join(",");
      const [tripRows] = await pool.query(
        `SELECT id, title, service_name, description FROM category_services WHERE id IN (${placeholders})`,
        uniqueServiceIds
      );

      // Construct dynamic tripDetail by concatenating fetched data
      dynamicTripDetail = tripRows
        .map(
          (row) =>
            `Trip Service ID: ${row.id}, Title: ${row.title}, Service Name: ${row.service_name}, Description: ${row.description}`
        )
        .join("\n\n");
    }
  }

  // Fallback to empty if no details fetched
  if (!dynamicTripDetail.trim()) {
    dynamicTripDetail =
      "No trip details available. Unable to match related products.";
  }

  // Step 1: Fetch all product data with additional category/type information
  const [rows] = await pool.query(
    `SELECT id, title, service_name, description, 
     CASE 
       WHEN LOWER(title) LIKE '%shirt%' OR LOWER(title) LIKE '%top%' OR LOWER(title) LIKE '%blouse%' THEN 'top'
       WHEN LOWER(title) LIKE '%pant%' OR LOWER(title) LIKE '%jean%' OR LOWER(title) LIKE '%short%' OR LOWER(title) LIKE '%skirt%' THEN 'bottom'
       WHEN LOWER(title) LIKE '%shoe%' OR LOWER(title) LIKE '%sandal%' OR LOWER(title) LIKE '%boot%' THEN 'footwear'
       WHEN LOWER(title) LIKE '%hat%' OR LOWER(title) LIKE '%cap%' OR LOWER(title) LIKE '%sunglasses%' THEN 'accessory'
       WHEN LOWER(title) LIKE '%jacket%' OR LOWER(title) LIKE '%coat%' OR LOWER(title) LIKE '%sweater%' THEN 'outerwear'
       WHEN LOWER(title) LIKE '%dress%' OR LOWER(title) LIKE '%gown%' THEN 'dress'
       ELSE 'other'
     END as product_category
     FROM category_services 
     ORDER BY created_at DESC LIMIT 300`
  );

  // Step 2: Chunk the products to avoid token limits
  const chunkSize = 40;
  const chunks = [];
  for (let i = 0; i < rows.length; i += chunkSize) {
    chunks.push(rows.slice(i, i + chunkSize));
  }

  // Helper function to clean OpenAI response
  const cleanResponse = (content) => {
    let cleaned = content.trim();
    cleaned = cleaned.replace(/^```json/, "");
    cleaned = cleaned.replace(/^```/, "");
    cleaned = cleaned.replace(/```$/, "");
    return cleaned;
  };

  // Step 3: Process each chunk for BOTH individual scoring AND outfit creation
  const processChunk = async (chunk, retryCount = 0) => {
    const productListText = chunk
      .map(
        (product) =>
          `Product ID: ${product.id}, Title: ${product.title}, Service Name: ${product.service_name}, Category: ${product.product_category}, About: ${product.description}`
      )
      .join("\n\n");

    try {
      const response = await openai.chat.completions.create({
        model: "gpt-4o",
        messages: [
          {
            role: "system",
            content: `You are a travel fashion assistant that provides BOTH individual clothing recommendations AND complete outfit suggestions.
            
            Your tasks:
            1. Score each individual apparel item for trip relevance (1-10)
            2. Create complete outfit by combining 2-3 complementary items
            3. ONLY consider physical apparel items (clothes, shoes, accessories)
            4. Consider trip context for both individual items and outfit combinations`,
          },
          {
            role: "user",
            content: `
              Trip Description:
              ${dynamicTripDetail}

              Available Products:
              ${productListText}

              Provide BOTH individual item scores AND perfect outfit combination. 
              Respond with ONLY this JSON structure:
              {
                "individual_scores": [
                  {"id": 123, "score": 8},
                  {"id": 456, "score": 7}
                ],
                "outfits": 
                  {
                    "outfit_id": "outfit_1",
                    "score": 9,
                    "items": [
                      {"id": 123, "role": "top"},
                      {"id": 456, "role": "bottom"}
                    ],
                    "description": "combined description of all items in the outfit"
                  }
              }
              
              No explanations, no code blocks, just the JSON.
            `,
          },
        ],
        temperature: 0.6,
        max_tokens: 3000,
      });

      console.log(
        `Dual processing chunk completed. Tokens used: ${response.usage.total_tokens}`
      );

      const rawContent = response.choices[0].message.content;
      const cleanedContent = cleanResponse(rawContent);
      try {
        return JSON.parse(cleanedContent);
      } catch (parseError) {
        console.error("Failed to parse dual response JSON:", parseError);
        console.error("Raw response was:", rawContent);
        return { individual_scores: [], outfits: [] };
      }
    } catch (error) {
      if (error.status === 429 && retryCount < 3) {
        const delay = Math.pow(2, retryCount) * 1000;
        console.error(`Rate limit hit. Retrying in ${delay / 1000} seconds...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
        return processChunk(chunk, retryCount + 1);
      } else {
        console.error("Error processing dual chunk:", error);
        return { individual_scores: [], outfits: [] };
      }
    }
  };

  // Step 4: Process all chunks in parallel
  const results = await Promise.all(chunks.map((chunk) => processChunk(chunk)));

  // Step 5: Aggregate individual scores
  const allIndividualScores = results
    .flatMap((result) => result.individual_scores || [])
    .filter((item) => item && item.id && item.score);

  allIndividualScores.sort((a, b) => b.score - a.score);
  const top10Individual = allIndividualScores.slice(0, 10).map((item) => ({
    id: item.id,
    score: item.score,
    reason: `Individual relevance score: ${item.score}`,
  }));

  // Step 6: Aggregate outfits
  const allOutfits = results
    .flatMap((result) => result.outfits || [])
    .filter((outfit) => outfit && outfit.items && outfit.items.length > 0);

  allOutfits.sort((a, b) => b.score - a.score);
  const top5Outfits = allOutfits.slice(0, 5);

  // Step 7: Return comprehensive results
  return {
    // Original individual ranking (as requested)
    top_individual_items: top10Individual,

    // Complete outfit recommendations
    recommended_outfits: top5Outfits.map((outfit) => ({
      outfit_id: outfit.outfit_id,
      score: outfit.score,
      reason: outfit.reason,
      items: outfit.items,
    })),

    // Summary statistics
    summary: {
      total_individual_items_analyzed: allIndividualScores.length,
      total_outfits_created: allOutfits.length,
      recommendation_type: "both_individual_and_outfits",
    },
  };
}

// New function to extract business-specific data for serviceIds 4773, 4774, 4775, 4776
async function extractBusinessPulseData(userId, userChatrooms) {
  const businessPulseData = {};
  const businessServiceIds = ["4773", "4774", "4775", "4776"];

  // Check if user has chatrooms with business service IDs
  const businessChatrooms = {};
  for (const [chatroomId, messages] of Object.entries(userChatrooms)) {
    const hasBusinessService = messages.some((msg) =>
      businessServiceIds.includes(msg.serviceId)
    );
    if (hasBusinessService) {
      businessChatrooms[chatroomId] = messages.filter((msg) =>
        businessServiceIds.includes(msg.serviceId)
      );
    }
  }

  if (Object.keys(businessChatrooms).length > 0) {
    console.log(
      `💼 Extracting business pulse data for user ${userId} - Services ${businessServiceIds.join(
        ", "
      )}`
    );

    // Flatten all business messages and sort by time
    let allBusinessMessages = [];
    for (const messages of Object.values(businessChatrooms)) {
      allBusinessMessages = allBusinessMessages.concat(messages);
    }
    allBusinessMessages.sort((a, b) => (a.time || 0) - (b.time || 0));

    // Build context for business messages
    const chatContext = allBusinessMessages
      .filter((m) => m.text && m.text.trim())
      .map(
        (m) =>
          `Sender ${m.senderId}: ${m.text} (Service: ${m.serviceId}, Time: ${m.time})`
      )
      .join("\n")
      .slice(0, 8000); // Limit context

    if (chatContext.trim()) {
      const prompt = `
IMPORTANT: Respond ONLY with valid JSON in the exact format below. No explanations, no extra text, no markdown code blocks.

Analyze this business conversation to extract specific business metrics and data:

${chatContext}

Extract the following business information if present in the conversation. If any information is not found or unclear, use the default values provided:

OUTPUT FORMAT (valid JSON only):
{
  "business_pulse": {
    "meetings": 12,
    "new_deals": 3,
    "invested": "€2.1M",
    "brands": 5,
    "growth": "89%"
  }
}

EXTRACTION RULES:
- meetings: Look for mentions of meetings scheduled, attended, or planned (e.g., "meeting tomorrow", "had 5 meetings", "scheduled 3 calls"). Count total number mentioned. Default: 12
- new_deals: Look for new business deals, contracts signed, new clients acquired (e.g., "signed 2 deals", "got 3 new clients", "closed the contract"). Default: 3
- invested: Look for investment amounts, funding received, money invested (e.g., "invested €500K", "raised $2M", "funding of €1.5M"). Include currency symbol. Default: "€2.1M"
- brands: Look for mentions of brand partnerships, brand collaborations, new brand acquisitions (e.g., "partnered with 3 brands", "working with Nike", "added 2 brands"). Default: 5
- growth: Look for growth percentages, revenue increase, business expansion metrics (e.g., "grew by 45%", "increased 78%", "growth of 92%"). Include % symbol. Default: "89%"

IMPORTANT EXTRACTION GUIDELINES:
- Only extract numbers and percentages that are explicitly mentioned in the conversation
- For invested amounts, preserve the original currency mentioned (€, $, £, etc.)
- For growth, look for percentage increases, revenue growth, user growth, or business expansion metrics
- If multiple values are mentioned for the same metric, use the most recent or highest value
- Use defaults only when no relevant information is found in the conversation
- Focus on concrete business metrics, not vague references

Only extract information that is explicitly mentioned in the business conversation context. Use defaults for missing information.
      `;

      try {
        const completion = await openai.chat.completions.create({
          model: "gpt-4",
          messages: [
            {
              role: "system",
              content:
                "You extract specific business metrics from business-related conversations. Always respond with valid JSON only, no code blocks or explanations. Focus on concrete numbers and percentages mentioned in the chat.",
            },
            { role: "user", content: prompt },
          ],
          temperature: 0.3,
        });

        const response = completion.choices[0].message.content.trim();
        // Clean the response to remove any markdown code blocks
        let cleanedResponse = response
          .replace(/```json\n?/g, "")
          .replace(/```\n?/g, "")
          .trim();

        try {
          const parsedData = JSON.parse(cleanedResponse);
          businessPulseData.business_pulse = parsedData.business_pulse;
          console.log(`✅ Extracted business pulse data for user ${userId}`);
        } catch (parseError) {
          console.warn(
            `⚠️ Failed to parse business pulse data for user ${userId}. Using defaults.`
          );
          businessPulseData.business_pulse = getDefaultBusinessPulseData();
        }
      } catch (error) {
        console.error(
          `❌ Error extracting business pulse data for user ${userId}:`,
          error
        );
        businessPulseData.business_pulse = getDefaultBusinessPulseData();
      }
    } else {
      businessPulseData.business_pulse = getDefaultBusinessPulseData();
    }
  } else {
    // No business service messages found, store default data
    businessPulseData.business_pulse = getDefaultBusinessPulseData();
  }

  return businessPulseData;
}

// Default data for business pulse
function getDefaultBusinessPulseData() {
  return {
    meetings: 12,
    new_deals: 3,
    invested: "€2.1M",
    brands: 5,
    growth: "89%",
  };
}
// Example usage (assuming 'pool' is your MySQL connection pool)
// getRelatedProductsBasedOnTrip(pool).catch(console.error);

async function getUserPreference(userId) {
  const [userPreferences] = await pool.query(
    `SELECT input_text from user_preferences_inputs WHERE user_id = ?`,
    [userId]
  );
  let str = "";
  userPreferences.forEach((element) => {
    str += element.input_text + ", ";
  });
  return str;
}

module.exports = {
  processAndSaveUserSummaries,
};
// getUserFCMToken();
