require("dotenv").config();
const fs = require("fs");
const https = require("https");
const { Server } = require("socket.io");
const { processAndSaveUserSummaries } = require("./chatSummary.js");
const cron = require("node-cron");
const socketAuth = require("./middleware/socketAuth");
const chatController = require("./controllers/chatController");
const { main } = require("./queue-processor.js");

// Load SSL certificate and key
const options = {
  key: fs.readFileSync("/etc/ssl/vps.amslux.app/vps.amslux.app.key"),
  cert: fs.readFileSync("/etc/ssl/vps.amslux.app/vps_amslux_app.crt"),
  ca: fs.readFileSync("/etc/ssl/vps.amslux.app/vps_amslux_app.ca-bundle"),
};
// Create HTTPS server
const httpsServer = https.createServer(options);
const io = new Server(httpsServer, {
  cors: { origin: "*" },
});

// Socket middleware
io.use(socketAuth);

// Socket event listeners
io.on("connection", (socket) => {
  socket.emit("user_data", { userId: socket.user.sub });

  socket.on("join_conversation", (data) =>
    chatController.joinConversation(socket, data)
  );
  socket.on("send_message", (data) =>
    chatController.sendMessage(io, socket, data)
  );
  socket.on("typing", (data) => chatController.typing(io, socket, data));
  socket.on("chat_overview", (data) => chatController.chatOverview(socket));
  socket.on("load_more_messages", (data) =>
    chatController.loadMoreMessages(socket, data)
  );
  socket.on("agent_status", (data) => chatController.agentStatus(io, socket));
  socket.on("mark_as_read", (data) => chatController.markAsRead(socket, data));
  socket.on("respond_to_request", (data) =>
    chatController.respondToChatRequest(io, socket, data)
  );

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.user);
  });

  socket.on("error", (err) => {
    console.error("Socket error:", err.message);
  });
});

// Start the HTTPS server
const PORT = process.env.PORT || 3000;
httpsServer.listen(PORT, async () => {
  // cron.schedule("0 * * * *", async () => {
  //   await processAndSaveUserSummaries();
  // });
  // // await processAndSaveUserSummaries();
  // console.log(`Secure Socket.IO server running on port ${PORT}`);
  main().catch(console.error);
});
