const db = require('../config/db');
const config = require('dotenv').config();
// Join a conversation and load chat history
exports.joinConversation = async (socket, data) => {
  const { conversationId } = data;
  const userId = socket.user.sub;
  
  try {
    // Initial load with pagination
    await loadMessages(socket, conversationId, 1);
    
    socket.join(conversationId);
  } catch (err) {
    socket.emit('conversation_error', { message: err.message });
  }
};

// Load more messages with pagination
exports.loadMoreMessages = async (socket, data) => {
  const { conversationId, page } = data;
  
  try {
    await loadMessages(socket, conversationId, page);
  } catch (err) {
    socket.emit('load_more_error', { message: err.message });
  }
};

// Helper function to load messages with pagination

async function loadMessages(socket, conversationId, page) {
  const limit = parseInt(config.parsed.chatMessagesPerPage, 10) || 30;
  const offset = (page - 1) * limit;

  // Fetch messages
  const [messages] = await db.query(
    `SELECT cm.*, 
            CONCAT(sender.first_name,' ',sender.last_name) AS sender_name, 
            sender.picture AS sender_avatar, 
            CONCAT(receiver.first_name,' ',receiver.last_name) AS receiver_name, 
            receiver.picture AS receiver_avatar, 
            cs.service_name AS service_name
     FROM chat_messages cm
     INNER JOIN users sender ON cm.sender_id = sender.id
     INNER JOIN users receiver ON cm.receiver_id = receiver.id
     INNER JOIN category_services cs ON cm.service_id = cs.id
     WHERE cm.room_id = ? 
     ORDER BY cm.created_at DESC
     LIMIT ? OFFSET ?`,
    [conversationId, limit, offset]
  );

  // Get total count for pagination
  const [[totalCount]] = await db.query(
    `SELECT COUNT(*) as total FROM chat_messages WHERE room_id = ?`,
    [conversationId]
  );

  // Get conversation info (you'll need to adapt this query to your DB schema)
  const [[conversation]] = await db.query(
    `SELECT r.id, s.title, r.type, r.service_id AS serviceId, 
            r.receiver_id AS managerId,r.room_id as conversationId,
            CONCAT(u.first_name, ' ', u.last_name) AS managerName,
            u.picture AS managerAvatar
     FROM chat_room r
     INNER JOIN users u ON r.receiver_id = u.id
     INNER JOIN category_services s ON s.id = r.service_id
     WHERE r.room_id = ?`,
    [conversationId]
  );
  const response = {
    conversation: {
      id: conversation.id,
      title: conversation.title,
      type: conversation.type,
      serviceId: conversation.serviceId,
      managerId: conversation.managerId,
      managerName: conversation.managerName,
      managerAvatar: conversation.managerAvatar,
      conversationId:conversationId
    },
    messages: messages
      .reverse() // To show oldest first
      .map(msg => ({
        id: msg.id,
        senderId: msg.sender_id,
        content: msg.body,
        timestamp: msg.created_at,
        status: msg.status || 'sent', // Default to 'sent' if status missing
      })),
    pagination: {
      total: totalCount.total,
      page: parseInt(page),
      limit,
      pages: Math.ceil(totalCount.total / limit),
      hasMore: (page * limit) < totalCount.total,
    }
  };
  console.log(response);
  if (page === 1) {
    socket.emit('conversation_history', response);
  } else {
    socket.emit('more_messages_loaded', response);
  }
}


// Send a message to a conversation
exports.sendMessage = async (io, socket, data) => {
  const { service_id, content } = data;
  const senderId = socket.user.sub;
  let receiver_id;
  try {
    const [userResult] = await db.query('SELECT user_type FROM users WHERE id = ?', [senderId]);
    const isAgent = userResult[0].user_type === 'agent';

    if (isAgent) {
      receiver_id = data.receiver_id;
    } else {
      const [agentResult] = await db.query("SELECT id FROM users WHERE user_type = 'agent' LIMIT 1");
      if (!agentResult.length) throw new Error('No agent available');
      receiver_id = agentResult[0].id;
    }

    const [roomResult] = await db.query(
      `SELECT room_id, status FROM chat_room 
       WHERE service_id = ? AND ((sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?))`,
      [service_id, senderId, receiver_id, receiver_id, senderId]
    );

    let conversationId;
    let isNew = false;
    let roomStatus = 'pending';

    if (roomResult.length > 0) {
      conversationId = roomResult[0].room_id;
      roomStatus = roomResult[0].status;
    } else {
      conversationId = new Date().getTime().toString();
      await db.query(
        `INSERT INTO chat_room (room_id, service_id, sender_id, receiver_id, created_at, status) 
         VALUES (?, ?, ?, ?, NOW(), 'pending')`,
        [conversationId, service_id, senderId, receiver_id]
      );
      isNew = true;
    }

    const [insertResult] = await db.query(
      `INSERT INTO chat_messages (room_id, service_id, sender_id, receiver_id, body, created_at) 
       VALUES (?, ?, ?, ?, ?, NOW())`,
      [conversationId, service_id, senderId, receiver_id, content]
    );

    const [messageResult] = await db.query(
      `SELECT cm.id, cm.room_id AS conversationId, cm.sender_id AS senderId,
      cm.receiver_id AS receiverId,cm.body AS content, cm.created_at AS timestamp, cm.status,
              CONCAT(sender.first_name,' ',sender.last_name) AS sender_name,
              CONCAT(receiver.first_name,' ',receiver.last_name) AS receiver_name,
              cs.service_name, sender.picture AS sender_avatar, receiver.picture AS receiver_avatar,cs.id as serviceId
       FROM chat_messages cm 
       INNER JOIN users sender ON cm.sender_id = sender.id 
       INNER JOIN users receiver ON cm.receiver_id = receiver.id 
       INNER JOIN category_services cs ON cm.service_id = cs.id 
       WHERE cm.id = ?`,
      [insertResult.insertId]
    );

    const message = messageResult[0];

    // Emit message normally only if conversation is accepted
    // if (roomStatus === 'accepted') {
      const messageToSend = {
        id: message.id,
        serviceId: message.serviceId,
        conversationId: conversationId, // Add this
        senderId: message.senderId,
        receiverId: message.receiverId,
        content: message.content,
        timestamp: message.timestamp,
        status: message.status || 'sent',
        sender_name: message.sender_name,
        receiver_name: message.receiver_name,
        service_name: message.service_name,
        sender_avatar: message.sender_avatar,
        receiver_avatar: message.receiver_avatar
    };
      io.to(conversationId).emit('new_message', messageToSend);

      
    // }

    // If it's a new conversation and pending, notify agent with a request
    // if (isNew || roomStatus === 'pending') {
    //   io.to(receiver_id).emit('chat_request', {
    //     conversationId,
    //     service_id,
    //     user: {
    //       id: senderId,
    //       name: message.sender_name,
    //       avatar: message.sender_avatar,
    //       receiver_id:receiver_id
    //     },
    //     message: content
    //   });
    // }

    try {
     
  
      const query = `
        SELECT cm.*, 
               CONCAT(sender.first_name, ' ', sender.last_name) AS sender_name, 
               CONCAT(receiver.first_name, ' ', receiver.last_name) AS receiver_name,
               sender.picture AS sender_avatar, 
               receiver.picture AS receiver_avatar,
	       receiver.id AS receiver_id,
               cs.service_name,
               cs.title,
               cs.description,
               cr.room_id,
               cr.status,
               cr.created_at AS requested_at,
               cr.sender_id,
               cr.receiver_id
        FROM chat_messages cm
        INNER JOIN chat_room cr ON cm.room_id = cr.room_id
        INNER JOIN (
          SELECT room_id, MAX(id) as last_message_id
          FROM chat_messages
          GROUP BY room_id
        ) grouped ON cm.id = grouped.last_message_id
        INNER JOIN users sender ON cm.sender_id = sender.id
        INNER JOIN users receiver ON cm.receiver_id = receiver.id
        INNER JOIN category_services cs ON cm.service_id = cs.id
        WHERE cm.room_id = ?
        ORDER BY cm.created_at DESC
      `;
  
      const [conversationsRaw] = await db.query(query, [conversationId]);
  
      const conversations = await Promise.all(conversationsRaw.map(async (conv) => {
        // Count unread messages for this room
        const [unreadResult] = await db.query(
          `SELECT COUNT(*) AS unreadCount FROM chat_messages 
           WHERE room_id = ? AND is_read = 0`,
          [conv.room_id]
        );
  
        // `Chat with ${conv.sender_id === userId ? conv.receiver_name : conv.sender_name}`
        return {
          id: conv.id,
          title: conv.title, // or customize this if needed
          description:conv.description ,
          category: conv.service_name, // Replace or map this properly if needed
          status: conv.status || 'In Progress',
	        receiver_id:conv.receiver_id,
	        receiver_name:conv.receiver_name,
          receiver_avatar:conv.receiver_avatar,
          requestedAt: conv.requested_at,
          conversationId: conv.room_id,
          unreadCount: unreadResult[0].unreadCount || 0,
          lastMessage: conv.body,
          lastMessageTime: conv.created_at,
        };
      }));
  
      io.to(conversationId).emit('update_overview', { conversations });

    } catch (err) {
      console.error(err);
      socket.emit('update_overview_error', { message: err.message });
    }

    // io.to(receiver_id).emit('update_overview');

  } catch (err) {
    socket.emit('message_error', { message: err.message });
  }
};


exports.respondToChatRequest = async (io, socket, data) => {
  const { conversationId, response } = data; // response = 'accept', 'reject', or 'complete'
  const userId = socket.user.sub;
  try {
    // Determine the status based on the response
    let status = '';
    if (response === 'accept') {
      status = 'accepted';
    } else if (response === 'reject') {
      status = 'rejected';
    } else if (response === 'complete') {
      status = 'completed';
    }

    // Update the chat status in the database
    await db.query(
      `UPDATE chat_room SET status = ? WHERE room_id = ?`,
      [status, conversationId]
    );

    // Get the sender_id and receiver_id of the original chat request
    const [[room]] = await db.query(
      `SELECT sender_id, receiver_id FROM chat_room WHERE room_id = ?`,
      [conversationId]
    );

    // Notify both sender and receiver based on the status
    if (status === 'accepted') {
      // Notify both sender and receiver about acceptance
      io.to(conversationId).emit('request_accepted', { conversationId, status });
      io.to(conversationId).emit('overview_update', { conversationId, status });
      // Emit the updated overview to both sender and receiver
    } else if (status === 'rejected') {
      // Notify sender about rejection
      io.to(conversationId).emit('request_rejected', { conversationId, status });

      // Optionally notify the receiver about the rejection

      // Emit the updated overview to both sender and receiver
      io.to(conversationId).emit('overview_update', { conversationId, status });
    } else if (status === 'completed') {
      // Notify both sender and receiver about the completion
      io.to(conversationId).emit('chat_completed', { conversationId, status });

      // Emit the updated overview to both sender and receiver
      io.to(conversationId).emit('overview_update', { conversationId, status });
    }

    // Notify the agent (user) that their response has been sent
    io.to(conversationId).emit('chat_request_response_sent', { conversationId, status });
    
  } catch (err) {
    socket.emit('chat_request_response_error', { message: err.message });
  }
};



// [Keep other existing functions...]
// Handle typing status
exports.typing = (io, socket, data) => {
  const { conversationId, isTyping, userId, userName } = data;
  console.log(data);
  socket.to(conversationId).emit('typing_status', {
    conversationId,
    userId,
    userName,
    isTyping
  });
};


// exports.chatOverview = async (socket) => {
//   const userId = socket.user.sub;

//   try {
//     const [userResult] = await db.query('SELECT user_type FROM users WHERE id = ?', [userId]);
//     const isAgent = userResult[0].user_type === 'agent';

//     const query = `
//       SELECT cm.*, 
//              CONCAT(sender.first_name, ' ', sender.last_name) AS sender_name, 
//              CONCAT(receiver.first_name, ' ', receiver.last_name) AS receiver_name,
//              sender.picture AS sender_avatar, 
//              receiver.picture AS receiver_avatar,
//              cs.service_name,
//              cr.room_id
//       FROM chat_messages cm
//       INNER JOIN chat_room cr ON cm.room_id = cr.room_id
//       INNER JOIN (
//         SELECT room_id, MAX(id) as last_message_id
//         FROM chat_messages
//         GROUP BY room_id
//       ) grouped ON cm.id = grouped.last_message_id
//       INNER JOIN users sender ON cm.sender_id = sender.id
//       INNER JOIN users receiver ON cm.receiver_id = receiver.id
//       INNER JOIN category_services cs ON cm.service_id = cs.id
//       WHERE (cr.sender_id = ? OR cr.receiver_id = ?)
//       ORDER BY cm.created_at DESC
//     `;

//     const [conversations] = await db.query(query, [userId, userId]);
    
//     // For agent view, add user names to the header
//     if (isAgent) {
//       conversations.forEach(conv => {
//         const otherUserId = conv.sender_id === userId ? conv.receiver_id : conv.sender_id;
//         const otherUserName = conv.sender_id === userId ? conv.receiver_name : conv.sender_name;
//         conv.other_user = { id: otherUserId, name: otherUserName };
//       });
//     }

//     socket.emit('chat_overview', conversations);

//   } catch (err) {
//     socket.emit('chat_overview_error', { message: err.message });
//   }
// };

exports.chatOverview = async (socket) => {
  const userId = socket.user.sub;
  try {
    const [userResult] = await db.query('SELECT user_type FROM users WHERE id = ?', [userId]);
    const isAgent = userResult[0].user_type === 'agent';

    const query = `
      SELECT cm.*, 
             CONCAT(sender.first_name, ' ', sender.last_name) AS sender_name, 
             CONCAT(receiver.first_name, ' ', receiver.last_name) AS receiver_name,
             sender.picture AS sender_avatar, 
             receiver.picture AS receiver_avatar,
	           receiver.id AS receiver_id,
             cs.service_name,
             cs.title,
             cs.description,
             cr.room_id,
             cr.status,
             cr.created_at AS requested_at,
             cr.sender_id,
             cr.receiver_id
      FROM chat_messages cm
      INNER JOIN chat_room cr ON cm.room_id = cr.room_id
      INNER JOIN (
        SELECT room_id, MAX(id) as last_message_id
        FROM chat_messages
        GROUP BY room_id
      ) grouped ON cm.id = grouped.last_message_id
      INNER JOIN users sender ON cm.sender_id = sender.id
      INNER JOIN users receiver ON cm.receiver_id = receiver.id
      INNER JOIN category_services cs ON cm.service_id = cs.id
      WHERE (cr.sender_id = ? OR cr.receiver_id = ?)
      ORDER BY cm.created_at DESC
    `;

    const [conversationsRaw] = await db.query(query, [userId, userId]);

    const conversations = await Promise.all(conversationsRaw.map(async (conv) => {
      // Count unread messages for this room
      const [unreadResult] = await db.query(
        `SELECT COUNT(*) AS unreadCount FROM chat_messages 
         WHERE room_id = ? AND receiver_id = ? AND is_read = 0`,
        [conv.room_id, userId]
      );

      // `Chat with ${conv.sender_id === userId ? conv.receiver_name : conv.sender_name}`
      return {
        id: conv.id,
        title: conv.title, // or customize this if needed
        description:conv.description ,
        category: conv.service_name, // Replace or map this properly if needed
        status: conv.status || 'In Progress',
	      receiver_id:conv.receiver_id,
        receiver_name:conv.receiver_name,
        receiver_avatar:conv.receiver_avatar,
        requestedAt: conv.requested_at,
        conversationId: conv.room_id,
        unreadCount: unreadResult[0].unreadCount || 0,
        lastMessage: conv.body,
        lastMessageTime: conv.created_at,
      };
    }));

    socket.emit('chat_overview', { conversations });

  } catch (err) {
    console.error(err);
    socket.emit('chat_overview_error', { message: err.message });
  }
};


// Track agent status
exports.agentStatus = async (io, socket) => {
  const userId = socket.user.sub;
  try {
    const [userResult] = await db.query('SELECT user_type FROM users WHERE id = ?', [userId]);
    const isAgent = userResult[0].user_type === 'agent';
    console.log(isAgent,'Agent');

    if (isAgent) {
      // Notify all users that agent is online
      io.emit('agent_status', { status: 'online' });
      
      socket.on('disconnect', () => {
        io.emit('agent_status', { status: 'offline' });
      });
    }
  } catch (err) {
    console.error('Agent status error:', err);
  }
};

// Mark messages as read
exports.markAsRead = async (socket, data) => {
  const { conversationId } = data;
  const userId = socket.user.sub;

  try {
    await db.query(
      `UPDATE chat_messages 
       SET is_read = true 
       WHERE room_id = ? AND receiver_id = ? AND is_read = false`,
      [conversationId, userId]
    );
    console.log('markAsRead call back');
    socket.emit('messages_read', { success: true });
  } catch (err) {
    socket.emit('messages_read', { success: false, message: err.message });
  }
};