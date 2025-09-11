const { verifyToken } = require('../utils/jwt');

const socketAuth = (socket, next) => {
  try {
    const token = socket.handshake.auth.token;
    if (!token) return next(new Error('No token provided'));

    const user = verifyToken(token);
    socket.user = user;
    next();
  } catch (err) {
    return next(new Error('Invalid token'));
  }
};

module.exports = socketAuth;
