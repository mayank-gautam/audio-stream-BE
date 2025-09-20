const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// Create HTTP server
const server = http.createServer();

// Create WebSocket server
const wss = new WebSocket.Server({ 
  server,
  perMessageDeflate: false
});

// Store connected clients
const clients = new Set();

// Audio data storage (in production, you might want to use a proper database or file system)
const audioSessions = new Map();

console.log('🎵 Audio Streaming Server Starting...');

wss.on('connection', (ws, req) => {
  const clientId = generateClientId();
  clients.add(ws);
  
  console.log(`📱 Client ${clientId} connected. Total clients: ${clients.size}`);
  
  // Send welcome message
  ws.send(JSON.stringify({
    type: 'connection',
    message: 'Connected to audio streaming server',
    clientId: clientId
  }));

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      handleMessage(ws, clientId, message);
    } catch (error) {
      console.error(`❌ Error parsing message from client ${clientId}:`, error.message);
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Invalid message format'
      }));
    }
  });

  ws.on('close', (code, reason) => {
    clients.delete(ws);
    // Clean up any ongoing audio session
    if (audioSessions.has(clientId)) {
      const session = audioSessions.get(clientId);
      if (session.writeStream) {
        session.writeStream.end();
      }
      audioSessions.delete(clientId);
    }
    console.log(`📱 Client ${clientId} disconnected. Code: ${code}, Reason: ${reason}. Total clients: ${clients.size}`);
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error for client ${clientId}:`, error.message);
  });
});

function handleMessage(ws, clientId, message) {
  switch (message.type) {
    case 'audio_data':
      handleAudioData(ws, clientId, message);
      break;
    
    case 'start_session':
      startAudioSession(ws, clientId, message);
      break;
    
    case 'end_session':
      endAudioSession(ws, clientId, message);
      break;
    
    case 'ping':
      ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
      break;
    
    default:
      console.log(`📦 Unknown message type from client ${clientId}:`, message.type);
      ws.send(JSON.stringify({
        type: 'error',
        message: `Unknown message type: ${message.type}`
      }));
  }
}

function handleAudioData(ws, clientId, message) {
  try {
    const { data, timestamp } = message;
    const audioBuffer = Buffer.from(data);
    
    console.log(`🎵 Received audio chunk from client ${clientId}: ${audioBuffer.length} bytes at ${new Date(timestamp).toISOString()}`);
    
    // Get or create audio session for this client
    let session = audioSessions.get(clientId);
    if (!session) {
      session = createAudioSession(clientId);
      audioSessions.set(clientId, session);
    }
    
    // Write audio data to file (you can modify this to process audio instead)
    if (session.writeStream) {
      session.writeStream.write(audioBuffer);
    }
    
    // Update session stats
    session.totalChunks++;
    session.totalBytes += audioBuffer.length;
    session.lastActivity = timestamp;
    
    // Process audio data (placeholder for your audio processing logic)
    processAudioChunk(clientId, audioBuffer, timestamp);
    
    // Send acknowledgment back to client
    ws.send(JSON.stringify({
      type: 'audio_ack',
      timestamp: timestamp,
      bytesReceived: audioBuffer.length,
      totalBytes: session.totalBytes,
      totalChunks: session.totalChunks
    }));
    
  } catch (error) {
    console.error(`❌ Error handling audio data from client ${clientId}:`, error.message);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Error processing audio data'
    }));
  }
}

function createAudioSession(clientId) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `audio_session_${clientId}_${timestamp}.webm`;
  const filepath = path.join(__dirname, 'audio_recordings', filename);
  
  // Ensure recordings directory exists
  const recordingsDir = path.join(__dirname, 'audio_recordings');
  if (!fs.existsSync(recordingsDir)) {
    fs.mkdirSync(recordingsDir, { recursive: true });
  }
  
  const session = {
    clientId,
    startTime: Date.now(),
    filename,
    filepath,
    writeStream: fs.createWriteStream(filepath),
    totalChunks: 0,
    totalBytes: 0,
    lastActivity: Date.now()
  };
  
  console.log(`🎵 Created new audio session for client ${clientId}: ${filename}`);
  
  return session;
}

function startAudioSession(ws, clientId, message) {
  console.log(`🎵 Starting audio session for client ${clientId}`);
  
  const session = createAudioSession(clientId);
  audioSessions.set(clientId, session);
  
  ws.send(JSON.stringify({
    type: 'session_started',
    sessionId: clientId,
    filename: session.filename
  }));
}

function endAudioSession(ws, clientId, message) {
  const session = audioSessions.get(clientId);
  
  if (session) {
    console.log(`🎵 Ending audio session for client ${clientId}`);
    console.log(`   📊 Session stats: ${session.totalChunks} chunks, ${session.totalBytes} bytes, ${((Date.now() - session.startTime) / 1000).toFixed(2)}s duration`);
    
    if (session.writeStream) {
      session.writeStream.end();
    }
    
    audioSessions.delete(clientId);
    
    ws.send(JSON.stringify({
      type: 'session_ended',
      sessionId: clientId,
      stats: {
        totalChunks: session.totalChunks,
        totalBytes: session.totalBytes,
        duration: Date.now() - session.startTime
      }
    }));
  } else {
    ws.send(JSON.stringify({
      type: 'error',
      message: 'No active session found'
    }));
  }
}

function processAudioChunk(clientId, audioBuffer, timestamp) {
  // Placeholder function for audio processing
  // Here you can implement:
  // - Audio analysis (volume, frequency analysis)
  // - Speech recognition
  // - Audio filtering or enhancement
  // - Real-time audio streaming to other clients
  // - Integration with AI services for transcription
  
  // Example: Simple volume analysis
  const volume = calculateAudioVolume(audioBuffer);
  
  if (volume > 0.1) { // Threshold for significant audio
    console.log(`🔊 Client ${clientId} audio activity detected (volume: ${(volume * 100).toFixed(1)}%)`);
  }
}

function calculateAudioVolume(buffer) {
  // Simple RMS calculation for audio volume
  let sum = 0;
  const samples = new Int16Array(buffer.buffer, buffer.byteOffset, buffer.byteLength / 2);
  
  for (let i = 0; i < samples.length; i++) {
    sum += samples[i] * samples[i];
  }
  
  return Math.sqrt(sum / samples.length) / 32768; // Normalize to 0-1 range
}

function generateClientId() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

// Cleanup function for graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down server...');
  
  // Close all audio sessions
  audioSessions.forEach((session, clientId) => {
    console.log(`🎵 Closing audio session for client ${clientId}`);
    if (session.writeStream) {
      session.writeStream.end();
    }
  });
  
  // Close all WebSocket connections
  clients.forEach(ws => {
    ws.close(1000, 'Server shutting down');
  });
  
  wss.close(() => {
    console.log('✅ Server shut down gracefully');
    process.exit(0);
  });
});

// Start the server
server.listen(3001, () => {
  console.log('🚀 Audio Streaming Server is running on port 3001');
  console.log('📡 WebSocket endpoint: ws://localhost:3001');
  console.log('🎵 Ready to receive audio streams...');
});

// Health check endpoint
server.on('request', (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      clients: clients.size,
      activeSessions: audioSessions.size,
      uptime: process.uptime()
    }));
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
  }
});