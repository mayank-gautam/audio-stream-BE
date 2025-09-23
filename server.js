const fastify = require("fastify");
const WebSocket = require("ws");
const fs = require("fs").promises;
const path = require("path");
const EventEmitter = require("events");

// Configuration constants
const CONFIG = {
  PORT: process.env.PORT || 3001,
  MAX_CONNECTIONS: parseInt(process.env.MAX_CONNECTIONS) || 1000,
  HEARTBEAT_INTERVAL: 30000, // 30 seconds
  AUDIO_RECORDING_DIR: path.join(__dirname, "audio_recordings"),
  MAX_FILE_SIZE: 100 * 1024 * 1024, // 100MB per session
  ALLOWED_AUDIO_FORMATS: ["webm", "wav", "mp3"],
  SESSION_TIMEOUT: 5 * 60 * 1000, // 5 minutes
};

class AudioStreamingServer extends EventEmitter {
  constructor() {
    super();
    this.app = fastify({
      logger: {
        level: 'info',
        serializers: {
          req: (req) => ({
            method: req.method,
            url: req.url,
            headers: req.headers,
            hostname: req.hostname,
            remoteAddress: req.ip,
            remotePort: req.socket.remotePort
          })
        }
      },
      bodyLimit: 10485760, // 10MB
      trustProxy: true
    });
    
    this.server = null;
    this.wss = null;
    this.clients = new Map(); // clientId -> { ws, session, isAlive }
    this.audioSessions = new Map(); // clientId -> session data
    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      totalBytesProcessed: 0,
      uptime: Date.now(),
    };

    this.init();
  }

  async init() {
    await this.setupDirectories();
  }

  async setupDirectories() {
    try {
      await fs.access(CONFIG.AUDIO_RECORDING_DIR);
    } catch {
      await fs.mkdir(CONFIG.AUDIO_RECORDING_DIR, { recursive: true });
    }
  }


  setupWebSocket(server) {
    this.wss = new WebSocket.Server({
      server: server,
      clientTracking: false, // We'll handle tracking manually
      perMessageDeflate: {
        zlibDeflateOptions: {
          threshold: 1024, // Only compress messages > 1KB
          concurrencyLimit: 10,
        },
      },
    });

    this.wss.on("connection", (ws, req) => {
      if (this.clients.size >= CONFIG.MAX_CONNECTIONS) {
        console.log("⚠️ Max connections reached, rejecting new connection");
        ws.close(1013, "Server overloaded");
        return;
      }

      const clientId = this.generateClientId();
      const clientIP = req.socket.remoteAddress;

      // Initialize client data
      this.clients.set(clientId, {
        ws,
        isAlive: true,
        joinTime: Date.now(),
        lastActivity: Date.now(),
        ip: clientIP,
      });

      this.stats.totalConnections++;
      this.stats.activeConnections = this.clients.size;

      console.log(
        `📱 Client ${clientId} connected from ${clientIP}. Total: ${this.clients.size}`
      );

      // Send welcome message
      this.sendToClient(clientId, {
        type: "connection",
        message: "Connected to audio streaming server",
        clientId,
        serverTime: Date.now(),
        config: {
          maxFileSize: CONFIG.MAX_FILE_SIZE,
          sessionTimeout: CONFIG.SESSION_TIMEOUT,
          supportedFormats: CONFIG.ALLOWED_AUDIO_FORMATS,
        },
      });

      // Set up event handlers
      ws.on("message", (data) => this.handleMessage(clientId, data));
      ws.on("close", (code, reason) =>
        this.handleDisconnection(clientId, code, reason)
      );
      ws.on("error", (error) => this.handleWebSocketError(clientId, error));
      ws.on("pong", () => this.handlePong(clientId));

      this.emit("clientConnected", { clientId, ip: clientIP });
    });

    this.wss.on("error", (error) => {
      console.error("❌ WebSocket Server Error:", error);
    });
  }

  handleMessage(clientId, data) {
    try {
      const client = this.clients.get(clientId);
      if (!client) return;

      client.lastActivity = Date.now();

      // Handle both JSON and binary data
      let message;
      if (data instanceof Buffer && data.length > 0) {
        // Try to parse as JSON first
        try {
          message = JSON.parse(data.toString());
        } catch {
          // If not JSON, treat as raw audio data
          this.handleRawAudioData(clientId, data);
          return;
        }
      } else {
        message = JSON.parse(data.toString());
      }

      this.routeMessage(clientId, message);
    } catch (error) {
      console.error(
        `❌ Error handling message from ${clientId}:`,
        error.message
      );
      this.sendErrorToClient(clientId, "Invalid message format");
    }
  }

  routeMessage(clientId, message) {
    const handlers = {
      audio_data: () => this.handleAudioDataMessage(clientId, message),
      start_session: () => this.startAudioSession(clientId, message),
      end_session: () => this.endAudioSession(clientId),
      ping: () => this.handlePing(clientId),
      get_status: () => this.sendSessionStatus(clientId),
    };

    const handler = handlers[message.type];
    if (handler) {
      handler();
    } else {
      this.sendErrorToClient(clientId, `Unknown message type: ${message.type}`);
    }
  }

  handleAudioDataMessage(clientId, message) {
    const { data, timestamp, format = "webm" } = message;

    if (!data) {
      this.sendErrorToClient(clientId, "No audio data provided");
      return;
    }

    if (!CONFIG.ALLOWED_AUDIO_FORMATS.includes(format)) {
      this.sendErrorToClient(clientId, `Unsupported audio format: ${format}`);
      return;
    }

    try {
      const audioBuffer = Buffer.from(data, "base64");
      this.processAudioBuffer(clientId, audioBuffer, timestamp || Date.now());
    } catch (error) {
      console.error(`❌ Error processing audio data for ${clientId}:`, error);
      this.sendErrorToClient(clientId, "Failed to process audio data");
    }
  }

  handleRawAudioData(clientId, audioBuffer) {
    this.processAudioBuffer(clientId, audioBuffer, Date.now());
  }

  async processAudioBuffer(clientId, audioBuffer, timestamp) {
    let session = this.audioSessions.get(clientId);

    if (!session) {
      session = await this.createAudioSession(clientId);
      this.audioSessions.set(clientId, session);
    }

    // Check file size limit
    if (session.totalBytes + audioBuffer.length > CONFIG.MAX_FILE_SIZE) {
      this.sendErrorToClient(clientId, "Session file size limit exceeded");
      this.forceEndSession(clientId);
      return;
    }

    try {
      // Write to file asynchronously
      if (session.writeStream) {
        await new Promise((resolve, reject) => {
          session.writeStream.write(audioBuffer, (error) => {
            if (error) reject(error);
            else resolve();
          });
        });
      }

      // Update session stats
      session.totalChunks++;
      session.totalBytes += audioBuffer.length;
      session.lastActivity = timestamp;
      this.stats.totalBytesProcessed += audioBuffer.length;

      // Process audio chunk for real-time analysis
      this.analyzeAudioChunk(clientId, audioBuffer);

      // Send acknowledgment
      this.sendToClient(clientId, {
        type: "audio_ack",
        timestamp,
        bytesReceived: audioBuffer.length,
        totalBytes: session.totalBytes,
        totalChunks: session.totalChunks,
        sessionDuration: timestamp - session.startTime,
      });
    } catch (error) {
      console.error(`❌ Error writing audio for ${clientId}:`, error);
      this.sendErrorToClient(clientId, "Failed to save audio data");
    }
  }

  async createAudioSession(clientId) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `audio_session_${clientId}_${timestamp}.webm`;
    const filepath = path.join(CONFIG.AUDIO_RECORDING_DIR, filename);

    const writeStream = require("fs").createWriteStream(filepath);

    const session = {
      clientId,
      startTime: Date.now(),
      filename,
      filepath,
      writeStream,
      totalChunks: 0,
      totalBytes: 0,
      lastActivity: Date.now(),
      format: "webm",
    };

    writeStream.on("error", (error) => {
      console.error(`❌ Write stream error for ${clientId}:`, error);
      this.forceEndSession(clientId);
    });

    console.log(`🎙️ Created audio session for ${clientId}: ${filename}`);
    return session;
  }

  startAudioSession(clientId, options = {}) {
    if (this.audioSessions.has(clientId)) {
      this.sendErrorToClient(clientId, "Session already active");
      return;
    }

    this.createAudioSession(clientId)
      .then((session) => {
        this.audioSessions.set(clientId, session);

        this.sendToClient(clientId, {
          type: "session_started",
          sessionId: clientId,
          filename: session.filename,
          startTime: session.startTime,
          maxFileSize: CONFIG.MAX_FILE_SIZE,
        });

        console.log(`🎬 Started audio session for ${clientId}`);
      })
      .catch((error) => {
        console.error(`❌ Failed to start session for ${clientId}:`, error);
        this.sendErrorToClient(clientId, "Failed to start audio session");
      });
  }

  endAudioSession(clientId) {
    const session = this.audioSessions.get(clientId);
    if (!session) {
      this.sendErrorToClient(clientId, "No active session found");
      return false;
    }

    return this.finalizeSession(clientId, session);
  }

  forceEndSession(clientId) {
    const session = this.audioSessions.get(clientId);
    if (!session) return false;

    return this.finalizeSession(clientId, session, true);
  }

  finalizeSession(clientId, session, forced = false) {
    try {
      if (session.writeStream && !session.writeStream.destroyed) {
        session.writeStream.end();
      }

      this.audioSessions.delete(clientId);

      const duration = Date.now() - session.startTime;
      const stats = {
        totalChunks: session.totalChunks,
        totalBytes: session.totalBytes,
        duration,
        filename: session.filename,
      };

      this.sendToClient(clientId, {
        type: "session_ended",
        sessionId: clientId,
        stats,
        forced,
      });

      console.log(
        `🏁 ${
          forced ? "Force " : ""
        }Ended audio session for ${clientId} - Duration: ${Math.round(
          duration / 1000
        )}s, Size: ${(stats.totalBytes / 1024).toFixed(1)}KB`
      );

      this.emit("sessionEnded", { clientId, stats, forced });
      return true;
    } catch (error) {
      console.error(`❌ Error finalizing session for ${clientId}:`, error);
      return false;
    }
  }

  analyzeAudioChunk(clientId, audioBuffer) {
    try {
      const volume = this.calculateAudioVolume(audioBuffer);

      if (volume > 0.1) {
        console.log(
          `🔊 Client ${clientId} audio activity (volume: ${(
            volume * 100
          ).toFixed(1)}%)`
        );

        this.sendToClient(clientId, {
          type: "audio_analysis",
          volume: volume,
          timestamp: Date.now(),
        });
      }
    } catch (error) {
      console.error(`❌ Error analyzing audio for ${clientId}:`, error);
    }
  }

  calculateAudioVolume(buffer) {
    if (buffer.length < 2) return 0;

    let sum = 0;
    const samples = new Int16Array(
      buffer.buffer,
      buffer.byteOffset,
      Math.floor(buffer.byteLength / 2)
    );

    for (let i = 0; i < samples.length; i++) {
      sum += samples[i] * samples[i];
    }

    return Math.sqrt(sum / samples.length) / 32768;
  }

  handlePing(clientId) {
    this.sendToClient(clientId, {
      type: "pong",
      timestamp: Date.now(),
      serverTime: Date.now(),
    });
  }

  handlePong(clientId) {
    const client = this.clients.get(clientId);
    if (client) {
      client.isAlive = true;
      client.lastActivity = Date.now();
    }
  }

  sendSessionStatus(clientId) {
    const session = this.audioSessions.get(clientId);
    const client = this.clients.get(clientId);

    this.sendToClient(clientId, {
      type: "status",
      hasActiveSession: !!session,
      sessionInfo: session
        ? {
            filename: session.filename,
            startTime: session.startTime,
            totalBytes: session.totalBytes,
            totalChunks: session.totalChunks,
            duration: Date.now() - session.startTime,
          }
        : null,
      connectionInfo: client
        ? {
            joinTime: client.joinTime,
            lastActivity: client.lastActivity,
            connectionDuration: Date.now() - client.joinTime,
          }
        : null,
    });
  }

  handleDisconnection(clientId, code, reason) {
    const client = this.clients.get(clientId);
    if (!client) return;

    this.clients.delete(clientId);
    this.stats.activeConnections = this.clients.size;

    // Clean up any active session
    if (this.audioSessions.has(clientId)) {
      this.forceEndSession(clientId);
    }

    console.log(
      `📱 Client ${clientId} disconnected (Code: ${code}, Reason: ${reason}). Total: ${this.clients.size}`
    );
    this.emit("clientDisconnected", { clientId, code, reason });
  }

  handleWebSocketError(clientId, error) {
    console.error(`❌ WebSocket error for client ${clientId}:`, error.message);

    const client = this.clients.get(clientId);
    if (client && client.ws.readyState === WebSocket.OPEN) {
      this.sendErrorToClient(clientId, "Connection error occurred");
    }
  }

  sendToClient(clientId, message) {
    const client = this.clients.get(clientId);
    if (!client || client.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    try {
      client.ws.send(JSON.stringify(message));
      return true;
    } catch (error) {
      console.error(`❌ Error sending to client ${clientId}:`, error);
      return false;
    }
  }

  sendErrorToClient(clientId, errorMessage) {
    this.sendToClient(clientId, {
      type: "error",
      message: errorMessage,
      timestamp: Date.now(),
    });
  }

  broadcast(message, excludeClientId = null) {
    let successCount = 0;

    for (const [clientId, client] of this.clients) {
      if (
        clientId !== excludeClientId &&
        client.ws.readyState === WebSocket.OPEN
      ) {
        try {
          client.ws.send(JSON.stringify(message));
          successCount++;
        } catch (error) {
          console.error(`❌ Error broadcasting to client ${clientId}:`, error);
        }
      }
    }

    return successCount;
  }

  startHeartbeat() {
    setInterval(() => {
      const deadClients = [];

      for (const [clientId, client] of this.clients) {
        if (!client.isAlive) {
          deadClients.push(clientId);
          continue;
        }

        client.isAlive = false;
        if (client.ws.readyState === WebSocket.OPEN) {
          client.ws.ping();
        }
      }

      // Clean up dead connections
      deadClients.forEach((clientId) => {
        console.log(`💀 Removing dead client: ${clientId}`);
        const client = this.clients.get(clientId);
        if (client) {
          client.ws.terminate();
          this.handleDisconnection(clientId, 1006, "Heartbeat timeout");
        }
      });
    }, CONFIG.HEARTBEAT_INTERVAL);
  }

  startSessionCleanup() {
    setInterval(() => {
      const now = Date.now();
      const staleSessionIds = [];

      for (const [clientId, session] of this.audioSessions) {
        if (now - session.lastActivity > CONFIG.SESSION_TIMEOUT) {
          staleSessionIds.push(clientId);
        }
      }

      staleSessionIds.forEach((clientId) => {
        console.log(`🧹 Cleaning up stale session: ${clientId}`);
        this.forceEndSession(clientId);
      });
    }, CONFIG.SESSION_TIMEOUT / 2); // Check every 2.5 minutes
  }

  calculateAverageSessionDuration() {
    if (this.audioSessions.size === 0) return 0;

    const now = Date.now();
    let totalDuration = 0;

    for (const session of this.audioSessions.values()) {
      totalDuration += now - session.startTime;
    }

    return Math.round(totalDuration / this.audioSessions.size / 1000); // in seconds
  }

  generateClientId() {
    return Math.random().toString(36).substring(2, 10).toUpperCase();
  }

  async start() {
    try {
      // Start Fastify server
      await this.app.listen({
        port: CONFIG.PORT,
        host: '0.0.0.0'
      });

      // Get the underlying HTTP server from Fastify
      this.server = this.app.server;

      // Setup WebSocket server using the Fastify HTTP server
      this.setupWebSocket(this.server);

      // Start background processes after server is running
      this.startHeartbeat();
      this.startSessionCleanup();

      console.log(
        `🚀 Audio Streaming Server started on http://localhost:${CONFIG.PORT}`
      );
      console.log(`📡 WebSocket endpoint: ws://localhost:${CONFIG.PORT}`);
      console.log(`📊 Max connections: ${CONFIG.MAX_CONNECTIONS}`);
      console.log(
        `💓 Heartbeat interval: ${CONFIG.HEARTBEAT_INTERVAL / 1000}s`
      );
      console.log(`📁 Audio recordings: ${CONFIG.AUDIO_RECORDING_DIR}`);
    } catch (error) {
      console.error("❌ Server startup error:", error);
      process.exit(1);
    }

    // Graceful shutdown
    const shutdown = () => this.shutdown();
    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  }

  async shutdown() {
    console.log("🛑 Shutting down server...");

    // Close all WebSocket connections
    for (const [clientId, client] of this.clients) {
      client.ws.close(1012, "Server shutting down");
    }

    // End all active sessions
    for (const clientId of this.audioSessions.keys()) {
      this.forceEndSession(clientId);
    }

    try {
      await this.app.close();
      console.log("✅ Server shutdown complete");
      process.exit(0);
    } catch (error) {
      console.error("❌ Error during shutdown:", error);
      process.exit(1);
    }
  }
}

// Create and start the server
const audioServer = new AudioStreamingServer();
audioServer.start().catch(console.error);

// Export for testing or external use
module.exports = AudioStreamingServer;