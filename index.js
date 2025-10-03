const fastify = require("fastify")();
const fs = require("fs");
const path = require("path");
const dayjs = require("dayjs");
const { BlobServiceClient } = require("@azure/storage-blob");
const winston = require("winston");

// =====================
// CONFIG
// =====================
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = process.env.CONTAINER_NAME || "call-logs";
const MAX_CONCURRENT_STREAMS = 500; // Limit open file streams
const FILE_ROTATE_SIZE = 50 * 1024 * 1024; // 50 MB per file

// =====================
// Logger
// =====================
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      (info) => `[${info.timestamp}] ${info.level.toUpperCase()}: ${info.message}`
    )
  ),
  transports: [new winston.transports.Console()],
});

// =====================
// Azure Setup
// =====================
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
containerClient.createIfNotExists();

// =====================
// State
// =====================
const callStreams = new Map(); // callId -> { stream, filePath, size, part }
let currentStreams = 0;

// =====================
// Helper: Upload with retry
// =====================
async function uploadWithRetry(filePath, blobName, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const blockBlobClient = containerClient.getBlockBlobClient(blobName);
      await blockBlobClient.uploadFile(filePath);
      logger.info(`Uploaded to Azure: ${blobName}`);
      return true;
    } catch (err) {
      logger.error(`Azure upload failed (attempt ${i + 1}): ${err.message}`);
      await new Promise((r) => setTimeout(r, 2000 * (i + 1))); // exponential backoff
    }
  }
  return false;
}

// =====================
// Helper: Create new stream
// =====================
function createNewStream(callId, part = 1) {
  const dateFolder = path.join(__dirname, "logs", dayjs().format("YYYY-MM-DD"));
  fs.mkdirSync(dateFolder, { recursive: true });

  const filePath = path.join(dateFolder, `${callId}_part${part}.txt`);
  const stream = fs.createWriteStream(filePath, { flags: "a" });

  // Stream error handling
  stream.on("error", (err) => {
    logger.error(`Stream error for callId=${callId}: ${err.message}`);
  });

  return { stream, filePath, size: 0, part };
}

// =====================
// WebSocket Setup
// =====================
fastify.register(require("@fastify/websocket"));

fastify.register(async (fastify) => {
  fastify.get("/ws", { websocket: true }, (connection) => {
    logger.info("Client connected");

    if (currentStreams >= MAX_CONCURRENT_STREAMS) {
      logger.warn("Max concurrent streams reached, closing connection");
      connection.socket.close(1013, "Server busy");
      return;
    }

    currentStreams++;

    const callId = Date.now() + "-" + Math.floor(Math.random() * 1000);
    let callData = createNewStream(callId);
    callStreams.set(callId, callData);

    // Backpressure-safe streaming
    connection.on("message", (msg) => {
      if (!callData) return;

      const chunk = msg.toString() + "\n";
      callData.size += Buffer.byteLength(chunk);

      // Rotate file if it exceeds FILE_ROTATE_SIZE
      if (callData.size > FILE_ROTATE_SIZE) {
        callData.stream.end();
        callData.part += 1;
        callData = createNewStream(callId, callData.part);
        callStreams.set(callId, callData);
      }

      const writeOk = callData.stream.write(chunk);
      if (!writeOk) {
        connection.pause();
        callData.stream.once("drain", () => connection.resume());
      }
    });

    // Handle disconnect
    connection.on("close", async () => {
      logger.info(`Client disconnected: callId=${callId}`);
      if (!callData) return;

      callData.stream.end(async () => {
        // Upload all parts
        for (let i = 1; i <= callData.part; i++) {
          const filePath = path.join(__dirname, "logs", dayjs().format("YYYY-MM-DD"), `${callId}_part${i}.txt`);
          const blobName = `${dayjs().format("YYYY-MM-DD")}/${callId}_part${i}.txt`;
          const uploaded = await uploadWithRetry(filePath, blobName);
          if (uploaded) fs.unlinkSync(filePath);
        }

        callStreams.delete(callId);
        currentStreams--;
        logger.info(`Finished processing callId=${callId}`);
      });
    });
  });
});

// =====================
// Start Server
// =====================
fastify.listen({ port: 3001, host: "0.0.0.0" }, (err) => {
  if (err) throw err;
  logger.info("Server running at http://0.0.0.0:3001");
});
