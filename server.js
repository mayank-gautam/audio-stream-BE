const fastify = require("fastify")();

fastify.register(require("@fastify/websocket"));

fastify.register(async function (fastify) {
  fastify.get("/ws", { websocket: true }, (connection, req) => {
    console.log("✅ Client connected!");

    connection.on("message", (msg) => {
      try {
        const data = JSON.parse(msg.toString());

        if (data.format === "pcm16") {
          const pcmBuffer = new Int16Array(data.data);
          const base64Audio = Buffer.from(pcmBuffer.buffer).toString("base64");

          console.log(base64Audio);

          connection.send(
            JSON.stringify({
              type: "pcm_received",
              status: "success",
              samples: pcmBuffer.length,
              sampleRate: data.sampleRate,
            })
          );
        } else if (data.type === "audio_data") {
          const audioBuffer = new Uint8Array(data.data);
          const base64Audio = Buffer.from(audioBuffer).toString("base64");
          console.log(base64Audio);
          
          connection.send(
            JSON.stringify({
              type: "audio_received",
              status: "success",
              bufferSize: audioBuffer.length,
            })
          );
        } else {
          console.log("📩 Received:", msg.toString());
        }
      } catch (error) {
        console.log("📩 Received (non-JSON):", msg.toString());
      }
    });

    connection.on("close", () => {
      console.log("❌ Client disconnected");
    });
  });
});

// Start server
fastify.listen({ port: 3001, host: "127.0.0.1" }, (err) => {
  if (err) throw err;
  console.log("🚀 Server running at http://127.0.0.1:3001");
  console.log("📡 WebSocket: ws://127.0.0.1:3001/ws");
});
