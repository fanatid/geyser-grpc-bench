async function main() {
  const transport = new WebTransport("https://geyser.lamports.dev:20002");
  console.log("WebTransport created");

  await transport.ready;
  console.log("WebTransport ready");

  const stream = await transport.createUnidirectionalStream();
  console.log("WebTransport request stream created");

  const request = JSON.stringify({
    streams: 1,
    max_backlog: 1024,
  });
  console.log(`request: ${request}`);
  const request_raw = new TextEncoder().encode(request);
  console.log(`request_raw: ${request_raw}`);

  const writer = stream.getWriter();
  writer.write(request_raw);
  await writer.close();
  console.log("WebTransport request stream closed");

  const uds = transport.incomingUnidirectionalStreams;
  const { done, value } = await uds.getReader().read();
  console.log(`WebTransport incoming stream, done: ${done}`);

  const reader = value.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      console.log("WebTransport incoming stream finished");
      break;
    }

    console.log(value);
  }
}

try {
  main();
} catch (error) {
  console.error("failed: ${error}");
}
