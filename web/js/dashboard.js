import "./protobuf.min.js";

const pb = window.protobuf;

// Use the exact package path from your proto file: com.trade.stream.common
pb.load("../common/src/main/proto/common.proto")
  .then(root => {
    const Inquiry = root.lookupType("com.trade.stream.common.Inquiry");
    setupWebSocket(Inquiry);
  })
  .catch(err => {
    console.error("Protobuf load error:", err);
  });

function setupWebSocket(Inquiry) {
  const ws = new WebSocket("ws://localhost:8080/ws/inquiries?client-1");

  // CRITICAL: Set binaryType so event.data is an ArrayBuffer, not a string
  ws.binaryType = "arraybuffer";
  const inquiryRows = new Map();
  ws.onopen = () => console.log("Connected to backend WebSocket");

  ws.onmessage = (event) => {
    try {
      // Convert raw data to Uint8Array for the decoder
      const buffer = new Uint8Array(event.data);
      const message = Inquiry.decode(buffer);

      // Convert to a plain object with specific formatting options
      const inquiryData = Inquiry.toObject(message, {
        longs: Number,
        enums: String, // Maps numeric status (2) to string ("DONE")
        defaults: true // Prevents undefined by filling missing fields with defaults
      });

      console.log("Decoded Inquiry:", inquiryData);
      const tbody = document.getElementById("inquiryTable");

      let rowInfo = inquiryRows.get(inquiryData.inquiryId);

            if (!rowInfo) {
              // First time: create row and 8 cells
              const row = document.createElement("tr");
              const cells = Array(8).fill().map(() => document.createElement("td"));
              cells.forEach(td => row.appendChild(td));

              // Optional: prepend to top for newest updates
              tbody.prepend(row);

              rowInfo = { row, cells };
              inquiryRows.set(inquiryData.inquiryId, rowInfo);

              // Limit table size
              if (tbody.children.length > 50) {
                const lastChild = tbody.lastChild;
                for (const [key, val] of inquiryRows.entries()) {
                  if (val.row === lastChild) {
                    inquiryRows.delete(key);
                    break;
                  }
                }
                tbody.removeChild(lastChild);
              }
            }
        const { row, cells } = rowInfo;

      if (inquiryData.status) {
        row.className = inquiryData.status.toLowerCase();
      }

       const sideClass = inquiryData.side === "BUY" ? "green-text" :
                      inquiryData.side === "SELL" ? "red-text" : "";

      cells[0].textContent = inquiryData.inquiryId || "";
      cells[1].textContent = inquiryData.instrumentId || "";
      cells[2].textContent = inquiryData.bookId || "";
      cells[3].textContent = inquiryData.quantity || 0;
      cells[4].textContent = (inquiryData.price / 100).toFixed(2) || 0;
      cells[5].textContent = inquiryData.side || "";
      cells[5].className = sideClass;
      cells[6].textContent = inquiryData.status || "";
      const position = inquiryData.position || 0;
      const positionValue = position / 100;
      cells[7].textContent = Math.abs(positionValue).toFixed(2);

      // Apply color based on sign
      if (positionValue > 0) {
        cells[7].className = "green-text";
      } else if (positionValue < 0) {
        cells[7].className = "red-text";
      } else {
        cells[7].className = ""; // neutral / zero
      }


      // Optional: move updated row to top only if not already first
      if (tbody.firstChild !== row) {
        tbody.prepend(row);
      }

    } catch (e) {
      console.error("Decoding error:", e);
    }
  };

  ws.onerror = (err) => console.error("WebSocket error:", err);
  ws.onclose = () => console.log("WebSocket connection closed");
}