import { useState } from "react";
import { v4 as uuidv4 } from "uuid";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

interface Message {
  id: string;
  text: string;
  type: "user" | "bot";
  timestamp: string;
}

export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  const fetchSynthesis = async (ticker: string) => {
    setIsProcessing(true);
    // Show initial bot message
    setMessages((prev) => [
      ...prev,
      {
        id: uuidv4(),
        text: `ProfitScout is analyzing ${ticker}...`,
        type: "bot",
        timestamp: new Date().toLocaleString(),
      },
    ]);

    try {
      // Fetch quantitative analysis from the quantitative endpoint
      const quantitativeRes = await fetch(`${backendUrl}/quantative/analyze_stock/${ticker}`);
      if (!quantitativeRes.ok) throw new Error(`Quantitative HTTP Error: ${quantitativeRes.status}`);
      const quantitativeData = await quantitativeRes.json();

      // Fetch qualitative analysis from the qualitative endpoint
      const qualitativeRes = await fetch(`${backendUrl}/qualitative/analyze_sec/${ticker}`);
      if (!qualitativeRes.ok) throw new Error(`Qualitative HTTP Error: ${qualitativeRes.status}`);
      const qualitativeData = await qualitativeRes.json();

      // Combine both analyses and send to the synthesizer endpoint
      const synthesisRes = await fetch(`${backendUrl}/synthesizer/synthesize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker,
          yahoo_analysis: quantitativeData.quantitative_analysis,
          sec_analysis: qualitativeData.qualitative_analysis,
        }),
      });
      if (!synthesisRes.ok) throw new Error(`Synthesis HTTP Error: ${synthesisRes.status}`);
      const synthesisData = await synthesisRes.json();

      const formattedMessage = `
📊 **Final Analysis for ${ticker}:**

${synthesisData.synthesis}
      `;

      setMessages((prev) => [
        ...prev,
        {
          id: uuidv4(),
          text: formattedMessage,
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          id: uuidv4(),
          text: "Error fetching synthesis. Try again.",
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);
      console.error(error);
    }
    setIsProcessing(false);
  };

  const handleSend = () => {
    if (!query.trim()) return;
    // Add user message
    setMessages((prev) => [
      ...prev,
      {
        id: uuidv4(),
        text: query,
        type: "user",
        timestamp: new Date().toLocaleString(),
      },
    ]);
    fetchSynthesis(query);
    setQuery("");
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-2xl shadow-lg rounded-xl bg-white p-4 space-y-4">
        <div className="h-96 overflow-y-auto border rounded-lg p-3 bg-gray-50">
          {messages.map((msg) => (
            <div key={msg.id} className={`p-2 ${msg.type === "user" ? "text-right" : "text-left"}`}>
              <span className="block text-sm text-gray-500">{msg.timestamp}</span>
              <div className={`inline-block p-2 rounded-lg ${msg.type === "user" ? "bg-blue-500 text-white" : "bg-gray-200 text-gray-900"}`}>
                {msg.text}
              </div>
            </div>
          ))}
        </div>
        <div className="flex space-x-2">
          <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Enter stock ticker (e.g., AMZN)" />
          <Button onClick={handleSend} disabled={isProcessing}>Send</Button>
        </div>
      </div>
    </div>
  );
}
