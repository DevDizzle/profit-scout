import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

// Custom function to generate a unique key
function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

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

    // Step 1: Show initial message
    setMessages((prev) => [
      ...prev,
      {
        id: generateUniqueId(),
        text: `ProfitScout is analyzing ${ticker}...`,
        type: "bot",
        timestamp: new Date().toLocaleString(),
      },
    ]);

    try {
      // Step 2: Fetch quantitative analysis
      const quantitativeRes = await fetch(`${backendUrl}/quantative/analyze_stock/${ticker}`);
      if (!quantitativeRes.ok)
        throw new Error(`Quantitative HTTP Error: ${quantitativeRes.status}`);
      const quantitativeData = await quantitativeRes.json();
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `Quantitative Metrics for ${ticker}:\n${JSON.stringify(
            quantitativeData.quantitative_analysis,
            null,
            2
          )}`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);

      // Step 3: Fetch qualitative analysis
      const qualitativeRes = await fetch(`${backendUrl}/qualitative/analyze_sec/${ticker}`);
      if (!qualitativeRes.ok)
        throw new Error(`Qualitative HTTP Error: ${qualitativeRes.status}`);
      const qualitativeData = await qualitativeRes.json();
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `Qualitative Analysis for ${ticker}:\n${qualitativeData.qualitative_analysis}`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);

      // Step 4: Synthesize the analyses
      const synthesisRes = await fetch(`${backendUrl}/synthesizer/synthesize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker,
          yahoo_analysis: quantitativeData.quantitative_analysis,
          sec_analysis: qualitativeData.qualitative_analysis,
        }),
      });
      if (!synthesisRes.ok)
        throw new Error(`Synthesis HTTP Error: ${synthesisRes.status}`);
      const synthesisData = await synthesisRes.json();
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `📊 Final Analysis for ${ticker}:\n${synthesisData.synthesis}`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
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
        id: generateUniqueId(),
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
              <div
                className={`inline-block p-2 rounded-lg ${
                  msg.type === "user" ? "bg-blue-500 text-white" : "bg-gray-200 text-gray-900"
                }`}
              >
                {msg.text}
              </div>
            </div>
          ))}
        </div>
        <div className="flex space-x-2">
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter stock ticker (e.g., AMZN)"
          />
          <Button onClick={handleSend} disabled={isProcessing}>
            Send
          </Button>
        </div>
      </div>
    </div>
  );
}
