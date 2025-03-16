import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

interface Message {
  id: number;
  text: string;
  type: "user" | "bot";
  timestamp: string;
}

export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  const fetchAnalysis = async (ticker: string) => {
    setIsProcessing(true);
    setMessages((prev) => [
      ...prev,
      { id: Date.now(), text: `ProfitScout is analyzing ${ticker}...`, type: "bot", timestamp: new Date().toLocaleString() },
    ]);

    try {
      const response = await fetch(`/agent1/analyze_stock/${ticker}`);
      const data = await response.json();

      const formattedMessage = `📈 Analysis for ${data.ticker}:
        - ROE: ${data.financial_ratios?.ROE || "N/A"}
        - Current Ratio: ${data.financial_ratios?.Current_Ratio || "N/A"}
        - Gross Margin: ${data.financial_ratios?.Gross_Margin || "N/A"}
        - P/E Ratio: ${data.financial_ratios?.P_E_Ratio || "N/A"}
        - Debt to Equity: ${data.financial_ratios?.Debt_to_Equity || "N/A"}
        - FCF Yield: ${data.financial_ratios?.FCF_Yield || "N/A"}

      📢 Conclusion: ${data.analysis}`;

      setMessages((prev) => [...prev, { id: Date.now(), text: formattedMessage, type: "bot", timestamp: new Date().toLocaleString() }]);
    } catch (error) {
      setMessages((prev) => [...prev, { id: Date.now(), text: "Error fetching analysis. Try again.", type: "bot", timestamp: new Date().toLocaleString() }]);
    }

    setIsProcessing(false);
  };

  const handleSend = () => {
    if (!query.trim()) return;
    setMessages((prev) => [...prev, { id: Date.now(), text: query, type: "user", timestamp: new Date().toLocaleString() }]);
    fetchAnalysis(query);
    setQuery("");
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-2xl shadow-lg rounded-xl bg-white p-4 space-y-4">
        <div className="h-96 overflow-y-auto border rounded-lg p-3 bg-gray-50">
          {messages.map((msg) => (
            <div key={msg.id} className={`p-2 ${msg.type === "user" ? "text-right" : "text-left"}`}>
              <span className="block text-sm text-gray-500">{msg.timestamp}</span>
              <div className={`inline-block p-2 rounded-lg ${msg.type === "user" ? "bg-blue-500 text-white" : "bg-gray-200 text-gray-900"}`}>{msg.text}</div>
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
