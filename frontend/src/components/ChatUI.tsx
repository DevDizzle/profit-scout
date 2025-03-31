import { useState, useEffect } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp } from "lucide-react";

interface Message {
  id: string;
  text: string;
  type: "user" | "bot";
  timestamp: string;
  expandable?: boolean;
  expanded?: boolean;
  summary?: string;
}

const funFacts = [
  "Candy Crush brings in more than $633,000 in revenue every day!",
  "Google was initially called BackRub before it was renamed.",
  "Pepsi got its name from pepsin, the digestive enzyme.",
  "Apple's retina scan technology is manufactured by Samsung.",
  "Pouring a perfect pint of Guinness takes exactly 119.5 seconds.",
  "Nike was named after the Greek goddess of victory.",
  "LEGO comes from the Danish word 'Leg Godt', meaning 'play well'.",
  "Nokia started as a wood mill in Finland before becoming a mobile giant.",
  "Satya Nadella worked 23 years at Microsoft before becoming its CEO."
];

function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [funFact, setFunFact] = useState("");

  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  const fetchSynthesis = async (ticker: string) => {
    setIsProcessing(true);
    setProgress(0);
    setFunFact(funFacts[Math.floor(Math.random() * funFacts.length)]);

    setMessages(prev => [
      ...prev,
      {
        id: generateUniqueId(),
        text: `⏳ ProfitScout is analyzing ${ticker}. Please wait (~30 secs). Fun Fact: ${funFact}`,
        type: "bot",
        timestamp: new Date().toLocaleString()
      }
    ]);

    const progressInterval = setInterval(() => {
      setProgress(prev => (prev >= 95 ? 95 : prev + 5));
    }, 1500);

    try {
      const [quantitativeRes, qualitativeRes] = await Promise.all([
        fetch(`${backendUrl}/quantative/analyze_stock/${ticker}`),
        fetch(`${backendUrl}/qualitative/analyze_sec/${ticker}`)
      ]);

      if (!quantitativeRes.ok || !qualitativeRes.ok) throw new Error("Analysis failed");

      const quantitativeData = await quantitativeRes.json();
      const qualitativeData = await qualitativeRes.json();

      const synthesisRes = await fetch(`${backendUrl}/synthesizer/synthesize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker,
          yahoo_analysis: quantitativeData.quantitative_analysis,
          sec_analysis: qualitativeData.qualitative_analysis
        })
      });

      if (!synthesisRes.ok) throw new Error("Synthesis failed");

      const synthesisData = await synthesisRes.json();

      setMessages(prev => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `📊 **Comprehensive Analysis for ${ticker}:**\n\n${synthesisData.synthesis}`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          expandable: true,
          expanded: false,
          summary: synthesisData.synthesis.slice(0, 250) + "... [Expand for full analysis]"
        }
      ]);
    } catch (error: any) {
      setMessages(prev => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `❌ Error: ${error.message}. Please try again.`,
          type: "bot",
          timestamp: new Date().toLocaleString()
        }
      ]);
    }

    clearInterval(progressInterval);
    setProgress(100);
    setIsProcessing(false);
  };

  const handleSend = () => {
    if (!query.trim()) return;
    setMessages(prev => [
      ...prev,
      { id: generateUniqueId(), text: query, type: "user", timestamp: new Date().toLocaleString() }
    ]);
    fetchSynthesis(query);
    setQuery("");
  };

  const toggleExpand = (id: string) => {
    setMessages(prev =>
      prev.map(msg =>
        msg.id === id && msg.expandable ? { ...msg, expanded: !msg.expanded } : msg
      )
    );
  };

  return (
    <div className="flex flex-col items-center justify-center w-full">
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-gray-900 text-white p-6 space-y-4">
        <div className="h-[70vh] overflow-y-auto border border-gray-700 rounded-lg p-4 bg-gray-800">
          {messages.map(msg => (
            <div key={msg.id} className={`mb-3 ${msg.type === "user" ? "text-right" : "text-left"}`}>
              <span className="block text-xs text-gray-400 mb-1">{msg.timestamp}</span>
              <div className={`inline-block p-3 rounded-lg ${msg.type === "user" ? "bg-blue-600" : "bg-gray-700"}`}>
                <pre className="whitespace-pre-wrap text-sm">
                  {msg.expandable && !msg.expanded ? msg.summary : msg.text}
                </pre>
                {msg.expandable && (
                  <button onClick={() => toggleExpand(msg.id)} className="text-xs text-blue-400 mt-1 flex items-center">
                    {msg.expanded ? (<>Collapse <ChevronUp size={16}/></>) : (<>Expand <ChevronDown size={16}/></>)}
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
        {isProcessing && (
          <div className="w-full bg-gray-700 rounded-full h-2">
            <div className="bg-indigo-500 h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
          </div>
        )}
        <div className="flex space-x-2">
          <Input value={query} onChange={e => setQuery(e.target.value)} placeholder="Enter stock ticker (e.g., AMZN)" disabled={isProcessing} />
          <Button onClick={handleSend} disabled={isProcessing}>{isProcessing ? "Analyzing..." : "Send"}</Button>
        </div>
      </div>
    </div>
  );
}
