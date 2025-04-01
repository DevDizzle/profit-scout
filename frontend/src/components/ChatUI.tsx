import { useState } from "react";
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

  // New function to call greeter's /chat endpoint.
  const fetchGreeterResponse = async (input: string) => {
    setIsProcessing(true);
    setProgress(0);
    const randomFunFact = funFacts[Math.floor(Math.random() * funFacts.length)];
    setFunFact(randomFunFact);

    // Add a temporary message showing that FinBot is processing.
    setMessages(prev => [
      ...prev,
      {
        id: generateUniqueId(),
        text: `⏳ FinBot is processing your message... Fun Fact: ${randomFunFact}`,
        type: "bot",
        timestamp: new Date().toLocaleString()
      }
    ]);

    try {
      const response = await fetch(`${backendUrl}/greeter/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: input })
      });

      if (!response.ok) throw new Error("Request failed");

      const data = await response.json();

      setMessages(prev => [
        ...prev,
        {
          id: generateUniqueId(),
          text: data.message,
          type: "bot",
          timestamp: new Date().toLocaleString()
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

    setIsProcessing(false);
    setProgress(100);
  };

  const handleSend = () => {
    if (!query.trim()) return;

    // Add the user's message.
    setMessages(prev => [
      ...prev,
      { id: generateUniqueId(), text: query, type: "user", timestamp: new Date().toLocaleString() }
    ]);

    // Pass the input to the greeter for a natural response.
    fetchGreeterResponse(query);
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
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-[#252525] text-white p-6 space-y-4">
        <div className="h-[70vh] overflow-y-auto rounded-lg p-4 bg-[#333333]">
          {messages.map(msg => (
            <div key={msg.id} className={`mb-3 ${msg.type === "user" ? "text-right" : "text-left"}`}>
              <span className="block text-xs text-[#CCCCCC] mb-1">{msg.timestamp}</span>
              <div className={`inline-block p-3 rounded-lg ${msg.type === "user" ? "bg-[#00A3E0] text-black" : "bg-[#1A1A1A] text-white"}`}>
                <pre className="whitespace-pre-wrap text-sm">
                  {msg.expandable && !msg.expanded ? msg.summary : msg.text}
                </pre>
                {msg.expandable && (
                  <button onClick={() => toggleExpand(msg.id)} className="text-xs text-[#FFD700] mt-1 flex items-center">
                    {msg.expanded ? (<>Collapse <ChevronUp size={16}/></>) : (<>Expand <ChevronDown size={16}/></>)}
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>

        {isProcessing && (
          <div className="w-full bg-[#333333] rounded-full h-2">
            <div className="bg-[#00A3E0] h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
          </div>
        )}

        <div className="flex space-x-2">
          <Input
            className="bg-[#1A1A1A] text-white placeholder-[#999]"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Enter a stock ticker or company name"
            disabled={isProcessing}
          />
          <Button onClick={handleSend} disabled={isProcessing}>
            {isProcessing ? "Processing..." : "Send"}
          </Button>
        </div>
      </div>
    </div>
  );
}
