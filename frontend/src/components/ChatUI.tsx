import { useState, useEffect, useRef } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import SimpleErrorBoundary from './SimpleErrorBoundary'; // <-- Import the Error Boundary

// Define interface for quantitative data (adjust keys as needed)
interface QuantitativeData {
  revenue_growth?: number | string | null;
  latest_revenue?: number | string | null;
  previous_revenue?: number | string | null;
  debt_to_equity?: number | string | null;
  total_debt?: number | string | null;
  total_equity?: number | string | null;
  fcf_yield?: number | string | null;
  operating_cash_flow?: number | string | null;
  capital_expenditure?: number | string | null;
  free_cash_flow?: number | string | null;
  market_cap?: number | string | null;
  price_trend_ratio?: number | string | null;
  ma_50?: number | string | null;
  ma_200?: number | string | null;
  recommendation?: string | null; // Quant recommendation
  error?: string | null; // Potential error from quant analysis
  raw_response?: string | null;
}

interface Message {
  id: string;
  text: string;
  type: "user" | "bot";
  timestamp: string;
  isLoading?: boolean;
  taskId?: string;
  quantitativeData?: QuantitativeData | null;
  isError?: boolean;
}

function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

// --- Helper Component for Quant Table ---
function QuantTable({ data }: { data: QuantitativeData | null | undefined }) {
  if (!data || typeof data !== 'object' || Object.keys(data).length === 0 || data.error) {
    return null;
  }

  const displayMap: Record<keyof QuantitativeData, string> = {
    revenue_growth: "Revenue Growth",
    latest_revenue: "Latest Revenue",
    previous_revenue: "Previous Revenue",
    debt_to_equity: "Debt/Equity",
    total_debt: "Total Debt",
    total_equity: "Total Equity",
    fcf_yield: "FCF Yield",
    free_cash_flow: "Free Cash Flow",
    market_cap: "Market Cap",
    price_trend_ratio: "Price Trend (50/200)",
    ma_50: "50-Day MA",
    ma_200: "200-Day MA",
    recommendation: "Quant Recommendation"
  };

  const formatValue = (value: any) => {
    if (typeof value === 'number') {
      if (String(value).includes('.') && Math.abs(value) < 5) {
        return value.toFixed(4);
      }
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return String(value ?? 'N/A');
  };

  return (
    <div className="mt-3 pt-3 border-t border-gray-600 text-xs">
      <h4 className="font-semibold mb-1 text-[#FFD700]">Quantitative Metrics:</h4>
      <table className="w-full text-left border-collapse">
        <tbody>
          {Object.entries(displayMap).map(([key, label]) =>
              data.hasOwnProperty(key) && data[key as keyof QuantitativeData] != null && (
               <tr key={key} className="border-b border-gray-700 last:border-b-0">
                 <td className="py-1 pr-2 font-medium text-[#CCCCCC]">{label}</td>
                 <td className="py-1 pl-2 text-white">{formatValue(data[key as keyof QuantitativeData])}</td>
               </tr>
             )
          )}
        </tbody>
      </table>
    </div>
  );
}


// --- Main Chat Component ---
export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null); // Ref for scrolling

  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  // Scroll to bottom effect
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]); // Trigger scroll whenever messages update

  // Cleanup effect for EventSource
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        console.log("Closing existing EventSource connection on unmount.");
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, []);


  // Function to handle the initial chat request and start SSE listening
  const handleInitialRequest = async (input: string, userMessageId: string) => {
    setIsProcessing(true);

    const thinkingMessageId = generateUniqueId();
    setMessages(prev => [
      ...prev,
      {
        id: thinkingMessageId,
        text: "FinBot is processing your request...",
        type: "bot",
        timestamp: new Date().toLocaleString(),
        isLoading: true,
      }
    ]);

    try {
      const response = await fetch(`${backendUrl}/greeter/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: input })
      });

      // Always remove thinking message regardless of ok status
      setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId));

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: "Request failed with status " + response.status }));
        throw new Error(errorData.detail || "Request failed");
      }

      const data = await response.json();

      // Add the actual initial response from the backend
      const initialBotMessageId = generateUniqueId();
      setMessages(prev => [
        ...prev,
        {
          id: initialBotMessageId,
          text: data.message,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          isError: data.status === "unrecognized" || data.status === "needs_clarification",
        }
      ]);

      if (data.status === "processing_started" && data.task_id) {
        console.log(`Processing started with Task ID: ${data.task_id}. Connecting to SSE...`);

        const finalResultMessageId = generateUniqueId();
        setMessages(prev => [
          ...prev,
          {
            id: finalResultMessageId,
            text: "⏳ Analysis in progress...",
            type: "bot",
            timestamp: new Date().toLocaleString(),
            isLoading: true,
            taskId: data.task_id,
          }
        ]);

        if (eventSourceRef.current) {
          console.log("Closing potentially lingering EventSource before opening new one.");
          eventSourceRef.current.close();
        }

        const newEventSource = new EventSource(`${backendUrl}/greeter/stream/${data.task_id}`);
        eventSourceRef.current = newEventSource;

        newEventSource.onmessage = (event) => {
          console.log("SSE Message Received:", event.data);
          let result; // Declare outside try block
          try {
             result = JSON.parse(event.data); // Parse here

            setMessages(prev => prev.map(msg => {
              if (msg.id === finalResultMessageId) {
                if (result.status === 'completed' && result.data) {
                  return {
                    ...msg,
                    text: result.data.synthesis || "Synthesis complete.",
                    quantitativeData: result.data.quantitativeData || null,
                    isLoading: false,
                    isError: false,
                  };
                } else { // Handle 'error' status from backend or other non-complete but final statuses
                  return {
                    ...msg,
                    text: `❌ Analysis ${result.status || 'failed'}: ${result.data?.message || 'Unknown error or timeout'}`,
                    isLoading: false,
                    isError: true,
                  };
                }
              }
              return msg;
            }));

          } catch (e) {
            console.error("Failed to parse SSE message data:", e, "Raw data:", event.data);
            setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Error processing analysis result from server.", isLoading: false, isError: true } : msg));
          } finally {
             // Close SSE only if the received message indicates completion or error
             if (result && (result.status === 'completed' || result.status === 'error' /* Add any other final statuses */)) {
                console.log(`SSE Connection Closed gracefully after status: ${result.status}.`);
                newEventSource.close();
                eventSourceRef.current = null;
                setIsProcessing(false); // Indicate overall processing finished
             } else if (!result) { // Handle the catch block case where result is undefined
                 console.log("SSE Connection Closed due to parsing error.");
                 newEventSource.close();
                 eventSourceRef.current = null;
                 setIsProcessing(false);
             }
             // Otherwise, keep listening for more messages if it's an intermediate status
          }
        };

        newEventSource.onerror = (error) => {
          console.error("SSE Error:", error);
          setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Connection error during analysis.", isLoading: false, isError: true } : msg));
          setIsProcessing(false);
          if (eventSourceRef.current) { // Check if it exists before closing
             eventSourceRef.current.close();
             eventSourceRef.current = null;
             console.log("SSE Connection Closed due to error.");
          }
        };

      } else {
        // If status wasn't "processing_started", no background task, so processing is done
        setIsProcessing(false);
      }

    } catch (error: any) {
      console.error("Fetch Greeter Response Error:", error);
      // Ensure thinking message is removed if it wasn't already
      setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId));
      // Add error message
      setMessages(prev => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `❌ Error: ${error.message}. Please check connection or backend.`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          isError: true,
        }
      ]);
      setIsProcessing(false); // Ensure processing stops on error
    }
  };

  const handleSend = () => {
    if (!query.trim() || isProcessing) return;

    const userMessageId = generateUniqueId();
    const messageToSend = query;

    setMessages(prev => [
      ...prev,
      { id: userMessageId, text: messageToSend, type: "user", timestamp: new Date().toLocaleString() }
    ]);

    handleInitialRequest(messageToSend, userMessageId);

    setQuery("");
  };


  return (
    <div className="flex flex-col items-center justify-center w-full">
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-[#252525] text-white p-6 flex flex-col"> {/* Changed to flex-col */}
        {/* Message Display Area */}
        <div className="flex-grow h-[70vh] overflow-y-auto rounded-lg p-4 bg-[#333333] space-y-4 mb-4"> {/* Added flex-grow and mb-4 */}
          {messages.map(msg => (
            // Wrap each message rendering block with the Error Boundary
            <SimpleErrorBoundary key={msg.id}>
              <div className={`flex ${msg.type === "user" ? "justify-end" : "justify-start"}`}>
                 <div className={`max-w-[80%] ${msg.type === "user" ? "text-right" : "text-left"}`}>
                   <span className={`block text-xs mb-1 ${msg.type === 'user' ? 'text-[#AAAAAA]' : 'text-[#CCCCCC]'}`}>{msg.timestamp}</span>
                   <div className={`inline-block p-3 rounded-lg ${
                       msg.isError ? "bg-red-800 text-red-100" :
                       msg.type === "user" ? "bg-[#00A3E0] text-black" :
                       "bg-[#1A1A1A] text-white"
                     }`}>
                     {msg.isLoading ? (
                       <div className="flex items-center space-x-2 text-sm">
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.3s]"></div>
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.15s]"></div>
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce"></div>
                         <span>{msg.text}</span>
                       </div>
                     ) : (
                       <>
                         {/* Render Markdown for bot text, pre for user */}
                         {msg.type === 'bot' ? (
                           <ReactMarkdown
                             className="prose prose-sm prose-invert max-w-none" // Tailwind Typography classes applied here
                             remarkPlugins={[remarkGfm]} // Markdown parsing plugins
                           >
                             {msg.text}
                           </ReactMarkdown>
                         ) : (
                           <pre className="whitespace-pre-wrap text-sm font-sans">{msg.text}</pre> // Added font-sans for consistency
                         )}

                         {/* Render Quant Table if data exists */}
                         {msg.quantitativeData && <QuantTable data={msg.quantitativeData} />}
                       </>
                     )}
                   </div>
                 </div>
               </div>
            </SimpleErrorBoundary>
          ))}
          {/* Invisible element to trigger scrolling */}
          <div ref={messagesEndRef} />
        </div>

        {/* Input Area */}
        <div className="flex space-x-2 pt-4 border-t border-gray-600">
          <Input
            className="bg-[#1A1A1A] text-white placeholder-[#999] border-[#444] focus:border-[#00A3E0] focus:ring-[#00A3E0]"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Enter a stock ticker or company name (e.g., MSFT)"
            disabled={isProcessing}
            onKeyDown={(e) => { if (e.key === 'Enter' && !isProcessing) handleSend(); }}
          />
          <Button onClick={handleSend} disabled={isProcessing || !query.trim()} > {/* Disable button if query is empty */}
            {isProcessing ? (
                <>
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Processing...
                </>
            ) : "Send"}
          </Button>
        </div>
      </div>
    </div>
  );
}
