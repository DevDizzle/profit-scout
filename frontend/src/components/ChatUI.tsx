import { useState, useEffect, useRef } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import SimpleErrorBoundary from './SimpleErrorBoundary'; // Assuming this is in the same directory or configured path

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
  recommendation?: string | null; // Quant recommendation (kept in interface, removed from display)
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
  isError?: boolean; // For actual processing/connection errors
  isGuidance?: boolean; // For informational messages like unrecognized input
}

function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

// --- Helper Component for Quant Table ---
function QuantTable({ data }: { data: QuantitativeData | null | undefined }) {
  if (!data || typeof data !== 'object' || Object.keys(data).length === 0 || data.error) {
    return null;
  }

  // **MODIFICATION:** Removed 'recommendation' from the display map
  const displayMap: Record<keyof Omit<QuantitativeData, 'recommendation' | 'error' | 'raw_response' | 'operating_cash_flow' | 'capital_expenditure'>, string> = {
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
    // recommendation: "Quant Recommendation" // <-- REMOVED THIS LINE
  };

  const formatValue = (value: any) => {
    if (typeof value === 'number') {
      // Heuristic check for potential percentage or ratio values vs large numbers
      if (String(value).includes('.') && Math.abs(value) < 100 && Math.abs(value) > 0.0001) {
         // Check if it might be a percentage based on context (you might refine this)
         // For now, format ratios/yields to more decimal places
         if (['revenue_growth', 'fcf_yield', 'debt_to_equity', 'price_trend_ratio'].some(key => Object.keys(data).includes(key))) {
             return value.toFixed(4);
         }
      }
      // Format potentially large numbers with commas
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    // Handle null/undefined or non-numeric strings
    return String(value ?? 'N/A');
  };


  return (
    <div className="mt-3 pt-3 border-t border-gray-600 text-xs">
      <h4 className="font-semibold mb-1 text-[#FFD700]">Quantitative Metrics:</h4>
      <table className="w-full text-left border-collapse">
        <tbody>
          {/* Use stricter typing for keys to ensure we only map defined display values */}
          {(Object.keys(displayMap) as Array<keyof typeof displayMap>).map((key) =>
              data.hasOwnProperty(key) && data[key] != null && (
               <tr key={key} className="border-b border-gray-700 last:border-b-0">
                 <td className="py-1 pr-2 font-medium text-[#CCCCCC]">{displayMap[key]}</td>
                 <td className="py-1 pl-2 text-white">{formatValue(data[key])}</td>
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
    // Return cleanup function
    return () => {
      if (eventSourceRef.current) {
        console.log("Closing existing EventSource connection on unmount.");
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, []); // Empty dependency array ensures this runs only on mount and unmount


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
        // This is a real error
        setMessages(prev => [
            ...prev,
            {
                id: generateUniqueId(),
                text: `❌ Error: ${errorData.detail || "Request failed"}. Please check connection or backend.`,
                type: "bot",
                timestamp: new Date().toLocaleString(),
                isError: true, // Set true error flag
            }
        ]);
        setIsProcessing(false);
        return; // Stop further processing on fetch error
      }

      const data = await response.json();

      // Add the actual initial response from the backend
      const initialBotMessageId = generateUniqueId();
      const isGuidanceMessage = data.status === "unrecognized" || data.status === "needs_clarification";
      setMessages(prev => [
        ...prev,
        {
          id: initialBotMessageId,
          text: data.message,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          // **MODIFICATION:** Set isGuidance instead of isError for these statuses
          isGuidance: isGuidanceMessage,
          isError: false, // Ensure isError is false for guidance messages
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
          let result;
          try {
             result = JSON.parse(event.data);

            setMessages(prev => prev.map(msg => {
              if (msg.id === finalResultMessageId) {
                // Handle final 'completed' status from SSE
                if (result.status === 'completed' && result.data) {
                  return {
                    ...msg,
                    text: result.data.synthesis || "Synthesis complete.",
                    quantitativeData: result.data.quantitativeData || null,
                    isLoading: false,
                    isError: false, // Successful completion is not an error
                    isGuidance: false,
                  };
                // Handle final 'error' status from SSE (or other failed statuses)
                } else if (result.status === 'error' /* Add other potential final non-complete statuses here */) {
                  return {
                    ...msg,
                    text: `❌ Analysis failed: ${result.data?.message || 'Unknown error during analysis'}`,
                    isLoading: false,
                    isError: true, // This is a real error from the backend task
                    isGuidance: false,
                  };
                }
                // Optional: Handle intermediate statuses if your backend sends them
                // else { return {...msg, text: result.data?.message || msg.text, isLoading: true }; }

                // Fallback for unexpected final statuses (treat as error)
                 return {
                    ...msg,
                    text: `❌ Analysis ended with unexpected status '${result.status}': ${result.data?.message || 'No details'}`,
                    isLoading: false,
                    isError: true,
                    isGuidance: false,
                  };
              }
              return msg;
            }));

          } catch (e) {
            console.error("Failed to parse SSE message data:", e, "Raw data:", event.data);
            // Treat parsing failure as a real error
            setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Error processing analysis result from server.", isLoading: false, isError: true, isGuidance: false } : msg));
            result = { status: 'parse_error' }; // Ensure finally block knows to close
          } finally {
             // Close SSE only if the received message indicates a *final* state (completed, error, parse_error, etc.)
             if (result && (result.status === 'completed' || result.status === 'error' || result.status === 'parse_error' /* Add any other final statuses */)) {
                console.log(`SSE Connection Closed gracefully after status: ${result.status}.`);
                newEventSource.close();
                eventSourceRef.current = null;
                setIsProcessing(false); // Indicate overall processing finished
             }
             // Otherwise, keep listening for more messages if it's an intermediate status
          }
        };

        newEventSource.onerror = (error) => {
          console.error("SSE Error:", error);
           // Treat SSE connection error as a real error
          setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Connection error during analysis.", isLoading: false, isError: true, isGuidance: false } : msg));
          setIsProcessing(false);
          if (eventSourceRef.current) {
             eventSourceRef.current.close();
             eventSourceRef.current = null;
             console.log("SSE Connection Closed due to error.");
          }
        };

      } else {
        // If initial response status wasn't "processing_started", no background task, so processing is done for now
        setIsProcessing(false);
      }

    } catch (error: any) {
        // This catch block handles errors during the *initial* fetch POST request itself
      console.error("Fetch Greeter Response Error:", error);
      // Ensure thinking message is removed if it wasn't already
      setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId));
      // Add a real error message
      setMessages(prev => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `❌ Error: ${error.message}. Please check connection or backend.`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          isError: true, // This is a real error
          isGuidance: false,
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
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-[#252525] text-white p-6 flex flex-col">
        {/* Message Display Area */}
        <div className="flex-grow h-[70vh] overflow-y-auto rounded-lg p-4 bg-[#333333] space-y-4 mb-4">
          {messages.map(msg => (
            <SimpleErrorBoundary key={msg.id}>
              <div className={`flex ${msg.type === "user" ? "justify-end" : "justify-start"}`}>
                 <div className={`max-w-[80%] ${msg.type === "user" ? "text-right" : "text-left"}`}>
                   <span className={`block text-xs mb-1 ${msg.type === 'user' ? 'text-[#AAAAAA]' : 'text-[#CCCCCC]'}`}>{msg.timestamp}</span>
                   {/* **MODIFICATION:** Updated className logic for different message types */}
                   <div className={`inline-block p-3 rounded-lg ${
                       msg.isError ? "bg-red-800 text-red-100" :              // Real errors = Red
                       msg.isGuidance ? "bg-yellow-700 text-yellow-100" :     // Guidance/Info = Yellow
                       msg.type === "user" ? "bg-[#00A3E0] text-black" :      // User message = Blue
                       "bg-[#1A1A1A] text-white"                             // Standard bot message = Dark grey
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
                            <div className="prose prose-sm prose-invert max-w-none">
                                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                    {msg.text}
                                </ReactMarkdown>
                            </div>
                         ) : (
                           <pre className="whitespace-pre-wrap text-sm font-sans">{msg.text}</pre>
                         )}

                         {/* Render Quant Table if data exists (Unchanged) */}
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
          <Button onClick={handleSend} disabled={isProcessing || !query.trim()} >
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
