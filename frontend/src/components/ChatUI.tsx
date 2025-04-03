import { useState, useEffect, useRef } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import SimpleErrorBoundary from './SimpleErrorBoundary'; // Assuming this is in the same directory or configured path

// --- Interfaces ---
interface QuantitativeData {
  revenue_growth?: number | string | null;
  latest_revenue?: number | string | null;
  previous_revenue?: number | string | null;
  debt_to_equity?: number | string | null;
  total_debt?: number | string | null;
  total_equity?: number | string | null;
  fcf_yield?: number | string | null;
  operating_cash_flow?: number | string | null; // Included for potential future use/debugging
  capital_expenditure?: number | string | null; // Included for potential future use/debugging
  free_cash_flow?: number | string | null;
  market_cap?: number | string | null;
  price_trend_ratio?: number | string | null;
  ma_50?: number | string | null;
  ma_200?: number | string | null;
  reporting_period_ending?: string | null; // ** ADDED **
  recommendation?: string | null; // Kept in interface if data source still sends it
  error?: string | null;
  raw_response?: string | null; // Included for potential future use/debugging
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
  isGuidance?: boolean;
}

// --- Utility Functions ---
function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

// --- Helper Component for Quant Table ---
function QuantTable({ data }: { data: QuantitativeData | null | undefined }) {
  // Guard clause: Check if data is valid
  if (!data || typeof data !== 'object' || Object.keys(data).length === 0 || data.error) {
    // Optionally log or display the error if present:
    // if (data?.error) { console.warn("Quant data error:", data.error); }
    return null; // Don't render the table if data is invalid, empty, or has an error flag
  }

  // Define keys to display and their labels explicitly
  // Use Partial<> for the Record type as not all keys from QuantitativeData might be in displayMap
  type DisplayKeys = keyof Omit<QuantitativeData, 'recommendation' | 'error' | 'raw_response' | 'operating_cash_flow' | 'capital_expenditure'>;
  const displayMap: Record<DisplayKeys, string> = {
    // ** ADDED reporting_period_ending **
    reporting_period_ending: "Reporting Period End",
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
    // recommendation: "Quant Recommendation" // Explicitly removed from display
  };

  // Function to format values for display
  const formatValue = (value: any) => {
    // Handle numbers: Format large numbers, potentially format ratios/percentages differently
    if (typeof value === 'number') {
        // Simple check for values likely representing ratios/yields (adjust threshold as needed)
        if (String(value).includes('.') && Math.abs(value) < 100 && Math.abs(value) > 0.00001) {
             // You could add more specific checks based on the key if needed here
             // e.g., if (key === 'revenue_growth' || key === 'fcf_yield') return `${(value * 100).toFixed(2)}%`;
             return value.toFixed(4); // Format likely ratios/yields to 4 decimals
        }
        // Format potentially large numbers with commas, limiting decimals
        return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    // Handle null, undefined, or string representations like "N/A"
    // Convert null/undefined to the string "N/A"
    return String(value ?? 'N/A');
  };


  return (
    <div className="mt-3 pt-3 border-t border-gray-600 text-xs">
      <h4 className="font-semibold mb-1 text-[#FFD700]">Quantitative Metrics:</h4>
      <table className="w-full text-left border-collapse">
        <tbody>
          {/* Iterate through the displayMap keys to control order and inclusion */}
          {(Object.keys(displayMap) as Array<DisplayKeys>).map((key) => {
            // **MODIFICATION:** Render row if the key is defined in displayMap AND exists in the data object
            // Using hasOwnProperty is safer than just checking data[key]
            if (data.hasOwnProperty(key)) {
              return (
                <tr key={key} className="border-b border-gray-700 last:border-b-0">
                  <td className="py-1 pr-2 font-medium text-[#CCCCCC]">{displayMap[key]}</td>
                  {/* formatValue handles null/undefined display */}
                  <td className="py-1 pl-2 text-white">{formatValue(data[key])}</td>
                </tr>
              );
            }
            // If key from displayMap is not in data, don't render the row
            return null;
          })}
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
  // (No changes needed in this function based on the request)
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

      setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId)); // Remove thinking message

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: "Request failed with status " + response.status }));
        setMessages(prev => [ /* ... Add error message ... */
            ...prev,
            { id: generateUniqueId(), text: `❌ Error: ${errorData.detail || "Request failed"}.`, type: "bot", timestamp: new Date().toLocaleString(), isError: true }
        ]);
        setIsProcessing(false);
        return;
      }

      const data = await response.json();
      const initialBotMessageId = generateUniqueId();
      const isGuidanceMessage = data.status === "unrecognized" || data.status === "needs_clarification";
      setMessages(prev => [ /* ... Add initial bot response ... */
        ...prev,
        { id: initialBotMessageId, text: data.message, type: "bot", timestamp: new Date().toLocaleString(), isGuidance: isGuidanceMessage, isError: false }
      ]);

      if (data.status === "processing_started" && data.task_id) {
        console.log(`Processing started with Task ID: ${data.task_id}. Connecting to SSE...`);
        const finalResultMessageId = generateUniqueId();
        setMessages(prev => [ /* ... Add 'Analysis in progress...' message ... */
            ...prev,
            { id: finalResultMessageId, text: "⏳ Analysis in progress...", type: "bot", timestamp: new Date().toLocaleString(), isLoading: true, taskId: data.task_id }
        ]);

        if (eventSourceRef.current) { eventSourceRef.current.close(); } // Close previous SSE
        const newEventSource = new EventSource(`${backendUrl}/greeter/stream/${data.task_id}`);
        eventSourceRef.current = newEventSource;

        newEventSource.onmessage = (event) => { /* ... Handle SSE messages ... */
            console.log("SSE Message Received:", event.data);
            let result;
            try {
                result = JSON.parse(event.data);
                setMessages(prev => prev.map(msg => {
                    if (msg.id === finalResultMessageId) {
                        if (result.status === 'completed' && result.data) {
                            return { /* ... Update message with final data ... */
                                ...msg,
                                text: result.data.synthesis || "Synthesis complete.",
                                quantitativeData: result.data.quantitativeData || null, // Assign quant data here
                                isLoading: false, isError: false, isGuidance: false,
                            };
                        } else if (result.status === 'error') {
                             return { /* ... Update message with error ... */
                                ...msg,
                                text: `❌ Analysis failed: ${result.data?.message || 'Unknown analysis error'}`,
                                isLoading: false, isError: true, isGuidance: false,
                            };
                        }
                        // Fallback for unexpected statuses
                         return { ...msg, text: `❌ Analysis ended unexpectedly: ${result.status || 'Unknown'}`, isLoading: false, isError: true, isGuidance: false };
                    }
                    return msg;
                }));
            } catch (e) { /* ... Handle parse error ... */
                 console.error("SSE parse error:", e);
                 setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Error processing result.", isLoading: false, isError: true } : msg));
                 result = { status: 'parse_error' }; // Ensure finally closes
            } finally { /* ... Close SSE on final status ... */
                 if (result && (result.status === 'completed' || result.status === 'error' || result.status === 'parse_error')) {
                    console.log(`SSE Closing: ${result.status}`);
                    newEventSource.close();
                    eventSourceRef.current = null;
                    setIsProcessing(false);
                 }
            }
        };
        newEventSource.onerror = (error) => { /* ... Handle SSE connection error ... */
            console.error("SSE Error:", error);
            setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Connection error during analysis.", isLoading: false, isError: true } : msg));
            setIsProcessing(false);
            if (eventSourceRef.current) { eventSourceRef.current.close(); eventSourceRef.current = null; }
        };
      } else {
        setIsProcessing(false); // No SSE needed
      }
    } catch (error: any) { /* ... Handle initial fetch error ... */
        console.error("Fetch Greeter Error:", error);
        setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId)); // Clean up thinking msg
        setMessages(prev => [
            ...prev,
            { id: generateUniqueId(), text: `❌ Error: ${error.message}.`, type: "bot", timestamp: new Date().toLocaleString(), isError: true }
        ]);
        setIsProcessing(false);
    }
  };

  // (handleSend unchanged)
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


  // --- Render JSX ---
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
                   <div className={`inline-block p-3 rounded-lg ${
                       msg.isError ? "bg-red-800 text-red-100" :              // Real errors = Red
                       msg.isGuidance ? "bg-yellow-700 text-yellow-100" :     // Guidance/Info = Yellow
                       msg.type === "user" ? "bg-[#00A3E0] text-black" :      // User message = Blue
                       "bg-[#1A1A1A] text-white"                             // Standard bot message = Dark grey
                     }`}>
                     {msg.isLoading ? ( /* ... Loading indicator ... */
                       <div className="flex items-center space-x-2 text-sm">
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.3s]"></div>
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.15s]"></div>
                         <div className="w-2 h-2 bg-current rounded-full animate-bounce"></div>
                         <span>{msg.text}</span>
                       </div>
                     ) : (
                       <>
                         {/* Render Markdown/Text */}
                         {msg.type === 'bot' ? (
                            <div className="prose prose-sm prose-invert max-w-none">
                                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                    {msg.text}
                                </ReactMarkdown>
                            </div>
                         ) : (
                           <pre className="whitespace-pre-wrap text-sm font-sans">{msg.text}</pre>
                         )}
                         {/* Render Quant Table if data exists */}
                         {/* This will now include the reporting_period_ending row */}
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

        {/* Input Area (unchanged) */}
        <div className="flex space-x-2 pt-4 border-t border-gray-600">
          {/* ... Input and Button ... */}
          <Input
            className="bg-[#1A1A1A] text-white placeholder-[#999] border-[#444] focus:border-[#00A3E0] focus:ring-[#00A3E0]"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Enter a stock ticker or company name (e.g., MSFT)"
            disabled={isProcessing}
            onKeyDown={(e) => { if (e.key === 'Enter' && !isProcessing) handleSend(); }}
          />
          <Button onClick={handleSend} disabled={isProcessing || !query.trim()} >
            {isProcessing ? ( /* ... Processing SVG ... */
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
