import { useState, useEffect, useRef } from "react"; // Added useEffect, useRef
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
// Removed ChevronDown, ChevronUp
import ReactMarkdown from 'react-markdown'; // Added for Markdown rendering
import remarkGfm from 'remark-gfm'; // Added for GitHub Flavored Markdown support (tables, etc.)

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
  text: string; // Will hold synthesis text OR initial messages
  type: "user" | "bot";
  timestamp: string;
  // Removed expandable, expanded, summary
  isLoading?: boolean; // Optional: flag for showing loading specific to this message
  taskId?: string; // Optional: To know which message triggered the async task
  quantitativeData?: QuantitativeData | null; // Optional: Holds final quant metrics
  isError?: boolean; // Optional: Flag for error messages
}

// Removed funFacts array

function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

// --- Helper Component for Quant Table ---
function QuantTable({ data }: { data: QuantitativeData | null | undefined }) {
  if (!data || typeof data !== 'object' || Object.keys(data).length === 0 || data.error) {
    // Optionally display error if present: return <div className="mt-2 text-red-400 text-xs">Quant Data Error: {data?.error}</div>;
    return null; // Don't render if no data, not an object, empty, or error
  }

  // Define which keys to display and their labels
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
    recommendation: "Quant Recommendation" // Display quant recommendation
    // Add/remove keys as desired
  };

  // Function to format values (numbers to fixed decimal places)
  const formatValue = (value: any) => {
    if (typeof value === 'number') {
      // Format percentages vs ratios differently if needed
      if (String(value).includes('.') && Math.abs(value) < 5) { // Basic heuristic for ratios/yields
        return value.toFixed(4);
      }
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 }); // Format large numbers
    }
    return String(value ?? 'N/A'); // Handle null/undefined
  };

  return (
    <div className="mt-3 pt-3 border-t border-gray-600 text-xs">
      <h4 className="font-semibold mb-1 text-[#FFD700]">Quantitative Metrics:</h4>
      <table className="w-full text-left border-collapse">
        <tbody>
          {Object.entries(displayMap).map(([key, label]) =>
             data.hasOwnProperty(key) && data[key as keyof QuantitativeData] != null && ( // Check if key exists and value is not null
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
  const [isProcessing, setIsProcessing] = useState(false); // Tracks if *any* request is in flight
  const eventSourceRef = useRef<EventSource | null>(null); // Ref to manage EventSource instance

  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  // Cleanup effect for EventSource
  useEffect(() => {
    // Return cleanup function
    return () => {
      if (eventSourceRef.current) {
        console.log("Closing existing EventSource connection.");
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, []); // Empty dependency array ensures this runs only on mount and unmount


  // Function to handle the initial chat request and start SSE listening
  const handleInitialRequest = async (input: string, userMessageId: string) => {
    setIsProcessing(true); // Indicate processing starts

     // Add a temporary bot message indicating processing has started for this request
     const thinkingMessageId = generateUniqueId();
     setMessages(prev => [
       ...prev,
       {
         id: thinkingMessageId,
         text: "FinBot is processing your request...", // Simple message
         type: "bot",
         timestamp: new Date().toLocaleString(),
         isLoading: true, // Mark this specific message as loading
       }
     ]);


    try {
      const response = await fetch(`${backendUrl}/greeter/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: input })
      });

      if (!response.ok) {
         const errorData = await response.json().catch(() => ({ detail: "Request failed with status " + response.status }));
         throw new Error(errorData.detail || "Request failed");
      }

      const data = await response.json();

      // --- Handle Initial Response ---
       // Remove the "thinking..." message
       setMessages(prev => prev.filter(msg => msg.id !== thinkingMessageId));

      // Add the actual initial response from the backend
      const initialBotMessageId = generateUniqueId();
      setMessages(prev => [
        ...prev,
        {
          id: initialBotMessageId,
          text: data.message, // e.g., "Okay, I recognized..." or guidance
          type: "bot",
          timestamp: new Date().toLocaleString(),
          isError: data.status === "unrecognized" || data.status === "needs_clarification", // Flag guidance/errors
        }
      ]);


      // --- If Processing Started, Connect to SSE ---
      if (data.status === "processing_started" && data.task_id) {
        console.log(`Processing started with Task ID: ${data.task_id}. Connecting to SSE...`);

         // Add a new message indicating analysis is running in background
         const finalResultMessageId = generateUniqueId(); // ID for the final result message
         setMessages(prev => [
             ...prev,
             {
                 id: finalResultMessageId, // This message will be updated by SSE
                 text: "⏳ Analysis in progress...",
                 type: "bot",
                 timestamp: new Date().toLocaleString(),
                 isLoading: true, // Mark this specific placeholder as loading
                 taskId: data.task_id, // Store task ID if needed
             }
         ]);


        // Close any previous connection
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
        }

        // Create new EventSource
        const newEventSource = new EventSource(`${backendUrl}/greeter/stream/${data.task_id}`);
        eventSourceRef.current = newEventSource;

        newEventSource.onmessage = (event) => {
          console.log("SSE Message Received:", event.data);
          try {
            const result = JSON.parse(event.data);

             // Update the specific placeholder message with the final result
            setMessages(prev => prev.map(msg => {
                if (msg.id === finalResultMessageId) {
                    if (result.status === 'completed' && result.data) {
                        return {
                            ...msg,
                            text: result.data.synthesis || "Synthesis complete.", // Display synthesis text
                            quantitativeData: result.data.quantitativeData || null, // Attach quant data
                            isLoading: false, // Stop loading indicator for this message
                            isError: false,
                        };
                    } else { // Handle 'error' status from backend or timeout
                        return {
                            ...msg,
                            text: `❌ Analysis failed: ${result.data?.message || 'Unknown error'}`,
                            isLoading: false,
                            isError: true,
                        };
                    }
                }
                return msg; // Keep other messages unchanged
            }));


          } catch (e) {
            console.error("Failed to parse SSE message data:", e);
             // Update placeholder with parse error message
             setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Error receiving analysis result.", isLoading: false, isError: true } : msg));
          } finally {
              setIsProcessing(false); // Indicate overall processing finished
              newEventSource.close(); // Close connection after receiving the final message
              eventSourceRef.current = null;
              console.log("SSE Connection Closed.");
          }
        };

        newEventSource.onerror = (error) => {
          console.error("SSE Error:", error);
           // Update placeholder with connection error message
           setMessages(prev => prev.map(msg => msg.id === finalResultMessageId ? { ...msg, text: "❌ Connection error during analysis.", isLoading: false, isError: true } : msg));
          setIsProcessing(false); // Indicate overall processing finished
          newEventSource.close();
          eventSourceRef.current = null;
          console.log("SSE Connection Closed due to error.");
        };

      } else {
         // If status wasn't "processing_started", no background task, so processing is done
         setIsProcessing(false);
      }

    } catch (error: any) {
      console.error("Fetch Greeter Response Error:", error);
       // Remove the "thinking..." message on error
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
    if (!query.trim() || isProcessing) return; // Prevent sending while processing

    const userMessageId = generateUniqueId();
    const messageToSend = query; // Capture current query

    // Add the user's message.
    setMessages(prev => [
      ...prev,
      { id: userMessageId, text: messageToSend, type: "user", timestamp: new Date().toLocaleString() }
    ]);

    // Call handler to fetch response and potentially start SSE
    handleInitialRequest(messageToSend, userMessageId);

    setQuery(""); // Clear input after sending
  };

  // Removed toggleExpand function

  return (
    <div className="flex flex-col items-center justify-center w-full">
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-[#252525] text-white p-6 space-y-4">
        {/* Message Display Area */}
        <div className="h-[70vh] overflow-y-auto rounded-lg p-4 bg-[#333333] space-y-4"> {/* Added space-y-4 */}
          {messages.map(msg => (
            <div key={msg.id} className={`flex ${msg.type === "user" ? "justify-end" : "justify-start"}`}> {/* Use flex for alignment */}
               <div className={`max-w-[80%] ${msg.type === "user" ? "text-right" : "text-left"}`}> {/* Constrain width */}
                <span className={`block text-xs mb-1 ${msg.type === 'user' ? 'text-[#AAAAAA]' : 'text-[#CCCCCC]'}`}>{msg.timestamp}</span>
                <div className={`inline-block p-3 rounded-lg ${
                    msg.isError ? "bg-red-800 text-red-100" : // Error styling
                    msg.type === "user" ? "bg-[#00A3E0] text-black" : // User styling
                    "bg-[#1A1A1A] text-white" // Bot styling
                 }`}>
                   {/* Conditionally render loading indicator or content */}
                   {msg.isLoading ? (
                        <div className="flex items-center space-x-2 text-sm">
                            <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.3s]"></div>
                            <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.15s]"></div>
                            <div className="w-2 h-2 bg-current rounded-full animate-bounce"></div>
                            <span>{msg.text}</span> {/* Show "Analysis in progress..." */}
                        </div>
                   ) : (
                       <>
                           {/* Render Markdown for bot text, pre for user */}
                           {msg.type === 'bot' ? (
                               <ReactMarkdown
                                   className="prose prose-sm prose-invert max-w-none" // Basic styling
                                   remarkPlugins={[remarkGfm]} // Enable GitHub Flavored Markdown (tables!)
                               >
                                   {msg.text}
                               </ReactMarkdown>
                           ) : (
                               <pre className="whitespace-pre-wrap text-sm">{msg.text}</pre>
                           )}

                           {/* Render Quant Table if data exists */}
                           {msg.quantitativeData && <QuantTable data={msg.quantitativeData} />}
                       </>
                   )}

                  {/* REMOVED Expand/Collapse Button Logic */}
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Removed separate progress bar, loading integrated into messages/button state */}

        {/* Input Area */}
        <div className="flex space-x-2 pt-4 border-t border-gray-600"> {/* Added top border */}
          <Input
            className="bg-[#1A1A1A] text-white placeholder-[#999] border-[#444] focus:border-[#00A3E0] focus:ring-[#00A3E0]" // Enhanced styling
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="Enter a stock ticker or company name (e.g., MSFT)"
            disabled={isProcessing} // Disable input while any processing is happening
             onKeyDown={(e) => { if (e.key === 'Enter' && !isProcessing) handleSend(); }} // Allow Enter key
          />
          <Button onClick={handleSend} disabled={isProcessing}>
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
