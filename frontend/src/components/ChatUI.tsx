```tsx
import { useState, useEffect, useRef } from "react";
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import SimpleErrorBoundary from './SimpleErrorBoundary';
// Recharts for chart rendering
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend
} from 'recharts';
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

// Utility: generate a unique ID for messages/tasks
const generateUniqueId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;

// Main Chat Component
enum MessageType {
  USER = 'user',
  BOT = 'bot',
}

interface Message {
  id: string;
  text: string;
  type: MessageType;
  timestamp: string;
  isLoading?: boolean;
  isError?: boolean;
  isGuidance?: boolean;
  quantitativeData?: Record<string, any>;
  attachments?: Array<{
    type: string;
    data: any;
    options?: any;
  }>;
  taskId?: string;
}

export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  // Scroll to bottom when messages update
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Clean up SSE on unmount
  useEffect(() => () => {
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
  }, []);

  // Send initial chat request and handle SSE
  const handleInitialRequest = async (input: string, userMessageId: string) => {
    setIsProcessing(true);
    const thinkingId = generateUniqueId();
    setMessages(prev => [
      ...prev,
      { id: thinkingId, text: 'Processing...', type: MessageType.BOT, timestamp: new Date().toLocaleString(), isLoading: true }
    ]);

    try {
      const response = await fetch(`${backendUrl}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: input })
      });
      setMessages(prev => prev.filter(msg => msg.id !== thinkingId));
      if (!response.ok) throw new Error((await response.json()).detail || response.statusText);

      const payload = await response.json();
      const botId = generateUniqueId();
      setMessages(prev => [
        ...prev,
        {
          id: botId,
          text: payload.reply,
          type: MessageType.BOT,
          timestamp: new Date().toLocaleString(),
          isError: false,
          isGuidance: false,
          quantitativeData: payload.quantitativeData,
          attachments: payload.attachments
        }
      ]);
      setIsProcessing(false);

    } catch (err: any) {
      console.error('Chat error:', err);
      setMessages(prev => [
        ...prev,
        { id: generateUniqueId(), text: `❌ ${err.message}`, type: MessageType.BOT, timestamp: new Date().toLocaleString(), isError: true }
      ]);
      setIsProcessing(false);
    }
  };

  const handleSend = () => {
    if (!query.trim() || isProcessing) return;
    const userId = generateUniqueId();
    setMessages(prev => [
      ...prev,
      { id: userId, text: query, type: MessageType.USER, timestamp: new Date().toLocaleString() }
    ]);
    handleInitialRequest(query, userId);
    setQuery("");
  };

  // Generic attachment renderer
  const renderAttachment = (att: any, index: number) => {
    switch (att.type) {
      case 'chart': {
        const { labels, datasets } = att.data;
        const chartData = labels.map((label: string, idx: number) => {
          const item: any = { x: label };
          datasets.forEach((ds: any) => { item[ds.label] = ds.values[idx]; });
          return item;
        });
        const ChartComponent = att.options?.chartType === 'bar' ? BarChart : LineChart;
        return (
          <ChartComponent
            key={index}
            width={att.options?.width || 400}
            height={att.options?.height || 250}
            data={chartData}
            className="my-4"
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="x" label={att.options?.xAxisLabel && { value: att.options.xAxisLabel, position: 'insideBottom' }} />
            <YAxis label={att.options?.yAxisLabel && { value: att.options.yAxisLabel, angle: -90, position: 'insideLeft' }} />
            <Tooltip />
            <Legend />
            {datasets.map((ds: any, i: number) => (
              att.options?.chartType === 'bar' ?
                <Bar key={i} dataKey={ds.label} name={ds.label} /> :
                <Line key={i} type="monotone" dataKey={ds.label} name={ds.label} />
            ))}
          </ChartComponent>
        );
      }
      case 'table': {
        const { columns, rows } = att.data;
        return (
          <div key={index} className="my-4 overflow-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  {columns.map((col: string) => (
                    <th key={col} className="px-2 py-1 border-b border-gray-600 text-sm text-[#CCCCCC]">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row: any[], rIdx: number) => (
                  <tr key={rIdx} className="border-b border-gray-700 last:border-b-0">
                    {row.map((cell, cIdx) => (
                      <td key={cIdx} className="px-2 py-1 text-sm text-white">{String(cell)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      }
      default:
        return null;
    }
  };

  return (
    <div className="flex flex-col items-center justify-center w-full">
      <div className="w-full max-w-4xl shadow-xl rounded-xl bg-[#252525] text-white p-6 flex flex-col">
        {/* Chat history */}
        <div className="flex-grow h-[70vh] overflow-y-auto rounded-lg p-4 bg-[#333333] space-y-4 mb-4">
          {messages.map(msg => (
            <SimpleErrorBoundary key={msg.id}>
              <div className={`flex ${msg.type === MessageType.USER ? 'justify-end' : 'justify-start'}`}>
                <div className={`max-w-[80%] ${msg.type === MessageType.USER ? 'text-right' : 'text-left'}`}>
                  <span className={`block text-xs mb-1 ${msg.type === MessageType.USER ? 'text-[#AAAAAA]' : 'text-[#CCCCCC]'}`}>{msg.timestamp}</span>
                  <div className={`inline-block p-3 rounded-lg ${
                    msg.isError ? 'bg-red-800 text-red-100' :
                    msg.isGuidance ? 'bg-yellow-700 text-yellow-100' :
                    msg.type === MessageType.USER ? 'bg-[#00A3E0] text-black' : 'bg-[#1A1A1A] text-white'
                  }`}>
                    {msg.isLoading ? (
                      <div className="flex items-center space-x-2 text-sm">
                        <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.3s]"></div>
                        <div className="w-2 h-2 bg-current rounded-full animate-bounce [animation-delay:-0.15s]"></div>
                        <div className="w-2 h-2 bg-current rounded-full animate-bounce"></div>
                        <span>{msg.text}</span>
                      </div>
                    ) : (
                      <>```
