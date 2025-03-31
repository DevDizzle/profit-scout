import React from "react";
import Header from "./components/Header";
import ChatUI from "./components/ChatUI";

function App() {
  return (
    <div className="min-h-screen flex flex-col bg-gradient-to-b from-[#1A1A1A] to-[#252525] text-white">
      <Header />
      <main className="flex flex-1 items-center justify-center px-4 py-8">
        <ChatUI />
      </main>
    </div>
  );
}

export default App;
