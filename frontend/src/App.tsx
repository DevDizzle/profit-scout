import React from "react";
import Header from "./components/Header";
import ChatUI from "./components/ChatUI";

function App() {
  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      <Header />
      <main className="flex flex-1 items-center justify-center p-4">
        <ChatUI />
      </main>
    </div>
  );
}

export default App;
