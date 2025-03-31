import React from "react";
import Header from "./components/Header";
import Sidebar from "./components/Sidebar";
import ChatUI from "./components/ChatUI";

function App() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <div className="flex flex-1">
        <Sidebar />
        <main className="flex-1 bg-gray-100 p-4">
          <ChatUI />
        </main>
      </div>
    </div>
  );
}

export default App;
