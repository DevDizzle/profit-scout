import React from "react";

const Header: React.FC = () => {
  return (
    <header className="bg-gradient-to-r from-blue-900 to-indigo-900 text-white py-6 px-4 shadow-md">
      <h1 className="text-4xl font-bold">Profit Scout</h1>
      <p className="mt-2 text-lg italic">
        Financial Analysis Powered by Futuristic AI Agents
      </p>
    </header>
  );
};

export default Header;
