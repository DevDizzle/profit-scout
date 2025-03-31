import React from "react";

const Header: React.FC = () => {
  return (
    <header className="bg-gradient-to-r from-indigo-700 to-purple-600 text-white py-8 px-6 shadow-md">
      <div className="max-w-4xl mx-auto text-center">
        <h1 className="text-4xl md:text-5xl font-bold mb-4">ProfitScout</h1>
        <p className="text-lg md:text-xl">
          ProfitScout is your personal investing assistant, designed to simplify financial analysis. Powered by AI, it retrieves insights from 10-K filings, highlights key metrics, and provides value-driven stock recommendations. Discover smarter investing with ProfitScout!
        </p>
      </div>
    </header>
  );
};

export default Header;
