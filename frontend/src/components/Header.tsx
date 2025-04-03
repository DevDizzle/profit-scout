import React from "react";

const Header: React.FC = () => {
  return (
    <header className="w-full py-10 px-6 text-center bg-transparent">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-4xl md:text-5xl font-bold text-white mb-3 tracking-tight">
          ProfitScout
        </h1>
        <p className="text-md md:text-lg text-[#CCCCCC] leading-relaxed mb-3"> {/* Added margin-bottom */}
          Your personal investing assistant—powered by AI. ProfitScout retrieves insights from 10-K filings,
          highlights key metrics, and delivers value-driven stock analysis so you can invest smarter.
        </p>
        {/* Added Instruction Line */}
        <p className="text-base text-cyan-400"> {/* Use a slightly different color for emphasis */}
          Enter an S&P 500 ticker (e.g., <code className="bg-gray-700 px-1.5 py-0.5 rounded text-xs text-white font-mono">AAPL</code>) or company name below to start.
        </p>
      </div>
    </header>
  );
};

export default Header;
