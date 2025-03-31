import React from "react";

const Header: React.FC = () => {
  return (
    <header className="w-full py-10 px-6 text-center bg-transparent">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-4xl md:text-5xl font-bold text-white mb-3 tracking-tight">
          ProfitScout
        </h1>
        <p className="text-md md:text-lg text-[#CCCCCC] leading-relaxed">
          Your personal investing assistant—powered by AI. ProfitScout retrieves insights from 10-K filings,
          highlights key metrics, and delivers value-driven stock recommendations so you can invest smarter.
        </p>
      </div>
    </header>
  );
};

export default Header;
