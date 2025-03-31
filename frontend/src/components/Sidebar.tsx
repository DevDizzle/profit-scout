import React from "react";

const Sidebar: React.FC = () => {
  return (
    <aside className="w-64 bg-gray-50 border-r border-gray-200 p-4">
      <h2 className="text-2xl font-semibold mb-4">Analysis Tools</h2>
      <ul className="space-y-2">
        <li className="p-2 rounded hover:bg-gray-100 cursor-pointer">Market Trends</li>
        <li className="p-2 rounded hover:bg-gray-100 cursor-pointer">Risk Assessment</li>
        <li className="p-2 rounded hover:bg-gray-100 cursor-pointer">Portfolio Optimization</li>
        <li className="p-2 rounded hover:bg-gray-100 cursor-pointer">News Analysis</li>
      </ul>
    </aside>
  );
};

export default Sidebar;
