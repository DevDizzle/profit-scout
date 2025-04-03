import React from 'react';

class SimpleErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI.
    return { hasError: true, error: error };
  }

  componentDidCatch(error, errorInfo) {
    // You can log the error to an error reporting service
    // or just log it to the console for debugging.
    console.error("ErrorBoundary caught an error:", error, errorInfo);
    this.setState({ errorInfo: errorInfo }); // Store errorInfo too if needed
  }

  render() {
    if (this.state.hasError) {
      // You can render any custom fallback UI
      return (
        <div className="p-3 rounded-lg bg-red-800 text-red-100">
          <p className="font-semibold">⚠️ Error rendering this message.</p>
          {/* Optional: Display error details during development */}
          {/*
          <details className="text-xs mt-1">
             <summary>Details</summary>
             <pre className="mt-1 whitespace-pre-wrap">
               {this.state.error && this.state.error.toString()}
               <br />
               {this.state.errorInfo && this.state.errorInfo.componentStack}
             </pre>
           </details>
          */}
        </div>
      );
    }

    // If no error, render the children components normally
    return this.props.children;
  }
}

export default SimpleErrorBoundary;
