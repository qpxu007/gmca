import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { API_URL } from './api.js'

// Global frontend error reporter — sends JS errors to the server log.
// Uses plain fetch (not axiosInstance) so it works before auth is set up.
const _reported = new Set();
function reportToServer(level, message, stack, component) {
    // Deduplicate identical messages within a session
    const key = `${level}:${message}`;
    if (_reported.has(key)) return;
    _reported.add(key);
    fetch(`${API_URL}/log`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ level, message, stack, url: window.location.href, component }),
    }).catch(() => {}); // never let the reporter itself throw
}

window.onerror = (message, source, lineno, colno, error) => {
    reportToServer('error', String(message), error?.stack, `${source}:${lineno}:${colno}`);
    return false; // let default handling continue
};

window.onunhandledrejection = (event) => {
    const err = event.reason;
    reportToServer('error', err?.message || String(err), err?.stack, 'unhandledrejection');
};

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
