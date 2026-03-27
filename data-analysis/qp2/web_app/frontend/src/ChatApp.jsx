import React, { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, MessageCircle, Send, Mic, MicOff, Bot } from 'lucide-react';
import { api, API_URL } from './api';
import './ChatApp.css';

const ChatApp = () => {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState('');
    const [sending, setSending] = useState(false);
    const [listening, setListening] = useState(false);
    const [aiLoading, setAiLoading] = useState(false);
    const messagesEndRef = useRef(null);
    const seenIds = useRef(new Set());
    const eventSourceRef = useRef(null);
    const recognitionRef = useRef(null);
    const user = localStorage.getItem('user') || 'anonymous';

    // Scroll to bottom on new messages
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages]);

    // Load history + connect SSE
    useEffect(() => {
        loadHistory();
        connectSSE();
        return () => {
            if (eventSourceRef.current) {
                eventSourceRef.current.close();
            }
        };
    }, []);

    const loadHistory = async () => {
        try {
            const data = await api.chatHistory();
            const msgs = data.messages || [];
            msgs.forEach(m => seenIds.current.add(m.msg_id));
            setMessages(msgs);
        } catch (e) {
            console.error('Failed to load chat history', e);
        }
    };

    const connectSSE = () => {
        const token = localStorage.getItem('token');
        if (!token) return;
        const es = new EventSource(`${API_URL}/chat/stream?token=${token}`);
        es.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                if (seenIds.current.has(msg.msg_id)) return;
                seenIds.current.add(msg.msg_id);
                setMessages(prev => [...prev, msg]);
            } catch (e) {
                console.error('SSE parse error', e);
            }
        };
        es.onerror = () => {
            // EventSource auto-reconnects
        };
        eventSourceRef.current = es;
    };

    const handleSend = async () => {
        const text = input.trim();
        if (!text || sending) return;
        setInput('');
        setSending(true);
        try {
            const res = await api.chatSend(text);
            // Message will arrive via SSE; add optimistically with returned msg_id
            seenIds.current.add(res.msg_id);
            setMessages(prev => [...prev, {
                role: 'user', content: text, user, timestamp: Date.now() / 1000, msg_id: res.msg_id
            }]);
        } catch (e) {
            console.error('Send failed', e);
        } finally {
            setSending(false);
        }
    };

    const handleAskAI = async () => {
        const text = input.trim();
        if (!text || aiLoading) return;
        setInput('');
        setAiLoading(true);
        try {
            await api.chatAskAI(text);
            // Both user msg and AI response arrive via SSE
        } catch (e) {
            console.error('AI request failed', e);
        } finally {
            setAiLoading(false);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    // --- Voice input ---
    const toggleVoice = () => {
        if (!('webkitSpeechRecognition' in window || 'SpeechRecognition' in window)) {
            alert('Speech recognition not supported in this browser.');
            return;
        }
        if (listening) {
            recognitionRef.current?.stop();
            setListening(false);
            return;
        }
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        const recognition = new SpeechRecognition();
        recognition.continuous = false;
        recognition.interimResults = false;
        recognition.lang = 'en-US';
        recognition.onresult = (event) => {
            const transcript = event.results[0][0].transcript;
            setInput(prev => prev + transcript);
        };
        recognition.onend = () => setListening(false);
        recognition.onerror = () => setListening(false);
        recognitionRef.current = recognition;
        recognition.start();
        setListening(true);
    };

    // --- Render helpers ---
    const formatTime = (ts) => {
        const d = new Date(ts * 1000);
        return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    const renderContent = (text) => {
        // Split on code blocks ```...```
        const parts = text.split(/(```[\s\S]*?```)/g);
        return parts.map((part, i) => {
            if (part.startsWith('```') && part.endsWith('```')) {
                const inner = part.slice(3, -3);
                // Remove optional language tag on first line
                const lines = inner.split('\n');
                const code = (lines[0].match(/^\w+$/) ? lines.slice(1) : lines).join('\n');
                return <pre key={i} className="chat-code-block"><code>{code}</code></pre>;
            }
            return <span key={i}>{part}</span>;
        });
    };

    return (
        <div className="chat-container">
            <div className="chat-toolbar">
                <Link to="/dashboard" className="back-link">
                    <ArrowLeft size={20} />
                    Dashboard
                </Link>
                <div className="chat-toolbar-title">
                    <MessageCircle size={20} />
                    AI Chat
                </div>
                <div style={{ flex: 1 }} />
            </div>

            <div className="chat-messages">
                {messages.map((msg, idx) => {
                    if (msg.role === 'event') {
                        return (
                            <div key={msg.msg_id || idx} className="chat-event">
                                {msg.content}
                            </div>
                        );
                    }
                    const isMe = msg.user === user;
                    const isAI = msg.role === 'assistant';
                    return (
                        <div key={msg.msg_id || idx} className={`chat-bubble ${isAI ? 'ai' : isMe ? 'me' : 'other'}`}>
                            <div className="chat-bubble-header">
                                <span className="chat-user">{msg.user}</span>
                                <span className="chat-time">{formatTime(msg.timestamp)}</span>
                            </div>
                            <div className="chat-bubble-content">
                                {renderContent(msg.content)}
                            </div>
                        </div>
                    );
                })}
                <div ref={messagesEndRef} />
            </div>

            <div className="chat-input-bar">
                <button
                    className={`chat-voice-btn ${listening ? 'active' : ''}`}
                    onClick={toggleVoice}
                    title={listening ? 'Stop listening' : 'Voice input'}
                >
                    {listening ? <MicOff size={20} /> : <Mic size={20} />}
                </button>
                <textarea
                    className="chat-input"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Type a message..."
                    rows={1}
                />
                <button
                    className="chat-send-btn"
                    onClick={handleSend}
                    disabled={!input.trim() || sending}
                    title="Send message"
                >
                    <Send size={20} />
                </button>
                <button
                    className="chat-ai-btn"
                    onClick={handleAskAI}
                    disabled={!input.trim() || aiLoading}
                    title="Ask AI"
                >
                    <Bot size={20} />
                    {aiLoading && <span className="chat-spinner" />}
                </button>
            </div>
        </div>
    );
};

export default ChatApp;
