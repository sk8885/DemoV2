import React, { useState, useRef, useEffect } from 'react';
import './MainPage.css';
import MessageDisplay from './MessageDisplay';

// Custom Hook for CSV Generation and Download
const useGenerateAndDownloadCsv = () => {
    const generateAndDownload = (data, type) => {
        if (!data || data.length === 0) {
            console.log(`No ${type} data to export.`);
            return;
        }

        let headers = ['Type', 'Timestamp', 'Payload'];
        let fileNamePrefix = '';
        let rows = [];

        if (type === 'tag_data') {
            rows = data.map(msg => {
                const payload = msg.data.payload ? JSON.stringify(msg.data.payload) : '{}';
                const type = msg.data.type || 'NA';
                const timestamp = msg.data.timestamp || 'NA';
                return `"${type}","${timestamp}","${payload.replace(/"/g, '""')}"`;
            });
            fileNamePrefix = 'tag_data';
        } else if (type === 'raw_report') {
            rows = data.map(report => {
                const payload = report.payload ? JSON.stringify(report.payload) : '{}';
                const type = report.type || 'NA';
                const timestamp = report.timestamp || 'NA';
                return `"${type}","${timestamp}","${payload.replace(/"/g, '""')}"`;
            });
            fileNamePrefix = 'raw_data(report)';
        }

        if (rows.length === 0) return;

        const csvContent = [headers.join(','), ...rows].join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        
        link.setAttribute('href', url);
        link.setAttribute('download', `${timestamp}_${fileNamePrefix}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    return generateAndDownload;
};

function MainPage() {
    const [isRunning, setIsRunning] = useState(false);
    const [messages, setMessages] = useState(() => {
        const savedMessages = localStorage.getItem('mqttTagMessages');
        return savedMessages ? JSON.parse(savedMessages) : [];
    });
    
    // --- NEW: Load raw messages from localStorage ---
    const [rawMessages, setRawMessages] = useState(() => {
        const savedRawMessages = localStorage.getItem('mqttRawMessages');
        return savedRawMessages ? JSON.parse(savedRawMessages) : [];
    });
    const [connectionStatus, setConnectionStatus] = useState('disconnected');
    const [errorMessage, setErrorMessage] = useState('');
    const [csvData, setCsvData] = useState(null);
    const [sourceZone, setSourceZone] = useState('');
    const [showSourceZoneSelect, setShowSourceZoneSelect] = useState(false);
    
    const fileInputRef = useRef(null);
    const wsRef = useRef(null);
    // --- NEW: Instantiate the CSV download hook ---
    const generateAndDownloadCsv = useGenerateAndDownloadCsv();

    // --- NEW: Effect for 15-minute automatic export ---
    useEffect(() => {
        const interval = setInterval(() => {
            console.log("Auto-saving data due to 15-minute interval...");
            // We pass a function to get the latest state directly
            setMessages(currentMessages => {
                generateAndDownloadCsv(currentMessages, 'tag_data');
                return currentMessages;
            });
            setRawMessages(currentRawMessages => {
                generateAndDownloadCsv(currentRawMessages, 'raw_report');
                return currentRawMessages;
            });
        }, 15 * 60 * 1000); // 15 minutes in milliseconds

        return () => clearInterval(interval); // Cleanup on component unmount
    }, [generateAndDownloadCsv]);

    const handleImport = () => {
        fileInputRef.current?.click();
    };

    const handleFileSelect = (event) => {
        const file = event.target.files?.[0];
        if (file) {
            if (file.type === 'text/csv' || file.name.endsWith('.csv')) {
                console.log('CSV file selected:', file.name);
                const reader = new FileReader();
                reader.onload = (e) => {
                    const csvContent = e.target?.result;
                    setCsvData(csvContent);
                    setShowSourceZoneSelect(true);
                    console.log('CSV content:', csvContent);
                };
                reader.readAsText(file);
            } else {
                alert('Please select a valid CSV file');
            }
        }
        event.target.value = '';
    };

    const connectWebSocket = (isResuming = false) => {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${wsProtocol}//localhost:8001/ws`;
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            console.log('WebSocket connected to MQTT');
            setConnectionStatus('connected');
            setErrorMessage('');
            if (!isResuming) {
                setMessages([]);
                setRawMessages([]); // Also clear raw messages on a fresh start
            }
        };

        ws.onmessage = (event) => {
            try {
                const messageData = JSON.parse(event.data);
                console.log('Received message:', messageData);
                if (messageData.type === 'report') {
                    setRawMessages((prevRawMessages) => [
                        ...prevRawMessages,
                        messageData,
                    ]);
                } else if (messageData.type === 'tag') {
                    setMessages((prevMessages) => [
                        ...prevMessages,
                        { data: messageData },
                    ]);
                }
            } catch (e) {
                console.error('Failed to parse message:', e);
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            setConnectionStatus('error');
            setErrorMessage('Failed to connect. Make sure websocket.py is running on port 8001.');
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
            setConnectionStatus('disconnected');
        };

        wsRef.current = ws;
    };

    const handleStart = () => {
        setIsRunning(true);
        setConnectionStatus('connecting');
        setErrorMessage('');
        console.log('Connecting to WebSocket server...');
        const isResuming = messages.length > 0 || rawMessages.length > 0;
        connectWebSocket(isResuming);
    };

    const handleStop = () => {
        setIsRunning(false);
        console.log('Disconnecting...');
        if (wsRef.current) {
            wsRef.current.close();
            wsRef.current = null;
        }
        setConnectionStatus('disconnected');
    };

    // --- MODIFIED: The handleExport button now just calls our hook ---
    const handleExport = () => {
        generateAndDownloadCsv(messages, 'tag_data');
        generateAndDownloadCsv(rawMessages, 'raw_report');
    };
    
    // --- MODIFIED: handleClear now saves before clearing ---
    const handleClear = () => {
        console.log("Clearing data and saving current state...");
        // Save current data before clearing
        // generateAndDownloadCsv(messages, 'tag_data');
        // generateAndDownloadCsv(rawMessages, 'raw_report');
        
        // Now clear the state
        setMessages([]);
        setRawMessages([]);
    };  

    // Save tag messages to localStorage
    useEffect(() => {
        localStorage.setItem('mqttTagMessages', JSON.stringify(messages));
    }, [messages]);
    
    // --- NEW: Save raw messages to localStorage ---
    useEffect(() => {
        localStorage.setItem('mqttRawMessages', JSON.stringify(rawMessages));
    }, [rawMessages]);

    useEffect(() => {
        return () => {
            if (wsRef.current) {
                wsRef.current.close();
            }
        };
    }, []);

    const filters = {
        csvData,
        sourceZone,
    };

    return ( 
        <div className="main-page">
            <input
                ref={fileInputRef}
                type="file"
                accept=".csv"
                onChange={handleFileSelect}
                style={{ display: 'none' }}
            />
            <div className="container">
                <h1>Control Panel</h1>
                {errorMessage && <div className="error-banner">{errorMessage}</div>}
                <div className="button-group">
                    <button className="btn btn-import" onClick={handleImport}>📥 Import</button>
                    <button className={`btn btn-start ${isRunning ? 'disabled' : ''}`} onClick={handleStart} disabled={isRunning}>▶️ Start</button>
                    <button className={`btn btn-stop ${!isRunning ? 'disabled' : ''}`} onClick={handleStop} disabled={!isRunning}>⏹️ Stop</button>
                    <button className="btn btn-clear" onClick={handleClear}>🗑️ Clear</button>
                    <button className="btn btn-export" onClick={handleExport}>📤 Export All</button>
                </div>
                {showSourceZoneSelect && (
                    <div className="source-zone-selector">
                        <label htmlFor="source-zone">Source Zone:</label>
                        <select id="source-zone" value={sourceZone} onChange={(e) => setSourceZone(e.target.value)}>
                            <option value="">Select a zone</option>
                            <option value="zone 1">Zone 1</option>
                            <option value="zone 2">Zone 2</option>
                        </select>
                    </div>
                )}
                <div className="status">
                    <p>Status: <span className={isRunning ? 'running' : 'stopped'}>{isRunning ? '▶️ Running' : '⏹️ Stopped'}</span></p>
                    <p>Connection: <span className={`connection-${connectionStatus}`}>{connectionStatus}</span></p>
                </div>
                <MessageDisplay messages={messages} filters={filters} />
            </div>
        </div>
    );
}

export default MainPage;
