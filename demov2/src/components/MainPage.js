import React, { useState, useRef, useEffect } from 'react';
import './MainPage.css';
import MessageDisplay from './MessageDisplay';

function MainPage() {
  const [isRunning, setIsRunning] = useState(false);
   const [messages, setMessages] = useState(() => {
    const savedMessages = localStorage.getItem('mqttMessages');
    return savedMessages ? JSON.parse(savedMessages) : [];
  });
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [errorMessage, setErrorMessage] = useState('');
  const [csvData, setCsvData] = useState(null);
  const [sourceZone, setSourceZone] = useState('');
  const [showSourceZoneSelect, setShowSourceZoneSelect] = useState(false);
  const fileInputRef = useRef(null);
  const wsRef = useRef(null); 
  // how are you

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
          setShowSourceZoneSelect(true); // Show the dropdown after file selection
          console.log('CSV content:', csvContent);
        };
        reader.readAsText(file);
      } else {
        alert('Please select a valid CSV file');
      }
    }
    event.target.value = '';
  };

  const connectWebSocket = (isResuming=false) => {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//localhost:8001/ws`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      console.log('WebSocket connected to MQTT');
      setConnectionStatus('connected');
      setErrorMessage('');
      if(!isResuming){
        setMessages([]);
      }
    };

    ws.onmessage = (event) => {
      try {
        const messageData = JSON.parse(event.data);
        console.log('Received message:', messageData);
        setMessages((prevMessages) => [
          ...prevMessages,
          {
            
            data: messageData
          }
          
        ]);
      } catch (e) {
        console.error('Failed to parse message:', e);
      }
    };

    ws.onerror = (error) => {   
      console.error('WebSocket error:', error);
      setConnectionStatus('error');
      setErrorMessage('Failed to connect to WebSocket server. Make sure websocket.py is running on port 8001.');
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
    
    const isResuming=messages.length > 0;
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

    const handleExport = () => {
    // Check if there are any messages to export
    if (messages.length === 0) {
      alert('No data to export');
      return;
    }

    // 1. Define the CSV headers
    const headers = ['Tag ID', 'Last Seen Time', 'Previous Zone', 'Current Zone'];

    // 2. Process the messages to create CSV rows
    const rows = messages.map(msg => {
      // Access the payload, which contains the data
      const payload = msg.data.payload;

      // Ensure we have a payload to work with
      if (!payload) {
        return '';
      }

      // Safely get each piece of data, defaulting to 'NA' if not present
      const tagId = payload.Tag || 'NA';
      const timestamp = payload.Timestamp || 'NA';
      const previousZone = payload.Previous_zone || 'NA';
      const currentZone = payload.Current_Zone || 'NA';

      // 3. Format the data into a comma-separated string for the row
      //    Fields are wrapped in quotes to handle any potential commas in the data.
      return `"${tagId}","${timestamp}","${previousZone}","${currentZone}"`;
    }).filter(row => row); // Filter out any empty rows from messages without payloads

    // 4. Combine headers and rows into a single CSV string
    const csvContent = [
      headers.join(','), // Header row
      ...rows           // All the data rows
    ].join('\n');       // Join all lines with a newline

    // 5. Create a blob and trigger the download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    if (link.download !== undefined) { // Feature detection
      const url = URL.createObjectURL(blob);
      const timestamp = new Date().toISOString().replace(/:/g, '-');
      link.setAttribute('href', url);
      link.setAttribute('download', `Zone_Events_${timestamp}.csv`);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }
  };

  useEffect(() => {
    localStorage.setItem('mqttMessages', JSON.stringify(messages));
  }, [messages]);
  useEffect(() => {
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);
  const handleClear = () => {
    setMessages([]);
  };
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
          <button
            className="btn btn-import"
            onClick={handleImport}
          >
            📥 Import
          </button>
          <button
            className={`btn btn-start ${isRunning ? 'disabled' : ''}`}
            onClick={handleStart}
            disabled={isRunning}
          >
            ▶️ Start
          </button>
          <button
            className={`btn btn-stop ${!isRunning ? 'disabled' : ''}`}
            onClick={handleStop}
            disabled={!isRunning}
          >
            ⏹️ Stop
          </button>
          <button
          className="btn btn-clear"
          onClick={handleClear}
        >
          🗑️ Clear
        </button>
          <button
            className="btn btn-export"
            onClick={handleExport}
          >
            📤 Export
          </button>
        </div>
        {showSourceZoneSelect && (
          <div className="source-zone-selector">
            <label htmlFor="source-zone">Source Zone:</label>
            <select
              id="source-zone"
              value={sourceZone}
              onChange={(e) => setSourceZone(e.target.value)}
            >
              <option value="">Select a zone</option>
              <option value="zone 1">Zone 1</option>
              <option value="zone 2">Zone 2</option>
            </select>
          </div>
        )}
        <div className="status">
          <p>Status: <span className={isRunning ? 'running' : 'stopped'}>
            {isRunning ? '▶️ Running' : '⏹️ Stopped'}
          </span></p>
          <p>Connection: <span className={`connection-${connectionStatus}`}>
            {connectionStatus}
          </span></p>
        </div>
        <MessageDisplay messages={messages} filters={filters} />
      </div>
    </div>
  );
}

export default MainPage;
