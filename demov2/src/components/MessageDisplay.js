import React, { useState, useEffect, useRef } from 'react';
import './MessageDisplay.css';

// This helper function remains the same
const parseCSVForTagIds = (csvText) => {
  if (!csvText) return new Set();
  const lines = csvText.trim().split('\n');
  const tagIds = lines.slice(1).map(line => line.split(',')[0].trim()).filter(Boolean);
  return new Set(tagIds);
};

function MessageDisplay({ messages, filters }) {
  const [displayData, setDisplayData] = useState([]);
  const [isCsvMode, setIsCsvMode] = useState(false);
  const displayDataRef = useRef(displayData);
  const processedMessagesCountRef = useRef(0); // Ref to track processed messages

  useEffect(() => {
    displayDataRef.current = displayData;
  }, [displayData]);

  useEffect(() => {
    if (filters && filters.csvData) {
      const parsedTags = parseCSVForTagIds(filters.csvData);
      const initialData = Array.from(parsedTags).map(tag => ({
        Tag: tag,
        State: 'Pending',
        Timestamp: '---',
        isKnown: true,
        updateId: 0,
      }));
      setDisplayData(initialData);
      setIsCsvMode(true);
      processedMessagesCountRef.current = 0; // Reset counter for new CSV data
      console.log(initialData);
    } else {
      setDisplayData([]);
      setIsCsvMode(false);
      processedMessagesCountRef.current = 0; // Reset counter when not in CSV mode
    }
  }, [filters.csvData]);

  useEffect(() => {
    // If there are no new messages, do nothing.
    const newMessages = messages.slice(processedMessagesCountRef.current);
    if (newMessages.length === 0 ) return;

    let newDisplayData = [...displayDataRef.current];
    let needsUpdate = false;

    // Process only the new messages
    

    newMessages.forEach(message => {
      const  processedPayload= message.data.payload;
      if (processedPayload && processedPayload.Tag) {
        const latestMessagePayload = {
            ...processedPayload,
            Tag: processedPayload.Tag.toUpperCase()
        };
        needsUpdate = true;
        const tagToUpdate = latestMessagePayload.Tag;
        const existingItemIndex = newDisplayData.findIndex(item => item.Tag === tagToUpdate);

        if (existingItemIndex !== -1) {
          if(newDisplayData[existingItemIndex].isKnown){
            newDisplayData[existingItemIndex] = {
            ...latestMessagePayload,
            isKnown:true,
            updateId: Date.now(),
          };
          }
          else{
            newDisplayData[existingItemIndex] = {
              ...latestMessagePayload,
              isKnown:false,
              updateId: Date.now(),
            };
          }
        } 
        else{
          const newItem = {
            ...latestMessagePayload,
            isKnown: false,
            updateId: Date.now(),
          };
          newDisplayData.push(newItem);
        }
      }
    });

    if (needsUpdate) {
      newDisplayData.sort((a, b) => (b.updateId || 0) - (a.updateId || 0));
      setDisplayData(newDisplayData);
    }

    // Update the counter to the new length
    processedMessagesCountRef.current = messages.length;

  }, [messages, isCsvMode]);

  const title = isCsvMode
    ? `Watchlist Active (${displayData.length} Tags)`
    : `Real-time Zone Events (${displayData.length})`;

  return (
    <div className="messages-container">
      <h2>{title}</h2>
      {displayData.length === 0 ? (
        <p className="no-messages">No data to display</p>
      ) : (
        <table className="messages-table">
          <thead>
            <tr>
              <th>Tag ID</th>
              <th>State</th>
              <th>Last Seen Time</th>
              <th>Zone Transition</th>
            </tr>
          </thead>
          <tbody>
            {displayData.map(item => {
              let rowClassName = 'message-item';
              if(item.State === 'Pending') {
                rowClassName += ' placeholder-row';
              } else if (isCsvMode) {
                if (item.isKnown) {
                  if (item.State === 'TRANSITION') {
                    if (item.Current_Zone === 2) rowClassName += ' green-background';
                    else if (item.Current_Zone === 1) rowClassName += ' yellow-background';
                  }
                } else {
                  if(item.State==='TRANSITION'){
                    rowClassName += ' red-background';
                  }
                }
              } else {
                if(item.State === 'TRANSITION'){
                if (item.Current_Zone === 2) rowClassName += ' green-background';
                else if (item.Current_Zone === 1) rowClassName += ' yellow-background';
                }
              }

              const previousZone =item.Previous_Zone || 'NA';
              const currentZone = item.Current_Zone ||  'NA';
              const zoneTransition = (item.Current_Zone ) ? `${previousZone} → ${currentZone}` : '';

              return (
                <tr key={item.Tag} className={rowClassName}>
                  <td>{item.Tag}</td>
                  <td>{item.State || 'NA'}</td>
                  <td>{item.Timestamp}</td>
                  <td>{zoneTransition}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default MessageDisplay;
