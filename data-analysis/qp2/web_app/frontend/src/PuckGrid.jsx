
import React from 'react';
import Slot from './Slot';
import Puck from './Puck';

export default function PuckGrid({ puckNames, slotsMap, onPuckDoubleClick }) {
    return (
        <div className="puck-grid">
            {puckNames.map(name => {
                const puckData = slotsMap[name];
                return (
                    <Slot key={name} id={name}>
                        {puckData && (
                            // Puck ID must be unique. Use original label is risky if duplicates allowed?
                            // Logic guarantees unique original_label in load? 
                            // Let's use `puck-${puckData.original_label}`.
                            <Puck 
                                id={`puck-${puckData.original_label}`} 
                                puckData={puckData} 
                                onDoubleClick={() => onPuckDoubleClick(name, puckData)}
                            />
                        )}
                    </Slot>
                );
            })}
        </div>
    );
}
