
import React from 'react';
import { useDraggable } from '@dnd-kit/core';
import { CSS } from '@dnd-kit/utilities';

export default function Puck({ id, puckData, onDoubleClick }) {
    const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
        id: id,
        data: { puckData } // Pass data for drag events
    });

    const style = {
        transform: CSS.Translate.toString(transform),
        zIndex: isDragging ? 100 : 'auto',
        opacity: isDragging ? 0.8 : 1,
    };

    // Helper to generate summary similar to logic.py
    const getSummary = () => {
        if (!puckData || !puckData.rows) return "Empty";
        const rows = puckData.rows;
        const count = rows.filter(r => r.CrystalID).length;
        const firstId = rows.find(r => r.CrystalID)?.CrystalID || "Empty";
        
        let text = `${count} Crystals\nFirst: ${firstId}`;
        
        // Find first data row
        const firstDataRow = rows.find(r => r.CrystalID);
        if (firstDataRow) {
            if (firstDataRow.Protein) text += `\n(Protein: ${firstDataRow.Protein})`;
            // Limit length?
        }
        return text;
    };

    return (
        <div 
            ref={setNodeRef} 
            style={style} 
            {...listeners} 
            {...attributes} 
            className="puck"
            onDoubleClick={onDoubleClick}
        >
            <div className="puck-title">Puck {puckData.original_label}</div>
            <div className="puck-info">{getSummary()}</div>
        </div>
    );
}
