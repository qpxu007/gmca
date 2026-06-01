
import React from 'react';
import { useDraggable } from '@dnd-kit/core';
import { CSS } from '@dnd-kit/utilities';

export default function Puck({ id, puckData, slotName, onDoubleClick }) {
    const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
        id: id,
        data: { puckData } // Pass data for drag events
    });

    const style = {
        transform: CSS.Translate.toString(transform),
        zIndex: isDragging ? 100 : 'auto',
        opacity: isDragging ? 0.8 : 1,
    };

    // Apply the same CrystalID transformation as the editor/export:
    // if CrystalID matches the original Port, remap to the current slot.
    const getDisplayCrystalID = (row, index) => {
        const cid = row.CrystalID || "";
        const oldPort = (row.Port || "").trim();
        if (slotName && cid === oldPort) {
            return `${slotName}${index + 1}`;
        }
        return cid;
    };

    const getSummary = () => {
        if (!puckData || !puckData.rows) return "Empty";
        const rows = puckData.rows;
        const count = rows.filter(r => r.CrystalID).length;
        const firstIndex = rows.findIndex(r => r.CrystalID);
        const firstId = firstIndex >= 0 ? getDisplayCrystalID(rows[firstIndex], firstIndex) : "Empty";

        let text = `${count} Crystals\nFirst: ${firstId}`;

        const firstDataRow = firstIndex >= 0 ? rows[firstIndex] : null;
        if (firstDataRow) {
            if (firstDataRow.Protein) text += `\n(Protein: ${firstDataRow.Protein})`;
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
            <div className="puck-title">Puck {puckData.rows?.find(r => r.Puck?.trim())?.Puck.trim() || puckData.original_label}</div>
            <div className="puck-info">{getSummary()}</div>
        </div>
    );
}
