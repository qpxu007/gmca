
import React from 'react';
import { useDroppable } from '@dnd-kit/core';

export default function Slot({ id, children }) {
    const { isOver, setNodeRef } = useDroppable({
        id: id,
    });

    const style = {
        backgroundColor: isOver ? '#d1ecf1' : undefined,
    };

    return (
        <div ref={setNodeRef} style={style} className="slot">
            <div className="slot-label">Slot {id}</div>
            {children}
        </div>
    );
}
