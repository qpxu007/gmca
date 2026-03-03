import React, { useState } from 'react';
import Modal from 'react-modal';
import RunManager from './RunManager';
import StaffManager from './StaffManager';
import QuotaManager from './QuotaManager';
import AvailabilityManager from './AvailabilityManager';
import DayTypeManager from './DayTypeManager'; // Import
import './ConfigModal.css';

Modal.setAppElement('#root');

const ConfigModal = ({ isOpen, onClose }) => {
    const [activeTab, setActiveTab] = useState('runs');

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Configuration"
            className="modal-content large-modal"
            overlayClassName="modal-overlay"
        >
            <div className="config-header">
                <h2>Configuration</h2>
                <div className="tabs">
                    <button 
                        className={`tab-btn ${activeTab === 'runs' ? 'active' : ''}`}
                        onClick={() => setActiveTab('runs')}
                    >
                        Runs
                    </button>
                    <button 
                        className={`tab-btn ${activeTab === 'staff' ? 'active' : ''}`}
                        onClick={() => setActiveTab('staff')}
                    >
                        Staff
                    </button>
                    <button 
                        className={`tab-btn ${activeTab === 'daytypes' ? 'active' : ''}`}
                        onClick={() => setActiveTab('daytypes')}
                    >
                        Day Types
                    </button>
                    <button 
                        className={`tab-btn ${activeTab === 'quotas' ? 'active' : ''}`}
                        onClick={() => setActiveTab('quotas')}
                    >
                        Quotas
                    </button>
                    <button 
                        className={`tab-btn ${activeTab === 'availability' ? 'active' : ''}`}
                        onClick={() => setActiveTab('availability')}
                    >
                        Availability
                    </button>
                </div>
            </div>

            <div className="config-body">
                {activeTab === 'runs' && <RunManager />}
                {activeTab === 'staff' && <StaffManager />}
                {activeTab === 'daytypes' && <DayTypeManager />}
                {activeTab === 'quotas' && <QuotaManager />}
                {activeTab === 'availability' && <AvailabilityManager />}
            </div>

            <div className="modal-actions">
                <button onClick={onClose} className="cancel-btn">Close</button>
            </div>
        </Modal>
    );
};

export default ConfigModal;
