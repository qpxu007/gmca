import React from 'react';
import Modal from 'react-modal';

Modal.setAppElement('#root');

const TextModal = ({ isOpen, onClose, title, content }) => {
    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel={title}
            style={{
                content: {
                    top: '5%',
                    left: '5%',
                    right: '5%',
                    bottom: '5%',
                    padding: '15px',
                    overflow: 'auto',
                    resize: 'both',
                }
            }}
        >
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
                <h3 style={{ margin: 0 }}>{title}</h3>
                <button onClick={onClose} style={{ cursor: 'pointer', padding: '5px 10px' }}>Close</button>
            </div>
            <pre style={{
                backgroundColor: '#f4f4f4',
                padding: '15px',
                borderRadius: '4px',
                overflowX: 'auto',
                whiteSpace: 'pre',
                fontFamily: 'monospace',
                fontSize: '0.85rem'
            }}>
                {content || 'No content available'}
            </pre>
        </Modal>
    );
};

export default TextModal;
