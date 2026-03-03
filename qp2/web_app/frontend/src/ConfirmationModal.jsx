import React from 'react';
import Modal from 'react-modal';
import './ConfirmationModal.css';

Modal.setAppElement('#root');

const ConfirmationModal = ({ isOpen, title, message, onConfirm, onCancel, confirmText = "Confirm", cancelText = "Cancel" }) => {
    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onCancel}
            contentLabel="Confirmation"
            className="confirm-modal-content"
            overlayClassName="confirm-modal-overlay"
        >
            <div className="confirm-header">
                <h3>{title}</h3>
            </div>
            <div className="confirm-body">
                <p>{message}</p>
            </div>
            <div className="confirm-actions">
                <button onClick={onCancel} className="confirm-cancel-btn">{cancelText}</button>
                <button onClick={onConfirm} className="confirm-ok-btn">{confirmText}</button>
            </div>
        </Modal>
    );
};

export default ConfirmationModal;
