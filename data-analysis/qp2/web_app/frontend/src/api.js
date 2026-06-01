
import axios from 'axios';

export const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'; // Adjust if hosted elsewhere

// Create axios instance with interceptor
const axiosInstance = axios.create({
    baseURL: API_URL,
    withCredentials: true,
});

axiosInstance.interceptors.response.use(
    (response) => response,
    (error) => {
        if (error.response && error.response.status === 401) {
            localStorage.removeItem('user');
            localStorage.removeItem('is_admin');
            localStorage.removeItem('beamline');
            localStorage.removeItem('groups');
            const basePath = import.meta.env.BASE_URL || '/';
            window.location.href = `${basePath}login?expired=1`;
        }
        return Promise.reject(error);
    }
);

export const api = {
  uploadFile: async (file, puckNames) => {
    const formData = new FormData();
    formData.append('file', file);
    if (puckNames) {
      formData.append('puck_names', puckNames);
    }
    const response = await axiosInstance.post(`/upload`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  createEmpty: async (puckNames) => {
    const params = new URLSearchParams();
    if (puckNames) {
      params.append('puck_names', puckNames);
    }
    const response = await axiosInstance.post(`/create_empty?${params.toString()}`);
    return response.data;
  },

  exportFile: async (payload) => {
    // payload: { puck_names: [], slots: [], filename: "name.xlsx" }
    const response = await axiosInstance.post(`/export`, payload, {
      responseType: 'blob', // Important for file download
    });
    return response;
  },

  sendToHttp: async (payload) => {
    // payload: { puck_names: [], slots: [], rpc_url: "..." }
    const response = await axiosInstance.post(`/send_to_http`, payload);
    return response.data;
  },

  // Database endpoints
  saveSpreadsheet: async (payload) => {
    // payload: { name: "...", puck_names: [], slots: [] }
    const response = await axiosInstance.post(`/spreadsheets/save`, payload);
    return response.data;
  },

  listSpreadsheets: async () => {
    const response = await axiosInstance.get(`/spreadsheets/list`);
    return response.data;
  },

  getSpreadsheet: async (id) => {
    const response = await axiosInstance.get(`/spreadsheets/${id}`);
    return response.data;
  },

  deleteSpreadsheet: async (id) => {
    const response = await axiosInstance.delete(`/spreadsheets/${id}`);
    return response.data;
  },

  // Scheduler Endpoints
  listRuns: async () => {
    const response = await axiosInstance.get(`/scheduler/runs`);
    return response.data;
  },

  createRun: async (data) => {
    const response = await axiosInstance.post(`/scheduler/runs`, data);
    return response.data;
  },

  deleteRun: async (id) => {
    const response = await axiosInstance.delete(`/scheduler/runs/${id}`);
    return response.data;
  },

  listBeamlines: async () => {
    const response = await axiosInstance.get(`/scheduler/beamlines`);
    return response.data;
  },

  listStaff: async () => {
    const response = await axiosInstance.get(`/scheduler/staff`);
    return response.data;
  },

  createStaff: async (data) => {
    const response = await axiosInstance.post(`/scheduler/staff`, data);
    return response.data;
  },

  updateStaff: async (data) => {
    const response = await axiosInstance.put(`/scheduler/staff`, data);
    return response.data;
  },

  deleteStaff: async (id) => {
    const response = await axiosInstance.delete(`/scheduler/staff/${id}`);
    return response.data;
  },

  listDayTypes: async () => {
    const response = await axiosInstance.get(`/scheduler/day_types`);
    return response.data;
  },

  createDayType: async (data) => {
    const response = await axiosInstance.post(`/scheduler/day_types`, data);
    return response.data;
  },

  updateDayType: async (data) => {
    const response = await axiosInstance.put(`/scheduler/day_types`, data);
    return response.data;
  },

  deleteDayType: async (id) => {
    const response = await axiosInstance.delete(`/scheduler/day_types/${id}`);
    return response.data;
  },

  getSchedule: async (runId) => {
    const response = await axiosInstance.get(`/scheduler/schedule/${runId}`);
    return response.data;
  },

  initDefaults: async () => {
    const response = await axiosInstance.post(`/scheduler/init_defaults`);
    return response.data;
  },

  updateScheduleDay: async (payload) => {
    // payload: { day_id, day_type_id, assigned_staff_id }
    const response = await axiosInstance.post(`/scheduler/day`, payload);
    return response.data;
  },

  // Quotas
  listQuotas: async (runId) => {
    const response = await axiosInstance.get(`/scheduler/quotas/${runId}`);
    return response.data;
  },

  updateQuota: async (data) => {
    // data: { staff_id, run_id, max_days, max_weekends }
    const response = await axiosInstance.post(`/scheduler/quotas`, data);
    return response.data;
  },

  // Availability
  listAvailability: async (staffId) => {
    const response = await axiosInstance.get(`/scheduler/availability/${staffId}`);
    return response.data;
  },

  updateAvailability: async (data) => {
    // data: { staff_id, date, preference }
    const response = await axiosInstance.post(`/scheduler/availability`, data);
    return response.data;
  },

  autoAssign: async (runId, overwrite = false) => {
    const response = await axiosInstance.post(`/scheduler/auto_assign/${runId}?overwrite=${overwrite}`);
    return response.data;
  },

  exportStaffSchedule: async (staffId) => {
    const response = await axiosInstance.get(`/scheduler/export/ics/${staffId}`, {
      responseType: 'blob', // Important for file download
    });
    return response;
  },

  // Datasets
  listDatasets: async (params = {}) => {
    // params: { search, limit, offset, sort_by, sort_desc }
    const response = await axiosInstance.get(`/datasets/list`, { params });
    return response.data;
  },

  downloadDataset: async (id, mode = 'master') => {
    const response = await axiosInstance.get(`/datasets/download/${id}`, {
      params: { mode },
      responseType: 'blob',
    });
    return response;
  },

  startZipJob: async (id) => {
    const response = await axiosInstance.post(`/datasets/zip/${id}`);
    return response.data;
  },

  getZipStatus: async (jobId) => {
    const response = await axiosInstance.get(`/datasets/zip/status/${jobId}`);
    return response.data;
  },

  downloadZip: async (jobId) => {
    const response = await axiosInstance.get(`/datasets/zip/download/${jobId}`, {
      responseType: 'blob',
    });
    return response;
  },

  // Crystal snapshots (see qp2/data_proc/server/PYBLUICE_SNAPSHOT_INTEGRATION.md)
  // Returns empty array until pybluice ships the CAMERA-event patch.
  listDatasetSnapshots: async (datasetId) => {
    const response = await axiosInstance.get(`/datasets/${datasetId}/snapshots`);
    return response.data;
  },

  // Fetches a single JPEG as a blob and returns an object URL.
  // Caller is responsible for URL.revokeObjectURL() when the image is no longer shown.
  fetchSnapshotBlobUrl: async (snapshotId) => {
    const response = await axiosInstance.get(`/snapshots/${snapshotId}/image`, {
      responseType: 'blob',
    });
    return URL.createObjectURL(response.data);
  },

  // Processing
  listProcessing: async (params = {}) => {
    const response = await axiosInstance.get(`/processing/list`, { params });
    return response.data;
  },

  downloadProcessingFile: async (id, field) => {
    const response = await axiosInstance.get(`/processing/download/${id}/${field}`, {
      responseType: 'blob',
    });
    return response;
  },

  // Chat
  chatHistory: async (room) => {
    const response = await axiosInstance.get(`/chat/history`, { params: room ? { room } : {} });
    return response.data;
  },

  chatSend: async (content, room) => {
    const response = await axiosInstance.post(`/chat/send${room ? `?room=${room}` : ''}`, { content });
    return response.data;
  },

  chatAskAI: async (content, room) => {
    const response = await axiosInstance.post(`/chat/ask_ai${room ? `?room=${room}` : ''}`, { content });
    return response.data;
  },

  chatUsers: async (room) => {
    const response = await axiosInstance.get(`/chat/users`, { params: room ? { room } : {} });
    return response.data;
  },

  chatInfo: async () => {
    const response = await axiosInstance.get(`/chat/info`);
    return response.data;
  },

  chatRooms: async () => {
    const response = await axiosInstance.get(`/chat/rooms`);
    return response.data;
  },

  chatArchive: async (room) => {
    const url = room ? `/chat/archive?room=${encodeURIComponent(room)}` : `/chat/archive`;
    const response = await axiosInstance.post(url);
    return response.data;
  },

  // Image Viewer
  viewerDatasets: async () => {
    const response = await axiosInstance.get(`/viewer/datasets`);
    return response.data;
  },

  viewerParams: async (datasetId, path = null, masterIndex = 0) => {
    const params = { master_index: masterIndex };
    if (path) params.path = path;
    const response = await axiosInstance.get(`/viewer/params/${datasetId}`, { params });
    return response.data;
  },

  viewerPixel: async (datasetId, path = null, frame, x, y, masterIndex = 0) => {
    const params = { frame, x, y, master_index: masterIndex };
    if (path) params.path = path;
    const response = await axiosInstance.get(`/viewer/pixel/${datasetId}`, { params });
    return response.data;
  },

  viewerRings: async (datasetId, path = null, masterIndex = 0) => {
    const params = { master_index: masterIndex };
    if (path) params.path = path;
    const response = await axiosInstance.get(`/viewer/rings/${datasetId}`, { params });
    return response.data;
  },

  viewerScan: async (path) => {
    const response = await axiosInstance.get(`/viewer/scan`, { params: { path } });
    return response.data;
  },

  viewerBrowse: async (path = '') => {
    const response = await axiosInstance.get(`/viewer/browse`, { params: { path } });
    return response.data;
  },

  // Experiment Preparation
  experimentList: async () => {
    const response = await axiosInstance.get(`/experiment/list`);
    return response.data;
  },

  experimentGet: async (esafId) => {
    const response = await axiosInstance.get(`/experiment/${esafId}`);
    return response.data;
  },

  experimentCreate: async (data) => {
    const response = await axiosInstance.post(`/experiment/create`, data);
    return response.data;
  },

  experimentUpdate: async (esafId, data) => {
    const response = await axiosInstance.put(`/experiment/${esafId}`, data);
    return response.data;
  },

  experimentEsafGroups: async () => {
    const response = await axiosInstance.get(`/experiment/esaf-groups`);
    return response.data;
  },

  experimentStaffList: async () => {
    const response = await axiosInstance.get(`/experiment/staff-list`);
    return response.data;
  },

  experimentMyIP: async () => {
    const response = await axiosInstance.get(`/experiment/my-ip`);
    return response.data;
  },

  experimentUploadFile: async (esafId, file) => {
    const formData = new FormData();
    formData.append('file', file);
    const response = await axiosInstance.post(`/experiment/${esafId}/files`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  experimentDownloadFile: async (esafId, fileId) => {
    const response = await axiosInstance.get(`/experiment/${esafId}/files/${fileId}`, {
      responseType: 'blob',
    });
    return response;
  },

  experimentDeleteFile: async (esafId, fileId) => {
    const response = await axiosInstance.delete(`/experiment/${esafId}/files/${fileId}`);
    return response.data;
  },

  experimentAddIP: async (esafId, data) => {
    const response = await axiosInstance.post(`/experiment/${esafId}/ips`, data);
    return response.data;
  },

  experimentUpdateIP: async (esafId, ipId, data) => {
    const response = await axiosInstance.put(`/experiment/${esafId}/ips/${ipId}`, data);
    return response.data;
  },

  experimentDeleteIP: async (esafId, ipId) => {
    const response = await axiosInstance.delete(`/experiment/${esafId}/ips/${ipId}`);
    return response.data;
  },

  experimentAddTracking: async (esafId, data) => {
    const response = await axiosInstance.post(`/experiment/${esafId}/tracking`, data);
    return response.data;
  },

  experimentDeleteTracking: async (esafId, trackId) => {
    const response = await axiosInstance.delete(`/experiment/${esafId}/tracking/${trackId}`);
    return response.data;
  },

  experimentAddHost: async (esafId, data) => {
    const response = await axiosInstance.post(`/experiment/${esafId}/hosts`, data);
    return response.data;
  },

  experimentDelete: async (esafId) => {
    const response = await axiosInstance.delete(`/experiment/${esafId}`);
    return response.data;
  },

  experimentDeleteHost: async (esafId, hostId) => {
    const response = await axiosInstance.delete(`/experiment/${esafId}/hosts/${hostId}`);
    return response.data;
  },

  experimentSequences: async (esafId) => {
    const response = await axiosInstance.get(`/experiment/${esafId}/sequences`);
    return response.data;
  },

  experimentSequenceFiles: async (esafId) => {
    const response = await axiosInstance.get(`/experiment/${esafId}/sequence-files`);
    return response.data;
  },

  reprocessDatasets: async (payload) => {
    const response = await axiosInstance.post('/processing/reprocess', payload);
    return response.data;
  },

  // --- Structure Models ---

  listModels: async (esafId) => {
    const response = await axiosInstance.get(`/models/${esafId}`);
    return response.data;
  },

  uploadModel: async (esafId, file) => {
    const formData = new FormData();
    formData.append('file', file);
    const response = await axiosInstance.post(`/models/${esafId}/upload`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  downloadModel: (esafId, modelId) => {
    return `${API_URL}/models/${esafId}/${modelId}/download`;
  },

  viewModelUrl: (esafId, modelId) => {
    return `${API_URL}/models/${esafId}/${modelId}/view`;
  },

  deleteModel: async (esafId, modelId) => {
    const response = await axiosInstance.delete(`/models/${esafId}/${modelId}`);
    return response.data;
  },

  // --- Structure Prediction ---

  listPrograms: async () => {
    const response = await axiosInstance.get('/predict/programs');
    return response.data;
  },

  submitPrediction: async (esafId, payload) => {
    const response = await axiosInstance.post(`/predict/${esafId}/submit`, payload);
    return response.data;
  },

  listPredictionJobs: async (esafId) => {
    const response = await axiosInstance.get(`/predict/${esafId}/jobs`);
    return response.data;
  },

  getPredictionJob: async (esafId, jobId) => {
    const response = await axiosInstance.get(`/predict/${esafId}/jobs/${jobId}`);
    return response.data;
  },

  importPredictionModels: async (esafId, jobId) => {
    const response = await axiosInstance.post(`/predict/${esafId}/jobs/${jobId}/import`);
    return response.data;
  },

  listModelsForSpreadsheet: async (esafId) => {
    const response = await axiosInstance.get(`/models/list-for-spreadsheet?esaf_id=${esafId}`);
    return response.data;
  },

  // --- RCSB Reports ---

  rcsbSearch: async (params) => {
    const response = await axiosInstance.post('/rcsb/search', params);
    return response.data;
  },

  rcsbSearchStatus: async (jobId) => {
    const response = await axiosInstance.get(`/rcsb/search/status/${jobId}`);
    return response.data;
  },

  rcsbExport: async (params) => {
    const response = await axiosInstance.post('/rcsb/export', params, {
      responseType: 'blob',
    });
    return response;
  },

  rcsbSyncApsDb: async () => {
    const response = await axiosInstance.post('/rcsb/sync-aps-db');
    return response.data;
  },

  rcsbUploadApsDb: async (file) => {
    const formData = new FormData();
    formData.append('file', file);
    const response = await axiosInstance.post('/rcsb/upload-aps-db', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  rcsbApsDbStatus: async () => {
    const response = await axiosInstance.get('/rcsb/aps-db-status');
    return response.data;
  },

  rcsbGetScheduledRecipients: async () => {
    const response = await axiosInstance.get('/rcsb/scheduled-recipients');
    return response.data;
  },

  rcsbSetScheduledRecipients: async (taskName, emails) => {
    const response = await axiosInstance.put(
      `/rcsb/scheduled-recipients/${taskName}`,
      emails,
      { headers: { 'Content-Type': 'application/json' } }
    );
    return response.data;
  },

  modelInfo: async (id) => {
    const response = await axiosInstance.get(`/processing/${id}/model-info`);
    return response.data;
  },

  // Archive tracker
  archiveListJobs: async (params = {}) => {
    const q = new URLSearchParams(params).toString();
    const res = await axiosInstance.get(`/archive/jobs${q ? '?' + q : ''}`);
    return res.data;
  },
  archiveGetJob: async (id) => {
    const res = await axiosInstance.get(`/archive/jobs/${id}`);
    return res.data;
  },
  archiveStatus: async () => {
    const res = await axiosInstance.get('/archive/status');
    return res.data;
  },
  archiveScan: async (dryRun = false) => {
    const res = await axiosInstance.post(`/archive/scan?dry_run=${dryRun}`);
    return res.data;
  },
  archiveAudit: async (dryRun = false) => {
    const res = await axiosInstance.post(`/archive/audit?dry_run=${dryRun}`);
    return res.data;
  },
  archiveReupload: async (payload) => {
    // payload: { ids: [1,2,3], skip_completed: true, dry_run: false }
    const res = await axiosInstance.post('/archive/reupload', payload);
    return res.data;
  },

  verifySession: async () => {
    const res = await axiosInstance.get('/user/info');
    return res.data;
  },

  // Distribution map
  distributionGenerateMap: async (formData) => {
    const res = await axiosInstance.post('/distribution/map', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return res.data;
  },
};
