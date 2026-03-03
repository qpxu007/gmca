
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'; // Adjust if hosted elsewhere

// Create axios instance with interceptor
const axiosInstance = axios.create({
    baseURL: API_URL
});

axiosInstance.interceptors.request.use(
    (config) => {
        const token = localStorage.getItem('token');
        if (token) {
            config.headers.Authorization = `Bearer ${token}`;
        }
        return config;
    },
    (error) => Promise.reject(error)
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
  }
};
