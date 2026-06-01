// Globus endpoint configuration
// Override at build time with VITE_GLOBUS_ENDPOINT_ID and VITE_GLOBUS_ENDPOINT_ID_2
const DATA_ENDPOINT = import.meta.env.VITE_GLOBUS_ENDPOINT_ID || "a21925f4-dbb9-4308-a017-09db1c837ac6";
export const GLOBUS_CONFIG = {
    endpoints: [
        {
            id: DATA_ENDPOINT,
            label: "GMCA Data",
            pathPrefix: "/mnt/beegfs"
        }
    ],
    defaultEndpoint: DATA_ENDPOINT
};
