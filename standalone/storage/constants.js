/* ============================
   STORAGE CONSTANTS
   ============================ */

// Database configuration
export const DB_NAME = 'versionInventoryDB';
export const DB_VERSION = 5; // Increment for Mapping Profiles support

// Store names
export const STORES = {
    INVENTORY: 'inventory',
    FILES: 'files',
    SHEETS: 'sheets',
    MATRIX: 'matrix',
    TEMPLATES: 'templates',
    DATASETS: 'datasets', // Added for Block B
    JOBS: 'jobs', // Added for v0.9
    SCAN_RESULTS: 'scanResults', // Added for v0.9
    MAPPING_PROFILES: 'mappingProfiles' // Added for v0.10
};

// Entity types (for inventory store)
export const ENTITIES = {
    ENVIRONMENTS: 'environments',
    HOSTS: 'hosts',
    SERVICES: 'services',
    ENDPOINTS: 'endpoints',
    SNAPSHOTS: 'snapshots'
};

// Legacy localStorage keys (for migration)
export const LEGACY_KEYS = {
    INVENTORY: 'versionInventory',
    MATRIX_PREFIX: 'matrix_'
};

// Migration flag
export const MIGRATION_FLAG = 'migration_completed_v1';

