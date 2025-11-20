/* ============================
   STORAGE CONSTANTS
   ============================ */

// Database configuration
export const DB_NAME = 'versionInventoryDB';
export const DB_VERSION = 2;

// Store names
export const STORES = {
    INVENTORY: 'inventory',
    FILES: 'files',
    SHEETS: 'sheets',
    MATRIX: 'matrix',
    TEMPLATES: 'templates'
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

