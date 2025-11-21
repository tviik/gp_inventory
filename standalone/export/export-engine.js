/* ============================
   EXPORT ENGINE (v0.12)
   ============================
   
   Модуль для экспорта конфигурации проекта в JSON.
*/

import {
    getInventory,
    listFiles,
    listSheets,
    getAllMatrix,
    listTemplates,
    listDatasets,
    listMappingProfiles,
    listJobs,
    listScanResults,
    listCredentials
} from '../storage/indexeddb.js';

// ============================
// CONSTANTS
// ============================

export const EXPORT_FORMAT_VERSION = '1.0';
export const EXPORT_APP_NAME = 'Version Inventory';

// ============================
// EXPORT FUNCTIONS
// ============================

/**
 * Сбор данных инвентаря
 * @returns {Promise<Object>}
 */
async function collectInventoryData() {
    try {
        const inventory = await getInventory();
        return {
            environments: inventory.environments || [],
            hosts: inventory.hosts || [],
            services: inventory.services || [],
            endpoints: inventory.endpoints || [],
            snapshots: inventory.snapshots || []
        };
    } catch (error) {
        console.error('[Export] Error collecting inventory:', error);
        return {
            environments: [],
            hosts: [],
            services: [],
            endpoints: [],
            snapshots: []
        };
    }
}

/**
 * Сбор данных Excel (файлы и листы)
 * @returns {Promise<Object>}
 */
async function collectExcelData() {
    try {
        const files = await listFiles();
        const sheets = await listSheets();
        
        // Не экспортируем бинарные данные файлов (слишком большие)
        // Экспортируем только метаданные
        const filesMetadata = files.map(file => ({
            id: file.id,
            name: file.name,
            size: file.size,
            type: file.type,
            uploadedAt: file.uploadedAt
        }));
        
        return {
            files: filesMetadata,
            sheets: sheets || []
        };
    } catch (error) {
        console.error('[Export] Error collecting Excel data:', error);
        return {
            files: [],
            sheets: []
        };
    }
}

/**
 * Сбор данных конфигурации (шаблоны, datasets, профили)
 * @returns {Promise<Object>}
 */
async function collectConfigurationData() {
    try {
        const [templates, datasets, mappingProfiles] = await Promise.all([
            listTemplates(),
            listDatasets(),
            listMappingProfiles()
        ]);
        
        return {
            templates: templates || [],
            datasets: datasets || [],
            mappingProfiles: mappingProfiles || []
        };
    } catch (error) {
        console.error('[Export] Error collecting configuration data:', error);
        return {
            templates: [],
            datasets: [],
            mappingProfiles: []
        };
    }
}

/**
 * Сбор данных Jobs и ScanResults
 * @returns {Promise<Object>}
 */
async function collectJobsData() {
    try {
        const [jobs, scanResults] = await Promise.all([
            listJobs(),
            listScanResults()
        ]);
        
        return {
            jobs: jobs || [],
            scanResults: scanResults || []
        };
    } catch (error) {
        console.error('[Export] Error collecting jobs data:', error);
        return {
            jobs: [],
            scanResults: []
        };
    }
}

/**
 * Сбор данных креденшалов
 * @returns {Promise<Array>}
 */
async function collectCredentialsData() {
    try {
        const credentials = await listCredentials();
        // Креденшалы экспортируются в зашифрованном виде (безопасность)
        return credentials || [];
    } catch (error) {
        console.error('[Export] Error collecting credentials:', error);
        return [];
    }
}

/**
 * Сбор данных Version Matrix
 * @returns {Promise<Object>}
 */
async function collectMatrixData() {
    try {
        const matrix = await getAllMatrix();
        return matrix || {};
    } catch (error) {
        console.error('[Export] Error collecting matrix data:', error);
        return {};
    }
}

/**
 * Форматирование данных экспорта
 * @param {Object} data - собранные данные
 * @param {Object} options - опции экспорта
 * @returns {Object} отформатированные данные
 */
function formatExportData(data, options) {
    const exportData = {
        version: EXPORT_FORMAT_VERSION,
        exportedAt: new Date().toISOString(),
        exportedBy: `${EXPORT_APP_NAME} v0.12`,
        inventory: null,
        files: null,
        sheets: null,
        templates: null,
        datasets: null,
        mappingProfiles: null,
        jobs: null,
        scanResults: null,
        credentials: null,
        matrix: null
    };
    
    if (options.includeInventory) {
        exportData.inventory = data.inventory;
    }
    
    if (options.includeExcel) {
        exportData.files = data.excel.files;
        exportData.sheets = data.excel.sheets;
    }
    
    if (options.includeTemplates) {
        exportData.templates = data.configuration.templates;
    }
    
    if (options.includeDatasets) {
        exportData.datasets = data.configuration.datasets;
    }
    
    if (options.includeMappingProfiles) {
        exportData.mappingProfiles = data.configuration.mappingProfiles;
    }
    
    if (options.includeJobs) {
        exportData.jobs = data.jobs.jobs;
    }
    
    if (options.includeScanResults) {
        exportData.scanResults = data.jobs.scanResults;
    }
    
    if (options.includeCredentials) {
        exportData.credentials = data.credentials;
    }
    
    if (options.includeMatrix) {
        exportData.matrix = data.matrix;
    }
    
    return exportData;
}

/**
 * Экспорт конфигурации проекта
 * @param {Object} options - опции экспорта
 * @returns {Promise<Object>} данные для экспорта
 */
export async function exportConfiguration(options = {}) {
    // Опции по умолчанию
    const defaultOptions = {
        includeInventory: true,
        includeExcel: true,
        includeTemplates: true,
        includeDatasets: true,
        includeMappingProfiles: true,
        includeJobs: true,
        includeScanResults: true,
        includeCredentials: false, // По умолчанию не включаем (безопасность)
        includeMatrix: true
    };
    
    const exportOptions = { ...defaultOptions, ...options };
    
    try {
        // Собираем данные параллельно
        const [inventory, excel, configuration, jobs, credentials, matrix] = await Promise.all([
            exportOptions.includeInventory ? collectInventoryData() : Promise.resolve(null),
            exportOptions.includeExcel ? collectExcelData() : Promise.resolve(null),
            (exportOptions.includeTemplates || exportOptions.includeDatasets || exportOptions.includeMappingProfiles)
                ? collectConfigurationData() : Promise.resolve(null),
            (exportOptions.includeJobs || exportOptions.includeScanResults) ? collectJobsData() : Promise.resolve(null),
            exportOptions.includeCredentials ? collectCredentialsData() : Promise.resolve(null),
            exportOptions.includeMatrix ? collectMatrixData() : Promise.resolve(null)
        ]);
        
        const collectedData = {
            inventory: inventory || { environments: [], hosts: [], services: [], endpoints: [], snapshots: [] },
            excel: excel || { files: [], sheets: [] },
            configuration: configuration || { templates: [], datasets: [], mappingProfiles: [] },
            jobs: jobs || { jobs: [], scanResults: [] },
            credentials: credentials || [],
            matrix: matrix || {}
        };
        
        // Форматируем данные
        const exportData = formatExportData(collectedData, exportOptions);
        
        return exportData;
    } catch (error) {
        console.error('[Export] Error exporting configuration:', error);
        throw new Error(`Ошибка экспорта: ${error.message}`);
    }
}

/**
 * Генерация имени файла для экспорта
 * @returns {string}
 */
export function generateExportFileName() {
    const date = new Date();
    const dateStr = date.toISOString().split('T')[0]; // YYYY-MM-DD
    return `version-inventory-config-${dateStr}.json`;
}

