/* ============================
   IMPORT ENGINE (v0.12)
   ============================
   
   Модуль для импорта конфигурации проекта из JSON.
*/

import {
    saveInventory,
    saveFile,
    saveSheet,
    setMatrixCell,
    saveTemplate,
    saveDataset,
    saveMappingProfile,
    saveJob,
    saveScanResult,
    saveCredential,
    getInventory,
    getFile,
    getTemplate,
    getDataset,
    getMappingProfile,
    getJob,
    getCredential
} from '../storage/indexeddb.js';

import { EXPORT_FORMAT_VERSION } from '../export/export-engine.js';

// ============================
// VALIDATION
// ============================

/**
 * Валидация файла импорта
 * @param {Object} data - данные из JSON файла
 * @returns {Object} { valid: boolean, errors: Array<string>, warnings: Array<string> }
 */
export function validateImportFile(data) {
    const errors = [];
    const warnings = [];

    if (!data || typeof data !== 'object') {
        errors.push('Файл не является валидным JSON объектом');
        return { valid: false, errors, warnings };
    }

    // Проверка версии формата
    if (!data.version) {
        warnings.push('Версия формата не указана. Файл может быть несовместим.');
    } else if (data.version !== EXPORT_FORMAT_VERSION) {
        warnings.push(`Версия формата (${data.version}) отличается от текущей (${EXPORT_FORMAT_VERSION}). Возможны проблемы совместимости.`);
    }

    // Проверка структуры
    const expectedKeys = [
        'inventory', 'files', 'sheets', 'templates', 'datasets',
        'mappingProfiles', 'jobs', 'scanResults', 'credentials', 'matrix'
    ];

    const hasAnyData = expectedKeys.some(key => data[key] !== null && data[key] !== undefined);
    if (!hasAnyData) {
        errors.push('Файл не содержит данных для импорта');
    }

    // Проверка структуры inventory
    if (data.inventory && typeof data.inventory === 'object') {
        const inventoryKeys = ['environments', 'hosts', 'services', 'endpoints', 'snapshots'];
        inventoryKeys.forEach(key => {
            if (data.inventory[key] && !Array.isArray(data.inventory[key])) {
                errors.push(`inventory.${key} должен быть массивом`);
            }
        });
    }

    // Проверка массивов
    const arrayKeys = ['files', 'sheets', 'templates', 'datasets', 'mappingProfiles', 'jobs', 'scanResults', 'credentials'];
    arrayKeys.forEach(key => {
        if (data[key] !== null && data[key] !== undefined && !Array.isArray(data[key])) {
            errors.push(`${key} должен быть массивом`);
        }
    });

    // Проверка matrix
    if (data.matrix !== null && data.matrix !== undefined && typeof data.matrix !== 'object') {
        errors.push('matrix должен быть объектом');
    }

    return {
        valid: errors.length === 0,
        errors,
        warnings
    };
}

/**
 * Парсинг файла импорта
 * @param {File} file - файл для импорта
 * @returns {Promise<Object>} распарсенные данные
 */
export async function parseImportFile(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();

        reader.onload = (e) => {
            try {
                const text = e.target.result;
                const data = JSON.parse(text);
                resolve(data);
            } catch (error) {
                reject(new Error(`Ошибка парсинга JSON: ${error.message}`));
            }
        };

        reader.onerror = () => {
            reject(new Error('Ошибка чтения файла'));
        };

        reader.readAsText(file);
    });
}

/**
 * Предпросмотр импорта
 * @param {Object} data - данные для импорта
 * @returns {Object} статистика импорта
 */
export function previewImport(data) {
    const preview = {
        inventory: {
            environments: 0,
            hosts: 0,
            services: 0,
            endpoints: 0,
            snapshots: 0
        },
        files: 0,
        sheets: 0,
        templates: 0,
        datasets: 0,
        mappingProfiles: 0,
        jobs: 0,
        scanResults: 0,
        credentials: 0,
        matrix: 0
    };

    if (data.inventory) {
        preview.inventory.environments = data.inventory.environments?.length || 0;
        preview.inventory.hosts = data.inventory.hosts?.length || 0;
        preview.inventory.services = data.inventory.services?.length || 0;
        preview.inventory.endpoints = data.inventory.endpoints?.length || 0;
        preview.inventory.snapshots = data.inventory.snapshots?.length || 0;
    }

    if (Array.isArray(data.files)) preview.files = data.files.length;
    if (Array.isArray(data.sheets)) preview.sheets = data.sheets.length;
    if (Array.isArray(data.templates)) preview.templates = data.templates.length;
    if (Array.isArray(data.datasets)) preview.datasets = data.datasets.length;
    if (Array.isArray(data.mappingProfiles)) preview.mappingProfiles = data.mappingProfiles.length;
    if (Array.isArray(data.jobs)) preview.jobs = data.jobs.length;
    if (Array.isArray(data.scanResults)) preview.scanResults = data.scanResults.length;
    if (Array.isArray(data.credentials)) preview.credentials = data.credentials.length;
    if (data.matrix && typeof data.matrix === 'object') {
        preview.matrix = Object.keys(data.matrix).length;
    }

    return preview;
}

// ============================
// IMPORT FUNCTIONS
// ============================

/**
 * Импорт данных инвентаря
 * @param {Object} inventory - данные инвентаря
 * @param {string} mode - режим импорта ('replace' | 'merge')
 * @returns {Promise<Object>} отчет об импорте
 */
async function importInventoryData(inventory, mode) {
    const report = {
        imported: { environments: 0, hosts: 0, services: 0, endpoints: 0, snapshots: 0 },
        updated: { environments: 0, hosts: 0, services: 0, endpoints: 0, snapshots: 0 },
        errors: []
    };

    try {
        if (mode === 'replace') {
            // Полная замена
            await saveInventory(inventory);
            report.imported = {
                environments: inventory.environments?.length || 0,
                hosts: inventory.hosts?.length || 0,
                services: inventory.services?.length || 0,
                endpoints: inventory.endpoints?.length || 0,
                snapshots: inventory.snapshots?.length || 0
            };
        } else {
            // Объединение: получаем текущий инвентарь и объединяем
            const current = await getInventory();
            const merged = {
                environments: [...(current.environments || []), ...(inventory.environments || [])],
                hosts: [...(current.hosts || []), ...(inventory.hosts || [])],
                services: [...(current.services || []), ...(inventory.services || [])],
                endpoints: [...(current.endpoints || []), ...(inventory.endpoints || [])],
                snapshots: [...(current.snapshots || []), ...(inventory.snapshots || [])]
            };

            // Удаляем дубликаты по ID
            const deduplicated = {
                environments: removeDuplicatesById(merged.environments),
                hosts: removeDuplicatesById(merged.hosts),
                services: removeDuplicatesById(merged.services),
                endpoints: removeDuplicatesById(merged.endpoints),
                snapshots: removeDuplicatesById(merged.snapshots)
            };

            await saveInventory(deduplicated);

            // Подсчет новых и обновленных
            ['environments', 'hosts', 'services', 'endpoints', 'snapshots'].forEach(entity => {
                const currentIds = new Set((current[entity] || []).map(item => item.id));
                const imported = (inventory[entity] || []).filter(item => !currentIds.has(item.id));
                const updated = (inventory[entity] || []).filter(item => currentIds.has(item.id));
                report.imported[entity] = imported.length;
                report.updated[entity] = updated.length;
            });
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта инвентаря: ${error.message}`);
    }

    return report;
}

/**
 * Импорт Excel данных
 * @param {Array} files - метаданные файлов
 * @param {Array} sheets - данные листов
 * @param {string} mode - режим импорта
 * @returns {Promise<Object>} отчет об импорте
 */
async function importExcelData(files, sheets, mode) {
    const report = {
        imported: { files: 0, sheets: 0 },
        updated: { files: 0, sheets: 0 },
        errors: []
    };

    try {
        // Импорт файлов (метаданные)
        if (Array.isArray(files)) {
            for (const file of files) {
                try {
                    if (mode === 'replace') {
                        await saveFile(file);
                        report.imported.files++;
                    } else {
                        // Проверяем, существует ли файл
                        const existing = await getFile(file.id).catch(() => null);
                        if (existing) {
                            report.updated.files++;
                        } else {
                            await saveFile(file);
                            report.imported.files++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта файла ${file.name}: ${error.message}`);
                }
            }
        }

        // Импорт листов
        if (Array.isArray(sheets)) {
            for (const sheet of sheets) {
                try {
                    await saveSheet(sheet);
                    report.imported.sheets++;
                } catch (error) {
                    report.errors.push(`Ошибка импорта листа ${sheet.name}: ${error.message}`);
                }
            }
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта Excel данных: ${error.message}`);
    }

    return report;
}

/**
 * Импорт данных конфигурации
 * @param {Object} config - данные конфигурации
 * @param {string} mode - режим импорта
 * @returns {Promise<Object>} отчет об импорте
 */
async function importConfigurationData(config, mode) {
    const report = {
        imported: { templates: 0, datasets: 0, mappingProfiles: 0 },
        updated: { templates: 0, datasets: 0, mappingProfiles: 0 },
        errors: []
    };

    try {
        // Импорт шаблонов
        if (Array.isArray(config.templates)) {
            for (const template of config.templates) {
                try {
                    if (mode === 'replace') {
                        await saveTemplate(template);
                        report.imported.templates++;
                    } else {
                        const existing = await getTemplate(template.id).catch(() => null);
                        if (existing) {
                            await saveTemplate(template);
                            report.updated.templates++;
                        } else {
                            await saveTemplate(template);
                            report.imported.templates++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта шаблона ${template.name}: ${error.message}`);
                }
            }
        }

        // Импорт datasets
        if (Array.isArray(config.datasets)) {
            for (const dataset of config.datasets) {
                try {
                    if (mode === 'replace') {
                        await saveDataset(dataset);
                        report.imported.datasets++;
                    } else {
                        const existing = await getDataset(dataset.id).catch(() => null);
                        if (existing) {
                            await saveDataset(dataset);
                            report.updated.datasets++;
                        } else {
                            await saveDataset(dataset);
                            report.imported.datasets++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта dataset ${dataset.name}: ${error.message}`);
                }
            }
        }

        // Импорт mapping profiles
        if (Array.isArray(config.mappingProfiles)) {
            for (const profile of config.mappingProfiles) {
                try {
                    if (mode === 'replace') {
                        await saveMappingProfile(profile);
                        report.imported.mappingProfiles++;
                    } else {
                        const existing = await getMappingProfile(profile.id).catch(() => null);
                        if (existing) {
                            await saveMappingProfile(profile);
                            report.updated.mappingProfiles++;
                        } else {
                            await saveMappingProfile(profile);
                            report.imported.mappingProfiles++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта профиля ${profile.name}: ${error.message}`);
                }
            }
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта конфигурации: ${error.message}`);
    }

    return report;
}

/**
 * Импорт Jobs и ScanResults
 * @param {Array} jobs - Jobs
 * @param {Array} scanResults - ScanResults
 * @param {string} mode - режим импорта
 * @returns {Promise<Object>} отчет об импорте
 */
async function importJobsData(jobs, scanResults, mode) {
    const report = {
        imported: { jobs: 0, scanResults: 0 },
        updated: { jobs: 0, scanResults: 0 },
        errors: []
    };

    try {
        // Импорт Jobs
        if (Array.isArray(jobs)) {
            for (const job of jobs) {
                try {
                    if (mode === 'replace') {
                        await saveJob(job);
                        report.imported.jobs++;
                    } else {
                        const existing = await getJob(job.id).catch(() => null);
                        if (existing) {
                            await saveJob(job);
                            report.updated.jobs++;
                        } else {
                            await saveJob(job);
                            report.imported.jobs++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта job ${job.name}: ${error.message}`);
                }
            }
        }

        // Импорт ScanResults
        if (Array.isArray(scanResults)) {
            for (const result of scanResults) {
                try {
                    await saveScanResult(result);
                    report.imported.scanResults++;
                } catch (error) {
                    report.errors.push(`Ошибка импорта scan result ${result.id}: ${error.message}`);
                }
            }
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта Jobs: ${error.message}`);
    }

    return report;
}

/**
 * Импорт креденшалов
 * @param {Array} credentials - креденшалы
 * @param {string} mode - режим импорта
 * @returns {Promise<Object>} отчет об импорте
 */
async function importCredentialsData(credentials, mode) {
    const report = {
        imported: 0,
        updated: 0,
        errors: [],
        warnings: []
    };

    try {
        if (Array.isArray(credentials)) {
            for (const credential of credentials) {
                try {
                    // Предупреждение: креденшалы могут быть недоступны, если ключ шифрования другой
                    if (credential.password && credential.password.startsWith('encrypted:')) {
                        report.warnings.push(`Креденшал "${credential.name}" зашифрован. Может быть недоступен, если ключ шифрования отличается.`);
                    }

                    if (mode === 'replace') {
                        await saveCredential(credential);
                        report.imported++;
                    } else {
                        const existing = await getCredential(credential.id).catch(() => null);
                        if (existing) {
                            await saveCredential(credential);
                            report.updated++;
                        } else {
                            await saveCredential(credential);
                            report.imported++;
                        }
                    }
                } catch (error) {
                    report.errors.push(`Ошибка импорта креденшала ${credential.name}: ${error.message}`);
                }
            }
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта креденшалов: ${error.message}`);
    }

    return report;
}

/**
 * Импорт Version Matrix
 * @param {Object} matrix - данные матрицы
 * @param {string} mode - режим импорта
 * @returns {Promise<Object>} отчет об импорте
 */
async function importMatrixData(matrix, mode) {
    const report = {
        imported: 0,
        errors: []
    };

    try {
        if (matrix && typeof matrix === 'object') {
            const cells = Object.keys(matrix);
            for (const cellKey of cells) {
                try {
                    const value = matrix[cellKey];
                    await setMatrixCell(cellKey, value);
                    report.imported++;
                } catch (error) {
                    report.errors.push(`Ошибка импорта ячейки матрицы ${cellKey}: ${error.message}`);
                }
            }
        }
    } catch (error) {
        report.errors.push(`Ошибка импорта матрицы: ${error.message}`);
    }

    return report;
}

/**
 * Удаление дубликатов по ID
 * @param {Array} items - массив элементов
 * @returns {Array} массив без дубликатов
 */
function removeDuplicatesById(items) {
    const seen = new Set();
    return items.filter(item => {
        if (seen.has(item.id)) {
            return false;
        }
        seen.add(item.id);
        return true;
    });
}

/**
 * Импорт конфигурации проекта
 * @param {Object} data - данные для импорта
 * @param {string} mode - режим импорта ('replace' | 'merge')
 * @returns {Promise<Object>} отчет об импорте
 */
export async function importConfiguration(data, mode = 'merge') {
    const report = {
        inventory: null,
        excel: null,
        configuration: null,
        jobs: null,
        credentials: null,
        matrix: null,
        errors: [],
        warnings: []
    };

    try {
        // Импорт инвентаря
        if (data.inventory) {
            report.inventory = await importInventoryData(data.inventory, mode);
            report.errors.push(...report.inventory.errors);
        }

        // Импорт Excel данных
        if (data.files || data.sheets) {
            report.excel = await importExcelData(data.files || [], data.sheets || [], mode);
            report.errors.push(...report.excel.errors);
        }

        // Импорт конфигурации
        if (data.templates || data.datasets || data.mappingProfiles) {
            report.configuration = await importConfigurationData({
                templates: data.templates || [],
                datasets: data.datasets || [],
                mappingProfiles: data.mappingProfiles || []
            }, mode);
            report.errors.push(...report.configuration.errors);
        }

        // Импорт Jobs
        if (data.jobs || data.scanResults) {
            report.jobs = await importJobsData(data.jobs || [], data.scanResults || [], mode);
            report.errors.push(...report.jobs.errors);
        }

        // Импорт креденшалов
        if (data.credentials) {
            report.credentials = await importCredentialsData(data.credentials, mode);
            report.errors.push(...report.credentials.errors);
            report.warnings.push(...report.credentials.warnings);
        }

        // Импорт матрицы
        if (data.matrix) {
            report.matrix = await importMatrixData(data.matrix, mode);
            report.errors.push(...report.matrix.errors);
        }
    } catch (error) {
        report.errors.push(`Критическая ошибка импорта: ${error.message}`);
    }

    return report;
}

