/* ============================
   SEARCH ENGINE
   ============================
   
   Интерфейс для работы с SearchWorker.
   Управляет индексацией и поиском по данным Excel.
*/

// ============================
// STATE
// ============================

let worker = null;
let workerReady = false;
let pendingMessages = [];
let messageIdCounter = 0;

// ============================
// WORKER MANAGEMENT
// ============================

/**
 * Инициализация SearchWorker
 */
export async function init() {
    if (worker) {
        return { success: true, ready: workerReady };
    }

    return new Promise((resolve) => {
        try {
            worker = new Worker('workers/search-worker.js', { type: 'module' });

            worker.onmessage = (e) => {
                const { type, payload } = e.data;

                if (type === 'WORKER_READY') {
                    workerReady = true;
                    resolve({ success: true, ready: true });
                } else {
                    // Обработка других сообщений через промисы
                    handleWorkerMessage(e.data);
                }
            };

            worker.onerror = (error) => {
                console.error('[SearchEngine] Worker error:', error);
                workerReady = false;
                resolve({ success: false, error: error.message });
            };
        } catch (error) {
            console.error('[SearchEngine] Failed to create worker:', error);
            resolve({ success: false, error: error.message });
        }
    });
}

/**
 * Обработка сообщений от Worker
 */
const pendingPromises = new Map();

function handleWorkerMessage(data) {
    const { type, payload, messageId } = data;

    if (messageId && pendingPromises.has(messageId)) {
        const { resolve, reject } = pendingPromises.get(messageId);
        pendingPromises.delete(messageId);

        if (type === 'ERROR') {
            reject(new Error(payload.message || 'Worker error'));
        } else {
            resolve(payload);
        }
    }
}

/**
 * Отправка сообщения в Worker с ожиданием ответа
 */
function sendMessage(type, payload) {
    return new Promise((resolve, reject) => {
        if (!worker || !workerReady) {
            reject(new Error('Worker not initialized or not ready'));
            return;
        }

        const messageId = ++messageIdCounter;
        pendingPromises.set(messageId, { resolve, reject });

        worker.postMessage({
            type,
            payload,
            messageId
        });

        // Таймаут на случай зависания
        setTimeout(() => {
            if (pendingPromises.has(messageId)) {
                pendingPromises.delete(messageId);
                reject(new Error('Worker timeout'));
            }
        }, 30000); // 30 секунд
    });
}

// ============================
// INDEXING
// ============================

/**
 * Индексация листа
 * @param {string} fileId - ID файла
 * @param {string} fileName - имя файла
 * @param {string} sheetName - имя листа
 * @param {Object} sheetData - данные листа { columns, rows }
 */
export async function indexSheet(fileId, fileName, sheetName, sheetData) {
    try {
        await sendMessage('INDEX_SHEET', {
            fileId,
            fileName,
            sheetName,
            sheetData
        });
        return { success: true };
    } catch (error) {
        console.error('[SearchEngine] Error indexing sheet:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Индексация всех листов из IndexedDB
 * @param {Function} getSheet - функция для получения листа из IndexedDB
 * @param {Function} listFiles - функция для получения списка файлов
 * @param {Function} listSheets - функция для получения списка листов
 */
export async function indexAllSheets(getSheet, listFiles, listSheets) {
    try {
        const files = await listFiles();
        let indexed = 0;
        let errors = 0;

        for (const file of files) {
            try {
                const sheetNames = await listSheets(file.id);
                
                for (const sheetName of sheetNames) {
                    try {
                        const sheetData = await getSheet(file.id, sheetName);
                        if (sheetData) {
                            await indexSheet(file.id, file.name, sheetName, sheetData);
                            indexed++;
                        }
                    } catch (error) {
                        console.error(`[SearchEngine] Error indexing sheet ${sheetName}:`, error);
                        errors++;
                    }
                }
            } catch (error) {
                console.error(`[SearchEngine] Error processing file ${file.id}:`, error);
                errors++;
            }
        }

        return {
            success: true,
            indexed,
            errors,
            total: indexed + errors
        };
    } catch (error) {
        console.error('[SearchEngine] Error indexing all sheets:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Удаление листа из индекса
 * @param {string} fileId - ID файла
 * @param {string} sheetName - имя листа
 */
export async function removeSheet(fileId, sheetName) {
    try {
        await sendMessage('REMOVE_SHEET', { fileId, sheetName });
        return { success: true };
    } catch (error) {
        console.error('[SearchEngine] Error removing sheet:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Очистка всего индекса
 */
export async function clearIndex() {
    try {
        await sendMessage('CLEAR_INDEX', {});
        return { success: true };
    } catch (error) {
        console.error('[SearchEngine] Error clearing index:', error);
        return { success: false, error: error.message };
    }
}

// ============================
// SEARCH
// ============================

/**
 * Поиск по индексу
 * @param {string} query - поисковый запрос
 * @param {Object} options - опции поиска { fileId?, sheetName?, columnName?, limit? }
 * @returns {Promise<Object>} результаты поиска
 */
export async function search(query, options = {}) {
    try {
        const results = await sendMessage('SEARCH', { query, options });
        return results;
    } catch (error) {
        console.error('[SearchEngine] Error searching:', error);
        return {
            query,
            totalMatches: 0,
            results: [],
            searchTime: 0,
            error: error.message
        };
    }
}

// ============================
// STATISTICS
// ============================

/**
 * Получение статистики индекса
 */
export async function getIndexStats() {
    try {
        const stats = await sendMessage('GET_STATS', {});
        return stats;
    } catch (error) {
        console.error('[SearchEngine] Error getting stats:', error);
        return null;
    }
}

// ============================
// UTILITIES
// ============================

/**
 * Проверка готовности Worker
 */
export function isReady() {
    return workerReady && worker !== null;
}

/**
 * Остановка Worker
 */
export function terminate() {
    if (worker) {
        worker.terminate();
        worker = null;
        workerReady = false;
        pendingPromises.clear();
    }
}

// ============================
// PUBLIC API
// ============================

export const searchEngine = {
    init,
    isReady,
    terminate,
    indexSheet,
    indexAllSheets,
    removeSheet,
    clearIndex,
    search,
    getIndexStats
};

export default searchEngine;

