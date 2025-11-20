/* ============================
   INDEXEDDB STORAGE ENGINE
   ============================
   
   Основной модуль для работы с IndexedDB.
   Обеспечивает хранение инвентаря, файлов, листов и матрицы версий.
*/

import { DB_NAME, DB_VERSION, STORES, ENTITIES, MIGRATION_FLAG, LEGACY_KEYS } from './constants.js';

// ============================
// PRIVATE STATE
// ============================

let db = null;
let initPromise = null;
let useFallback = false; // флаг для fallback на localStorage

// ============================
// ERROR HANDLING
// ============================

class StorageError extends Error {
    constructor(message, code = 'STORAGE_ERROR') {
        super(message);
        this.name = 'StorageError';
        this.code = code;
    }
}

function handleError(error, context) {
    console.error(`[IndexedDB] Error in ${context}:`, error);

    // Если IndexedDB недоступен, переключаемся на fallback
    if (error.name === 'NotSupportedError' ||
        error.name === 'SecurityError' ||
        (error.name === 'DOMException' && error.code === 18)) {
        console.warn('[IndexedDB] IndexedDB not available, using localStorage fallback');
        useFallback = true;
        return new StorageError('IndexedDB not available', 'NOT_SUPPORTED');
    }

    return new StorageError(error.message, 'UNKNOWN_ERROR');
}

// ============================
// DATABASE INITIALIZATION
// ============================

/**
 * Инициализация IndexedDB базы данных
 * Создает stores и индексы согласно схеме
 */
function initDatabase() {
    return new Promise((resolve, reject) => {
        if (!window.indexedDB) {
            const error = new StorageError('IndexedDB is not supported in this browser', 'NOT_SUPPORTED');
            reject(error);
            return;
        }

        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onerror = () => {
            reject(handleError(request.error, 'initDatabase'));
        };

        request.onsuccess = () => {
            db = request.result;

            // Обработка закрытия БД
            db.onclose = () => {
                console.warn('[IndexedDB] Database connection closed');
                db = null;
            };

            // Обработка ошибок БД
            db.onerror = (event) => {
                console.error('[IndexedDB] Database error:', event.target.error);
            };

            console.log('[IndexedDB] Database opened successfully');
            resolve(db);
        };

        request.onupgradeneeded = (event) => {
            const database = event.target.result;
            const transaction = event.target.transaction;

            // Store: inventory
            if (!database.objectStoreNames.contains(STORES.INVENTORY)) {
                const inventoryStore = database.createObjectStore(STORES.INVENTORY, {
                    keyPath: 'id'
                });
                inventoryStore.createIndex('entity', 'entity', { unique: false });
                console.log('[IndexedDB] Created store: inventory');
            }

            // Store: files
            if (!database.objectStoreNames.contains(STORES.FILES)) {
                const filesStore = database.createObjectStore(STORES.FILES, {
                    keyPath: 'id'
                });
                filesStore.createIndex('name', 'name', { unique: false });
                filesStore.createIndex('uploadedAt', 'uploadedAt', { unique: false });
                console.log('[IndexedDB] Created store: files');
            }

            // Store: sheets
            if (!database.objectStoreNames.contains(STORES.SHEETS)) {
                const sheetsStore = database.createObjectStore(STORES.SHEETS, {
                    keyPath: 'id'
                });
                sheetsStore.createIndex('fileId', 'fileId', { unique: false });
                sheetsStore.createIndex('name', 'name', { unique: false });
                console.log('[IndexedDB] Created store: sheets');
            }

            // Store: matrix
            if (!database.objectStoreNames.contains(STORES.MATRIX)) {
                database.createObjectStore(STORES.MATRIX, {
                    keyPath: 'id'
                });
                console.log('[IndexedDB] Created store: matrix');
            }

            // Store: templates
            if (!database.objectStoreNames.contains(STORES.TEMPLATES)) {
                const templatesStore = database.createObjectStore(STORES.TEMPLATES, {
                    keyPath: 'id'
                });
                templatesStore.createIndex('type', 'type', { unique: false });
                templatesStore.createIndex('category', 'category', { unique: false });
                console.log('[IndexedDB] Created store: templates');
            }
        };
    });
}

/**
 * Инициализация storage модуля
 * Открывает БД и проверяет доступность
 */
export async function init() {
    if (initPromise) {
        return initPromise;
    }

    initPromise = (async () => {
        try {
            await initDatabase();
            useFallback = false;
            return { success: true, fallback: false };
        } catch (error) {
            console.warn('[IndexedDB] Initialization failed, will use localStorage fallback');
            useFallback = true;
            return { success: false, fallback: true, error: error.message };
        }
    })();

    return initPromise;
}

/**
 * Проверка доступности IndexedDB
 */
export function isAvailable() {
    return !useFallback && db !== null && window.indexedDB !== undefined;
}

/**
 * Получение текущего экземпляра БД
 * @throws {StorageError} если БД не инициализирована
 */
export function getDB() {
    if (useFallback) {
        throw new StorageError('IndexedDB not available, use localStorage fallback', 'NOT_AVAILABLE');
    }
    if (!db) {
        throw new StorageError('Database not initialized. Call init() first.', 'NOT_INITIALIZED');
    }
    return db;
}

/**
 * Создание транзакции
 */
function createTransaction(storeNames, mode = 'readonly') {
    const database = getDB();
    const names = Array.isArray(storeNames) ? storeNames : [storeNames];
    return database.transaction(names, mode);
}

/**
 * Получение object store
 */
function getStore(storeName, mode = 'readonly') {
    const transaction = createTransaction(storeName, mode);
    return transaction.objectStore(storeName);
}

// ============================
// UTILITY FUNCTIONS
// ============================

/**
 * Генерация ID для записи
 */
export function generateId(prefix = '') {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 9);
    return prefix ? `${prefix}_${timestamp}_${random}` : `${timestamp}_${random}`;
}

/**
 * Создание составного ключа для инвентаря
 */
export function createInventoryKey(entity, itemId) {
    return `${entity}::${itemId}`;
}

/**
 * Парсинг составного ключа инвентаря
 */
export function parseInventoryKey(key) {
    const parts = key.split('::');
    if (parts.length !== 2) {
        throw new StorageError(`Invalid inventory key format: ${key}`, 'INVALID_KEY');
    }
    return { entity: parts[0], id: parts[1] };
}

// ============================
// INVENTORY OPERATIONS
// ============================

/**
 * Получение всех элементов инвентаря для указанной сущности
 * @param {string} entity - тип сущности (environments, hosts, etc.)
 * @returns {Promise<Array>} массив элементов
 */
export async function getInventory(entity) {
    if (!isAvailable()) {
        // Fallback на localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        if (!raw) return [];
        try {
            const parsed = JSON.parse(raw);
            return Array.isArray(parsed[entity]) ? parsed[entity] : [];
        } catch (e) {
            console.error('[IndexedDB] Fallback localStorage parse error:', e);
            return [];
        }
    }

    const store = getStore(STORES.INVENTORY, 'readonly');
    const index = store.index('entity');

    return new Promise((resolve, reject) => {
        const request = index.getAll(entity);
        request.onsuccess = () => {
            const items = request.result.map(record => record.data);
            resolve(items);
        };
        request.onerror = () => {
            reject(handleError(request.error, 'getInventory'));
        };
    });
}

/**
 * Сохранение всех элементов инвентаря для указанной сущности
 * @param {string} entity - тип сущности
 * @param {Array} items - массив элементов для сохранения
 * @returns {Promise<void>}
 */
export async function saveInventory(entity, items) {
    if (!isAvailable()) {
        // Fallback на localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        let data = {};
        if (raw) {
            try {
                data = JSON.parse(raw);
            } catch (e) {
                console.error('[IndexedDB] Fallback localStorage parse error:', e);
            }
        }
        data[entity] = items;
        localStorage.setItem(LEGACY_KEYS.INVENTORY, JSON.stringify(data));
        return;
    }

    const transaction = createTransaction(STORES.INVENTORY, 'readwrite');
    const store = transaction.objectStore(STORES.INVENTORY);
    const index = store.index('entity');

    // Удаляем старые записи для этой сущности
    return new Promise((resolve, reject) => {
        const deleteRequest = index.openKeyCursor(IDBKeyRange.only(entity));
        const keysToDelete = [];

        deleteRequest.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                keysToDelete.push(cursor.primaryKey);
                cursor.continue();
            } else {
                // Удаляем старые записи
                const deletePromises = keysToDelete.map(key => {
                    return new Promise((res, rej) => {
                        const delReq = store.delete(key);
                        delReq.onsuccess = () => res();
                        delReq.onerror = () => rej(delReq.error);
                    });
                });

                Promise.all(deletePromises).then(() => {
                    // Добавляем новые записи
                    const addPromises = items.map(item => {
                        if (!item || !item.id) {
                            console.warn(`[IndexedDB] Skipping invalid item in ${entity}:`, item);
                            return Promise.resolve();
                        }
                        const key = createInventoryKey(entity, item.id);
                        const record = {
                            id: key,
                            entity: entity,
                            data: item
                        };
                        return new Promise((res, rej) => {
                            const addReq = store.put(record);
                            addReq.onsuccess = () => res();
                            addReq.onerror = () => rej(addReq.error);
                        });
                    });

                    Promise.all(addPromises)
                        .then(() => resolve())
                        .catch(err => reject(handleError(err, 'saveInventory')));
                }).catch(err => reject(handleError(err, 'saveInventory')));
            }
        };

        deleteRequest.onerror = () => {
            reject(handleError(deleteRequest.error, 'saveInventory'));
        };
    });
}

/**
 * Добавление одного элемента в инвентарь
 * @param {string} entity - тип сущности
 * @param {Object} item - элемент для добавления
 * @returns {Promise<void>}
 */
export async function addInventoryItem(entity, item) {
    if (!item || !item.id) {
        throw new StorageError('Item must have an id', 'INVALID_ITEM');
    }

    if (!isAvailable()) {
        // Fallback на localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        let data = {};
        if (raw) {
            try {
                data = JSON.parse(raw);
            } catch (e) {
                console.error('[IndexedDB] Fallback localStorage parse error:', e);
            }
        }
        if (!Array.isArray(data[entity])) {
            data[entity] = [];
        }
        data[entity].push(item);
        localStorage.setItem(LEGACY_KEYS.INVENTORY, JSON.stringify(data));
        return;
    }

    const store = getStore(STORES.INVENTORY, 'readwrite');
    const key = createInventoryKey(entity, item.id);
    const record = {
        id: key,
        entity: entity,
        data: item
    };

    return new Promise((resolve, reject) => {
        const request = store.put(record);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(handleError(request.error, 'addInventoryItem'));
    });
}

/**
 * Обновление элемента инвентаря
 * @param {string} entity - тип сущности
 * @param {string} id - ID элемента
 * @param {Object} updates - объект с обновлениями
 * @returns {Promise<void>}
 */
export async function updateInventoryItem(entity, id, updates) {
    if (!isAvailable()) {
        // Fallback на localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        if (!raw) return;
        try {
            const data = JSON.parse(raw);
            if (Array.isArray(data[entity])) {
                const index = data[entity].findIndex(item => item.id === id);
                if (index !== -1) {
                    data[entity][index] = { ...data[entity][index], ...updates };
                    localStorage.setItem(LEGACY_KEYS.INVENTORY, JSON.stringify(data));
                }
            }
        } catch (e) {
            console.error('[IndexedDB] Fallback localStorage parse error:', e);
        }
        return;
    }

    const store = getStore(STORES.INVENTORY, 'readwrite');
    const key = createInventoryKey(entity, id);

    return new Promise((resolve, reject) => {
        const getRequest = store.get(key);
        getRequest.onsuccess = () => {
            const record = getRequest.result;
            if (!record) {
                reject(new StorageError(`Item ${id} not found in ${entity}`, 'NOT_FOUND'));
                return;
            }

            record.data = { ...record.data, ...updates };
            const putRequest = store.put(record);
            putRequest.onsuccess = () => resolve();
            putRequest.onerror = () => reject(handleError(putRequest.error, 'updateInventoryItem'));
        };
        getRequest.onerror = () => {
            reject(handleError(getRequest.error, 'updateInventoryItem'));
        };
    });
}

/**
 * Удаление элемента из инвентаря
 * @param {string} entity - тип сущности
 * @param {string} id - ID элемента
 * @returns {Promise<void>}
 */
export async function deleteInventoryItem(entity, id) {
    if (!isAvailable()) {
        // Fallback на localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        if (!raw) return;
        try {
            const data = JSON.parse(raw);
            if (Array.isArray(data[entity])) {
                data[entity] = data[entity].filter(item => item.id !== id);
                localStorage.setItem(LEGACY_KEYS.INVENTORY, JSON.stringify(data));
            }
        } catch (e) {
            console.error('[IndexedDB] Fallback localStorage parse error:', e);
        }
        return;
    }

    const store = getStore(STORES.INVENTORY, 'readwrite');
    const key = createInventoryKey(entity, id);

    return new Promise((resolve, reject) => {
        const request = store.delete(key);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(handleError(request.error, 'deleteInventoryItem'));
    });
}

// ============================
// FILES OPERATIONS
// ============================

/**
 * Сохранение метаданных файла
 * @param {Object} fileData - метаданные файла { name, type, size, uploadedAt, metadata }
 * @param {string} fileId - ID файла (если не указан, будет сгенерирован)
 * @returns {Promise<string>} ID сохраненного файла
 */
export async function saveFile(fileData, fileId = null) {
    if (!isAvailable()) {
        console.warn('[IndexedDB] Files storage not available, using fallback');
        // Fallback: сохраняем только метаданные в localStorage
        const files = JSON.parse(localStorage.getItem('excel_files') || '[]');
        const id = fileId || generateId('file');
        files.push({ id, ...fileData });
        localStorage.setItem('excel_files', JSON.stringify(files));
        return id;
    }

    const store = getStore(STORES.FILES, 'readwrite');
    const id = fileId || generateId('file');
    const record = {
        id: id,
        name: fileData.name || '',
        type: fileData.type || 'xlsx',
        size: fileData.size || 0,
        uploadedAt: fileData.uploadedAt || new Date().toISOString(),
        metadata: fileData.metadata || {}
    };

    return new Promise((resolve, reject) => {
        const request = store.put(record);
        request.onsuccess = () => resolve(id);
        request.onerror = () => reject(handleError(request.error, 'saveFile'));
    });
}

/**
 * Получение метаданных файла
 * @param {string} fileId - ID файла
 * @returns {Promise<Object|null>} метаданные файла
 */
export async function getFile(fileId) {
    if (!isAvailable()) {
        const files = JSON.parse(localStorage.getItem('excel_files') || '[]');
        return files.find(f => f.id === fileId) || null;
    }

    const store = getStore(STORES.FILES, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.get(fileId);
        request.onsuccess = () => resolve(request.result || null);
        request.onerror = () => reject(handleError(request.error, 'getFile'));
    });
}

/**
 * Получение списка всех файлов
 * @returns {Promise<Array>} массив метаданных файлов
 */
export async function listFiles() {
    if (!isAvailable()) {
        return JSON.parse(localStorage.getItem('excel_files') || '[]');
    }

    const store = getStore(STORES.FILES, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.getAll();
        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => reject(handleError(request.error, 'listFiles'));
    });
}

/**
 * Удаление файла и всех его листов
 * @param {string} fileId - ID файла
 * @returns {Promise<void>}
 */
export async function deleteFile(fileId) {
    if (!isAvailable()) {
        const files = JSON.parse(localStorage.getItem('excel_files') || '[]');
        const filtered = files.filter(f => f.id !== fileId);
        localStorage.setItem('excel_files', JSON.stringify(filtered));
        // Также удаляем листы из localStorage (если есть)
        return;
    }

    const transaction = createTransaction([STORES.FILES, STORES.SHEETS], 'readwrite');
    const filesStore = transaction.objectStore(STORES.FILES);
    const sheetsStore = transaction.objectStore(STORES.SHEETS);
    const sheetsIndex = sheetsStore.index('fileId');

    return new Promise((resolve, reject) => {
        // Удаляем файл
        const deleteFileRequest = filesStore.delete(fileId);

        // Удаляем все листы файла
        const deleteSheetsRequest = sheetsIndex.openKeyCursor(IDBKeyRange.only(fileId));
        const keysToDelete = [];

        deleteSheetsRequest.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                keysToDelete.push(cursor.primaryKey);
                cursor.continue();
            } else {
                // Удаляем все найденные листы
                const deletePromises = keysToDelete.map(key => {
                    return new Promise((res, rej) => {
                        const delReq = sheetsStore.delete(key);
                        delReq.onsuccess = () => res();
                        delReq.onerror = () => rej(delReq.error);
                    });
                });

                Promise.all(deletePromises)
                    .then(() => resolve())
                    .catch(err => reject(handleError(err, 'deleteFile')));
            }
        };

        deleteFileRequest.onsuccess = () => {
            // Файл удален, ждем удаления листов
        };

        deleteFileRequest.onerror = () => {
            reject(handleError(deleteFileRequest.error, 'deleteFile'));
        };

        deleteSheetsRequest.onerror = () => {
            reject(handleError(deleteSheetsRequest.error, 'deleteFile'));
        };
    });
}

// ============================
// SHEETS OPERATIONS
// ============================

/**
 * Сохранение листа Excel
 * @param {string} fileId - ID файла
 * @param {string} sheetName - имя листа
 * @param {Object} sheetData - данные листа { columns, rows, rowCount }
 * @returns {Promise<void>}
 */
export async function saveSheet(fileId, sheetName, sheetData) {
    if (!isAvailable()) {
        console.warn('[IndexedDB] Sheets storage not available, using fallback');
        // Fallback: сохраняем в localStorage (ограниченный размер)
        const key = `excel_sheet_${fileId}_${sheetName}`;
        try {
            localStorage.setItem(key, JSON.stringify(sheetData));
        } catch (e) {
            console.error('[IndexedDB] localStorage quota exceeded for sheet:', e);
        }
        return;
    }

    const store = getStore(STORES.SHEETS, 'readwrite');
    const id = `${fileId}_${sheetName}`;
    const record = {
        id: id,
        fileId: fileId,
        name: sheetName,
        columns: sheetData.columns || [],
        rowCount: sheetData.rowCount || (sheetData.rows ? sheetData.rows.length : 0),
        data: sheetData.rows || [] // Сохраняем данные напрямую (для небольших листов)
    };

    return new Promise((resolve, reject) => {
        const request = store.put(record);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(handleError(request.error, 'saveSheet'));
    });
}

/**
 * Получение листа Excel
 * @param {string} fileId - ID файла
 * @param {string} sheetName - имя листа
 * @returns {Promise<Object|null>} данные листа { columns, rows }
 */
export async function getSheet(fileId, sheetName) {
    if (!isAvailable()) {
        const key = `excel_sheet_${fileId}_${sheetName}`;
        const raw = localStorage.getItem(key);
        return raw ? JSON.parse(raw) : null;
    }

    const store = getStore(STORES.SHEETS, 'readonly');
    const id = `${fileId}_${sheetName}`;

    return new Promise((resolve, reject) => {
        const request = store.get(id);
        request.onsuccess = () => {
            if (!request.result) {
                resolve(null);
                return;
            }
            const result = {
                columns: request.result.columns || [],
                rows: request.result.data || []
            };
            resolve(result);
        };
        request.onerror = () => reject(handleError(request.error, 'getSheet'));
    });
}

/**
 * Получение списка листов для файла
 * @param {string} fileId - ID файла
 * @returns {Promise<Array<string>>} массив имен листов
 */
export async function listSheets(fileId) {
    if (!isAvailable()) {
        // Fallback: перебираем localStorage ключи
        const sheets = [];
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key && key.startsWith(`excel_sheet_${fileId}_`)) {
                const sheetName = key.replace(`excel_sheet_${fileId}_`, '');
                sheets.push(sheetName);
            }
        }
        return sheets;
    }

    const store = getStore(STORES.SHEETS, 'readonly');
    const index = store.index('fileId');

    return new Promise((resolve, reject) => {
        const request = index.getAll(fileId);
        request.onsuccess = () => {
            const sheets = (request.result || []).map(record => record.name);
            resolve(sheets);
        };
        request.onerror = () => reject(handleError(request.error, 'listSheets'));
    });
}

/**
 * Удаление листа
 * @param {string} fileId - ID файла
 * @param {string} sheetName - имя листа
 * @returns {Promise<void>}
 */
export async function deleteSheet(fileId, sheetName) {
    if (!isAvailable()) {
        const key = `excel_sheet_${fileId}_${sheetName}`;
        localStorage.removeItem(key);
        return;
    }

    const store = getStore(STORES.SHEETS, 'readwrite');
    const id = `${fileId}_${sheetName}`;

    return new Promise((resolve, reject) => {
        const request = store.delete(id);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(handleError(request.error, 'deleteSheet'));
    });
}

// ============================
// UTILITY OPERATIONS
// ============================

/**
 * Очистка всех данных из IndexedDB
 * @returns {Promise<void>}
 */
export async function clearAll() {
    if (!isAvailable()) {
        // Fallback: очищаем localStorage
        localStorage.removeItem(LEGACY_KEYS.INVENTORY);
        for (let i = localStorage.length - 1; i >= 0; i--) {
            const key = localStorage.key(i);
            if (key && (key.startsWith(LEGACY_KEYS.MATRIX_PREFIX) ||
                key.startsWith('excel_') ||
                key.startsWith('matrix_'))) {
                localStorage.removeItem(key);
            }
        }
        return;
    }

    const transaction = createTransaction([STORES.INVENTORY, STORES.FILES, STORES.SHEETS, STORES.MATRIX], 'readwrite');

    return new Promise((resolve, reject) => {
        const stores = [
            transaction.objectStore(STORES.INVENTORY),
            transaction.objectStore(STORES.FILES),
            transaction.objectStore(STORES.SHEETS),
            transaction.objectStore(STORES.MATRIX)
        ];

        const clearPromises = stores.map(store => {
            return new Promise((res, rej) => {
                const request = store.clear();
                request.onsuccess = () => res();
                request.onerror = () => rej(request.error);
            });
        });

        Promise.all(clearPromises)
            .then(() => resolve())
            .catch(err => reject(handleError(err, 'clearAll')));
    });
}

/**
 * Очистка данных конкретной сущности инвентаря
 * @param {string} entity - тип сущности
 * @returns {Promise<void>}
 */
export async function clearEntity(entity) {
    if (!isAvailable()) {
        // Fallback: очищаем из localStorage
        const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
        if (raw) {
            try {
                const data = JSON.parse(raw);
                data[entity] = [];
                localStorage.setItem(LEGACY_KEYS.INVENTORY, JSON.stringify(data));
            } catch (e) {
                console.error('[IndexedDB] Error clearing entity from localStorage:', e);
            }
        }
        return;
    }

    const store = getStore(STORES.INVENTORY, 'readwrite');
    const index = store.index('entity');

    return new Promise((resolve, reject) => {
        const request = index.openKeyCursor(IDBKeyRange.only(entity));
        const keysToDelete = [];

        request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                keysToDelete.push(cursor.primaryKey);
                cursor.continue();
            } else {
                // Удаляем все найденные записи
                const deletePromises = keysToDelete.map(key => {
                    return new Promise((res, rej) => {
                        const delReq = store.delete(key);
                        delReq.onsuccess = () => res();
                        delReq.onerror = () => rej(delReq.error);
                    });
                });

                Promise.all(deletePromises)
                    .then(() => resolve())
                    .catch(err => reject(handleError(err, 'clearEntity')));
            }
        };

        request.onerror = () => {
            reject(handleError(request.error, 'clearEntity'));
        };
    });
}

/**
 * Получение статистики по данным в IndexedDB
 * @returns {Promise<Object>} объект со статистикой
 */
export async function getStats() {
    if (!isAvailable()) {
        return {
            available: false,
            message: 'IndexedDB not available, using localStorage fallback'
        };
    }

    const stats = {
        available: true,
        inventory: {},
        files: 0,
        sheets: 0,
        matrix: 0
    };

    try {
        // Статистика по инвентарю
        for (const entity of Object.values(ENTITIES)) {
            const items = await getInventory(entity);
            stats.inventory[entity] = items.length;
        }

        // Статистика по файлам
        const files = await listFiles();
        stats.files = files.length;

        // Статистика по листам
        const sheetsStore = getStore(STORES.SHEETS, 'readonly');
        const sheetsRequest = sheetsStore.getAll();
        await new Promise((resolve, reject) => {
            sheetsRequest.onsuccess = () => {
                stats.sheets = sheetsRequest.result.length;
                resolve();
            };
            sheetsRequest.onerror = () => reject(sheetsRequest.error);
        });

        // Статистика по матрице
        const matrixStore = getStore(STORES.MATRIX, 'readonly');
        const matrixRequest = matrixStore.getAll();
        await new Promise((resolve, reject) => {
            matrixRequest.onsuccess = () => {
                stats.matrix = matrixRequest.result.length;
                resolve();
            };
            matrixRequest.onerror = () => reject(matrixRequest.error);
        });
    } catch (error) {
        console.error('[IndexedDB] Error getting stats:', error);
        stats.error = error.message;
    }

    return stats;
}

// ============================
// PUBLIC API
// ============================

// ============================
// MATRIX OPERATIONS
// ============================

/**
 * Получение значения ячейки матрицы
 * @param {string} serviceId - ID сервиса
 * @param {string} envId - ID окружения
 * @returns {Promise<string>} значение ячейки или пустая строка
 */
export async function getMatrixCell(serviceId, envId) {
    const cellId = `${serviceId}__${envId}`;

    if (!isAvailable()) {
        // Fallback на localStorage
        return localStorage.getItem(LEGACY_KEYS.MATRIX_PREFIX + cellId) || '';
    }

    const store = getStore(STORES.MATRIX, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.get(cellId);
        request.onsuccess = () => {
            resolve(request.result ? request.result.value : '');
        };
        request.onerror = () => reject(handleError(request.error, 'getMatrixCell'));
    });
}

/**
 * Установка значения ячейки матрицы
 * @param {string} serviceId - ID сервиса
 * @param {string} envId - ID окружения
 * @param {string} value - значение для сохранения
 * @returns {Promise<void>}
 */
export async function setMatrixCell(serviceId, envId, value) {
    const cellId = `${serviceId}__${envId}`;

    if (!isAvailable()) {
        // Fallback на localStorage
        if (value) {
            localStorage.setItem(LEGACY_KEYS.MATRIX_PREFIX + cellId, value);
        } else {
            localStorage.removeItem(LEGACY_KEYS.MATRIX_PREFIX + cellId);
        }
        return;
    }

    const store = getStore(STORES.MATRIX, 'readwrite');

    return new Promise((resolve, reject) => {
        if (value) {
            const record = {
                id: cellId,
                value: value
            };
            const request = store.put(record);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(handleError(request.error, 'setMatrixCell'));
        } else {
            // Удаляем пустое значение
            const request = store.delete(cellId);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(handleError(request.error, 'setMatrixCell'));
        }
    });
}

/**
 * Получение всех значений матрицы
 * @returns {Promise<Object>} объект { "serviceId__envId": "value", ... }
 */
export async function getAllMatrix() {
    if (!isAvailable()) {
        // Fallback на localStorage
        const matrix = {};
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key && key.startsWith(LEGACY_KEYS.MATRIX_PREFIX)) {
                const cellId = key.substring(LEGACY_KEYS.MATRIX_PREFIX.length);
                matrix[cellId] = localStorage.getItem(key);
            }
        }
        return matrix;
    }

    const store = getStore(STORES.MATRIX, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.getAll();
        request.onsuccess = () => {
            const matrix = {};
            (request.result || []).forEach(record => {
                matrix[record.id] = record.value;
            });
            resolve(matrix);
        };
        request.onerror = () => reject(handleError(request.error, 'getAllMatrix'));
    });
}

// ============================
// TEMPLATE OPERATIONS
// ============================

/**
 * Сохранение шаблона
 * @param {Object} templateData - данные шаблона
 * @param {string} templateId - ID шаблона (если не указан, будет сгенерирован)
 * @returns {Promise<string>} ID сохраненного шаблона
 */
export async function saveTemplate(templateData, templateId = null) {
    if (!isAvailable()) {
        // Fallback на localStorage
        const templates = JSON.parse(localStorage.getItem('templates') || '[]');
        const id = templateId || generateId('template');
        const template = {
            id,
            ...templateData,
            createdAt: templateData.createdAt || new Date().toISOString(),
            updatedAt: new Date().toISOString()
        };
        const index = templates.findIndex(t => t.id === id);
        if (index >= 0) {
            templates[index] = template;
        } else {
            templates.push(template);
        }
        localStorage.setItem('templates', JSON.stringify(templates));
        return id;
    }

    const store = getStore(STORES.TEMPLATES, 'readwrite');
    const id = templateId || generateId('template');
    const template = {
        id,
        name: templateData.name || '',
        type: templateData.type || 'generic',
        category: templateData.category || 'commands',
        description: templateData.description || '',
        template: templateData.template || '',
        variables: templateData.variables || [],
        createdAt: templateData.createdAt || new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        usageCount: templateData.usageCount || 0
    };

    return new Promise((resolve, reject) => {
        const request = store.put(template);
        request.onsuccess = () => resolve(id);
        request.onerror = () => reject(handleError(request.error, 'saveTemplate'));
    });
}

/**
 * Получение шаблона
 * @param {string} templateId - ID шаблона
 * @returns {Promise<Object|null>} шаблон
 */
export async function getTemplate(templateId) {
    if (!isAvailable()) {
        const templates = JSON.parse(localStorage.getItem('templates') || '[]');
        return templates.find(t => t.id === templateId) || null;
    }

    const store = getStore(STORES.TEMPLATES, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.get(templateId);
        request.onsuccess = () => resolve(request.result || null);
        request.onerror = () => reject(handleError(request.error, 'getTemplate'));
    });
}

/**
 * Получение списка всех шаблонов
 * @returns {Promise<Array>} массив шаблонов
 */
export async function listTemplates() {
    if (!isAvailable()) {
        return JSON.parse(localStorage.getItem('templates') || '[]');
    }

    const store = getStore(STORES.TEMPLATES, 'readonly');

    return new Promise((resolve, reject) => {
        const request = store.getAll();
        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => reject(handleError(request.error, 'listTemplates'));
    });
}

/**
 * Получение шаблонов по типу
 * @param {string} type - тип шаблона
 * @returns {Promise<Array>} массив шаблонов
 */
export async function getTemplatesByType(type) {
    if (!isAvailable()) {
        const templates = JSON.parse(localStorage.getItem('templates') || '[]');
        return templates.filter(t => t.type === type);
    }

    const store = getStore(STORES.TEMPLATES, 'readonly');
    const index = store.index('type');

    return new Promise((resolve, reject) => {
        const request = index.getAll(type);
        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => reject(handleError(request.error, 'getTemplatesByType'));
    });
}

/**
 * Получение шаблонов по категории
 * @param {string} category - категория шаблона
 * @returns {Promise<Array>} массив шаблонов
 */
export async function getTemplatesByCategory(category) {
    if (!isAvailable()) {
        const templates = JSON.parse(localStorage.getItem('templates') || '[]');
        return templates.filter(t => t.category === category);
    }

    const store = getStore(STORES.TEMPLATES, 'readonly');
    const index = store.index('category');

    return new Promise((resolve, reject) => {
        const request = index.getAll(category);
        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => reject(handleError(request.error, 'getTemplatesByCategory'));
    });
}

/**
 * Удаление шаблона
 * @param {string} templateId - ID шаблона
 * @returns {Promise<void>}
 */
export async function deleteTemplate(templateId) {
    if (!isAvailable()) {
        const templates = JSON.parse(localStorage.getItem('templates') || '[]');
        const filtered = templates.filter(t => t.id !== templateId);
        localStorage.setItem('templates', JSON.stringify(filtered));
        return;
    }

    const store = getStore(STORES.TEMPLATES, 'readwrite');

    return new Promise((resolve, reject) => {
        const request = store.delete(templateId);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(handleError(request.error, 'deleteTemplate'));
    });
}

export const storageAPI = {
    init,
    isAvailable,
    generateId,
    createInventoryKey,
    parseInventoryKey,
    // Inventory operations
    getInventory,
    saveInventory,
    addInventoryItem,
    updateInventoryItem,
    deleteInventoryItem,
    // Files operations
    saveFile,
    getFile,
    listFiles,
    deleteFile,
    // Sheets operations
    saveSheet,
    getSheet,
    listSheets,
    deleteSheet,
    // Matrix operations
    getMatrixCell,
    setMatrixCell,
    getAllMatrix,
    // Template operations
    saveTemplate,
    getTemplate,
    listTemplates,
    deleteTemplate,
    getTemplatesByType,
    getTemplatesByCategory
};

// Экспорт для использования в других модулях
export default storageAPI;

