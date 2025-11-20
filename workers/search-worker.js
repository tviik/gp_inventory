/* ============================
   SEARCH WORKER
   ============================
   
   WebWorker для индексации и поиска по данным Excel.
   Работает в отдельном потоке, не блокирует UI.
*/

// ============================
// STATE
// ============================

let searchIndex = {}; // Инвертированный индекс: word -> [entries]
let indexMetadata = {
    fileCount: 0,
    sheetCount: 0,
    totalRows: 0,
    indexedAt: null
};

// ============================
// TOKENIZATION
// ============================

/**
 * Токенизация текста - разбиение на слова
 * @param {string} text - исходный текст
 * @returns {Array<string>} массив слов
 */
function tokenize(text) {
    if (!text || typeof text !== 'string') {
        return [];
    }

    // Нормализация: lowercase, удаление спецсимволов
    const normalized = text.toLowerCase()
        .replace(/[^\w\sа-яё]/gi, ' ')
        .trim();

    if (!normalized) {
        return [];
    }

    // Разбиение на слова (пробелы, дефисы, подчеркивания)
    const words = normalized.split(/[\s\-_]+/)
        .filter(w => w.length >= 2); // минимум 2 символа

    return words;
}

// ============================
// INDEXING
// ============================

/**
 * Индексация одной строки
 * @param {string} fileId - ID файла
 * @param {string} fileName - имя файла
 * @param {string} sheetName - имя листа
 * @param {number} rowIndex - индекс строки
 * @param {Object} row - данные строки (Record<string, any>)
 * @param {Array<string>} columns - список колонок
 * @returns {Array} массив записей индекса
 */
function indexRow(fileId, fileName, sheetName, rowIndex, row, columns) {
    const entries = [];

    columns.forEach((colName, colIndex) => {
        const value = row[colName];
        if (value == null || value === '') {
            return;
        }

        const text = String(value);
        const words = tokenize(text);

        words.forEach(word => {
            const matchPosition = text.toLowerCase().indexOf(word);
            
            entries.push({
                fileId,
                fileName,
                sheetName,
                rowIndex,
                columnName: colName,
                columnIndex: colIndex,
                value: text,
                matchPosition
            });
        });
    });

    return entries;
}

/**
 * Индексация листа
 * @param {string} fileId - ID файла
 * @param {string} fileName - имя файла
 * @param {string} sheetName - имя листа
 * @param {Object} sheetData - данные листа { columns, rows }
 */
function indexSheet(fileId, fileName, sheetName, sheetData) {
    const { columns = [], rows = [] } = sheetData;

    rows.forEach((row, rowIndex) => {
        const entries = indexRow(fileId, fileName, sheetName, rowIndex, row, columns);

        entries.forEach(entry => {
            const word = tokenize(entry.value)[0]; // Берем первое слово для ключа
            if (!word) return;

            if (!searchIndex[word]) {
                searchIndex[word] = [];
            }

            searchIndex[word].push(entry);
        });
    });

    // Обновляем метаданные
    indexMetadata.sheetCount++;
    indexMetadata.totalRows += rows.length;
}

/**
 * Удаление листа из индекса
 * @param {string} fileId - ID файла
 * @param {string} sheetName - имя листа
 */
function removeSheetFromIndex(fileId, sheetName) {
    // Удаляем все записи для этого листа
    Object.keys(searchIndex).forEach(word => {
        searchIndex[word] = searchIndex[word].filter(entry => {
            return !(entry.fileId === fileId && entry.sheetName === sheetName);
        });

        // Удаляем пустые массивы
        if (searchIndex[word].length === 0) {
            delete searchIndex[word];
        }
    });

    // Обновляем метаданные (приблизительно)
    indexMetadata.sheetCount = Math.max(0, indexMetadata.sheetCount - 1);
}

/**
 * Очистка всего индекса
 */
function clearIndex() {
    searchIndex = {};
    indexMetadata = {
        fileCount: 0,
        sheetCount: 0,
        totalRows: 0,
        indexedAt: null
    };
}

// ============================
// SEARCH
// ============================

/**
 * Поиск по индексу
 * @param {string} query - поисковый запрос
 * @param {Object} options - опции поиска
 * @returns {Object} результаты поиска
 */
function search(query, options = {}) {
    const startTime = performance.now();
    const { fileId, sheetName, columnName, limit = 100 } = options;

    if (!query || typeof query !== 'string') {
        return {
            query: '',
            totalMatches: 0,
            results: [],
            searchTime: 0
        };
    }

    // Токенизируем запрос
    const queryWords = tokenize(query);
    if (queryWords.length === 0) {
        return {
            query,
            totalMatches: 0,
            results: [],
            searchTime: 0
        };
    }

    // Ищем совпадения для каждого слова
    const matchesMap = new Map(); // Для дедупликации результатов

    queryWords.forEach(word => {
        const entries = searchIndex[word] || [];

        entries.forEach(entry => {
            // Фильтрация по опциям
            if (fileId && entry.fileId !== fileId) return;
            if (sheetName && entry.sheetName !== sheetName) return;
            if (columnName && entry.columnName !== columnName) return;

            // Ключ для дедупликации
            const key = `${entry.fileId}::${entry.sheetName}::${entry.rowIndex}::${entry.columnName}`;

            if (!matchesMap.has(key)) {
                matchesMap.set(key, {
                    ...entry,
                    matchCount: 1,
                    matchedWords: [word]
                });
            } else {
                const existing = matchesMap.get(key);
                existing.matchCount++;
                if (!existing.matchedWords.includes(word)) {
                    existing.matchedWords.push(word);
                }
            }
        });
    });

    // Преобразуем в массив и сортируем по релевантности
    let results = Array.from(matchesMap.values())
        .sort((a, b) => {
            // Сначала по количеству совпадений
            if (b.matchCount !== a.matchCount) {
                return b.matchCount - a.matchCount;
            }
            // Затем по позиции совпадения (раньше = лучше)
            return a.matchPosition - b.matchPosition;
        })
        .slice(0, limit);

    const searchTime = performance.now() - startTime;

    return {
        query,
        totalMatches: matchesMap.size,
        results,
        searchTime: Math.round(searchTime * 100) / 100
    };
}

/**
 * Получение статистики индекса
 */
function getIndexStats() {
    const wordCount = Object.keys(searchIndex).length;
    let totalEntries = 0;
    Object.values(searchIndex).forEach(entries => {
        totalEntries += entries.length;
    });

    return {
        ...indexMetadata,
        wordCount,
        totalEntries
    };
}

// ============================
// MESSAGE HANDLER
// ============================

self.onmessage = function(e) {
    const { type, payload, messageId } = e.data;

    try {
        switch (type) {
            case 'INDEX_SHEET':
                indexSheet(
                    payload.fileId,
                    payload.fileName,
                    payload.sheetName,
                    payload.sheetData
                );
                self.postMessage({
                    type: 'INDEX_SHEET_SUCCESS',
                    payload: { fileId: payload.fileId, sheetName: payload.sheetName },
                    messageId
                });
                break;

            case 'REMOVE_SHEET':
                removeSheetFromIndex(payload.fileId, payload.sheetName);
                self.postMessage({
                    type: 'REMOVE_SHEET_SUCCESS',
                    payload: { fileId: payload.fileId, sheetName: payload.sheetName },
                    messageId
                });
                break;

            case 'CLEAR_INDEX':
                clearIndex();
                self.postMessage({ 
                    type: 'CLEAR_INDEX_SUCCESS',
                    payload: {},
                    messageId 
                });
                break;

            case 'SEARCH':
                const results = search(payload.query, payload.options);
                self.postMessage({
                    type: 'SEARCH_RESULT',
                    payload: results,
                    messageId
                });
                break;

            case 'GET_STATS':
                const stats = getIndexStats();
                self.postMessage({
                    type: 'STATS_RESULT',
                    payload: stats,
                    messageId
                });
                break;

            default:
                self.postMessage({
                    type: 'ERROR',
                    payload: { message: `Unknown message type: ${type}` },
                    messageId
                });
        }
    } catch (error) {
        self.postMessage({
            type: 'ERROR',
            payload: { message: error.message, stack: error.stack },
            messageId
        });
    }
};

// Уведомление о готовности
self.postMessage({ type: 'WORKER_READY' });

