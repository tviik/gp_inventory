/* ============================
   SCAN RESULT MODEL (v0.9)
   ============================
   
   Модель данных для результатов опроса (ScanResults).
   Валидация, создание, обновление ScanResult объектов.
*/

/**
 * Схема ScanResult
 */
export const SCAN_RESULT_SCHEMA = {
    id: { required: true, type: 'string' },
    jobId: { required: true, type: 'string' },
    endpointId: { required: false, type: 'string' },
    hostId: { required: false, type: 'string' },
    status: { required: true, type: 'string', enum: ['success', 'error', 'timeout'] },
    response: { required: false, type: 'string' },
    version: { required: false, type: 'string' },
    build: { required: false, type: 'string' },
    error: { required: false, type: 'string' },
    duration: { required: false, type: 'number' },
    timestamp: { required: true, type: 'string' },
    metadata: { required: false, type: 'object' }
};

/**
 * Значения по умолчанию для ScanResult
 */
export function createDefaultScanResult() {
    return {
        jobId: '',
        endpointId: null,
        hostId: null,
        status: 'success',
        response: null,
        version: null,
        build: null,
        error: null,
        duration: null,
        timestamp: new Date().toISOString(),
        metadata: null
    };
}

/**
 * Валидация ScanResult
 * @param {Object} result - объект ScanResult для валидации
 * @returns {Object} { valid: boolean, errors: Array<string> }
 */
export function validateScanResult(result) {
    const errors = [];

    if (!result || typeof result !== 'object') {
        return { valid: false, errors: ['ScanResult must be an object'] };
    }

    // Проверка обязательных полей
    if (!result.jobId || typeof result.jobId !== 'string' || result.jobId.trim().length === 0) {
        errors.push('ScanResult jobId is required and must be a non-empty string');
    }

    if (!result.status || !['success', 'error', 'timeout'].includes(result.status)) {
        errors.push('ScanResult status must be either "success", "error", or "timeout"');
    }

    // Должен быть указан либо endpointId, либо hostId
    if (!result.endpointId && !result.hostId) {
        errors.push('ScanResult must have either endpointId or hostId');
    }

    if (result.endpointId && result.hostId) {
        errors.push('ScanResult cannot have both endpointId and hostId');
    }

    if (result.endpointId && (typeof result.endpointId !== 'string' || result.endpointId.trim().length === 0)) {
        errors.push('ScanResult endpointId must be a non-empty string or null');
    }

    if (result.hostId && (typeof result.hostId !== 'string' || result.hostId.trim().length === 0)) {
        errors.push('ScanResult hostId must be a non-empty string or null');
    }

    // Проверка даты
    if (!result.timestamp || !isValidISOString(result.timestamp)) {
        errors.push('ScanResult timestamp must be a valid ISO string');
    }

    // Проверка duration
    if (result.duration !== null && result.duration !== undefined) {
        if (typeof result.duration !== 'number' || result.duration < 0) {
            errors.push('ScanResult duration must be a non-negative number or null');
        }
    }

    // Проверка metadata
    if (result.metadata !== null && result.metadata !== undefined) {
        if (typeof result.metadata !== 'object' || Array.isArray(result.metadata)) {
            errors.push('ScanResult metadata must be an object or null');
        }
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

/**
 * Создание нового ScanResult с валидацией
 * @param {Object} resultData - данные для создания ScanResult
 * @param {string} resultId - ID результата (если не указан, будет сгенерирован)
 * @returns {Object} валидированный ScanResult объект
 */
export function createScanResult(resultData, resultId = null) {
    const result = {
        ...createDefaultScanResult(),
        ...resultData
    };

    if (resultId) {
        result.id = resultId;
    }

    // Устанавливаем timestamp, если не указан
    if (!result.timestamp) {
        result.timestamp = new Date().toISOString();
    }

    // Валидация
    const validation = validateScanResult(result);
    if (!validation.valid) {
        throw new Error(`Invalid ScanResult: ${validation.errors.join(', ')}`);
    }

    return result;
}

/**
 * Обновление ScanResult
 * @param {Object} existingResult - существующий ScanResult
 * @param {Object} updates - обновления
 * @returns {Object} обновленный ScanResult
 */
export function updateScanResult(existingResult, updates) {
    const updatedResult = {
        ...existingResult,
        ...updates,
        id: existingResult.id, // ID не изменяется
        timestamp: existingResult.timestamp // timestamp не изменяется
    };

    // Валидация
    const validation = validateScanResult(updatedResult);
    if (!validation.valid) {
        throw new Error(`Invalid ScanResult update: ${validation.errors.join(', ')}`);
    }

    return updatedResult;
}

/**
 * Извлечение version и build из ответа
 * @param {string} response - ответ от endpoint
 * @returns {Object} { version: string|null, build: string|null }
 */
export function extractVersionFromResponse(response) {
    if (!response || typeof response !== 'string') {
        return { version: null, build: null };
    }

    try {
        // Пробуем парсить как JSON
        const json = JSON.parse(response);
        
        // Ищем version и build в разных вариантах названий
        const version = json.version || json.Version || json.VERSION || 
                       json.app_version || json.appVersion || 
                       json.iib_version || json.iibVersion || null;
        
        const build = json.build || json.Build || json.BUILD || 
                     json.build_number || json.buildNumber || 
                     json.buildno || json.buildNo || null;

        return { version, build };
    } catch (e) {
        // Не JSON, пробуем regex
        const versionMatch = response.match(/version[:\s]*([\d.]+)/i);
        const buildMatch = response.match(/build[:\s]*([\d\w]+)/i);

        return {
            version: versionMatch ? versionMatch[1] : null,
            build: buildMatch ? buildMatch[1] : null
        };
    }
}

/**
 * Проверка валидности ISO строки
 * @param {string} str - строка для проверки
 * @returns {boolean}
 */
function isValidISOString(str) {
    if (typeof str !== 'string') return false;
    const date = new Date(str);
    return !isNaN(date.getTime()) && str === date.toISOString();
}

