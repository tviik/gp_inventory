/* ============================
   JOB MODEL (v0.9)
   ============================
   
   Модель данных для задач опроса (Jobs).
   Валидация, создание, обновление Job объектов.
*/

/**
 * Схема Job
 */
export const JOB_SCHEMA = {
    id: { required: true, type: 'string' },
    name: { required: true, type: 'string', maxLength: 200 },
    description: { required: false, type: 'string', maxLength: 1000 },
    type: { required: true, type: 'string', enum: ['endpoints', 'hosts'] },
    targetIds: { required: true, type: 'array', minLength: 1 },
    templateId: { required: false, type: 'string' },
    schedule: { required: false, type: 'object' },
    createdAt: { required: true, type: 'string' },
    updatedAt: { required: true, type: 'string' },
    lastRunAt: { required: false, type: 'string' },
    status: { required: true, type: 'string', enum: ['active', 'paused'] }
};

/**
 * Значения по умолчанию для Job
 */
export function createDefaultJob() {
    return {
        name: '',
        description: '',
        type: 'endpoints',
        targetIds: [],
        templateId: null,
        schedule: null,
        status: 'active',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        lastRunAt: null
    };
}

/**
 * Валидация Job
 * @param {Object} job - объект Job для валидации
 * @returns {Object} { valid: boolean, errors: Array<string> }
 */
export function validateJob(job) {
    const errors = [];

    if (!job || typeof job !== 'object') {
        return { valid: false, errors: ['Job must be an object'] };
    }

    // Проверка обязательных полей
    if (!job.name || typeof job.name !== 'string' || job.name.trim().length === 0) {
        errors.push('Job name is required and must be a non-empty string');
    } else if (job.name.length > 200) {
        errors.push('Job name must not exceed 200 characters');
    }

    if (!job.type || !['endpoints', 'hosts'].includes(job.type)) {
        errors.push('Job type must be either "endpoints" or "hosts"');
    }

    if (!Array.isArray(job.targetIds) || job.targetIds.length === 0) {
        errors.push('Job targetIds must be a non-empty array');
    }

    if (job.targetIds && Array.isArray(job.targetIds)) {
        const invalidIds = job.targetIds.filter(id => typeof id !== 'string' || id.trim().length === 0);
        if (invalidIds.length > 0) {
            errors.push('All targetIds must be non-empty strings');
        }
    }

    if (job.description && typeof job.description === 'string' && job.description.length > 1000) {
        errors.push('Job description must not exceed 1000 characters');
    }

    if (job.templateId && typeof job.templateId !== 'string') {
        errors.push('Job templateId must be a string or null');
    }

    if (job.status && !['active', 'paused'].includes(job.status)) {
        errors.push('Job status must be either "active" or "paused"');
    }

    // Проверка schedule (если указан)
    if (job.schedule) {
        if (typeof job.schedule !== 'object') {
            errors.push('Job schedule must be an object');
        } else {
            if (typeof job.schedule.enabled !== 'boolean') {
                errors.push('Job schedule.enabled must be a boolean');
            }
            if (job.schedule.interval && (typeof job.schedule.interval !== 'number' || job.schedule.interval <= 0)) {
                errors.push('Job schedule.interval must be a positive number');
            }
        }
    }

    // Проверка дат
    if (job.createdAt && !isValidISOString(job.createdAt)) {
        errors.push('Job createdAt must be a valid ISO string');
    }

    if (job.updatedAt && !isValidISOString(job.updatedAt)) {
        errors.push('Job updatedAt must be a valid ISO string');
    }

    if (job.lastRunAt && !isValidISOString(job.lastRunAt)) {
        errors.push('Job lastRunAt must be a valid ISO string or null');
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

/**
 * Создание нового Job с валидацией
 * @param {Object} jobData - данные для создания Job
 * @param {string} jobId - ID Job (если не указан, будет сгенерирован)
 * @returns {Object} валидированный Job объект
 */
export function createJob(jobData, jobId = null) {
    const job = {
        ...createDefaultJob(),
        ...jobData
    };

    if (jobId) {
        job.id = jobId;
    }

    // Обновляем даты
    if (!job.createdAt) {
        job.createdAt = new Date().toISOString();
    }
    job.updatedAt = new Date().toISOString();

    // Валидация
    const validation = validateJob(job);
    if (!validation.valid) {
        throw new Error(`Invalid Job: ${validation.errors.join(', ')}`);
    }

    return job;
}

/**
 * Обновление Job
 * @param {Object} existingJob - существующий Job
 * @param {Object} updates - обновления
 * @returns {Object} обновленный Job
 */
export function updateJob(existingJob, updates) {
    const updatedJob = {
        ...existingJob,
        ...updates,
        id: existingJob.id, // ID не изменяется
        createdAt: existingJob.createdAt, // createdAt не изменяется
        updatedAt: new Date().toISOString() // Обновляем updatedAt
    };

    // Валидация
    const validation = validateJob(updatedJob);
    if (!validation.valid) {
        throw new Error(`Invalid Job update: ${validation.errors.join(', ')}`);
    }

    return updatedJob;
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

