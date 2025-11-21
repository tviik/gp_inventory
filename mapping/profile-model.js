/* ============================
   MAPPING PROFILE MODEL (v0.10)
   ============================
   
   Модель данных для профилей маппинга Excel → сущности.
   Валидация, создание, обновление Profile объектов.
*/

/**
 * Допустимые типы сущностей
 */
export const VALID_ENTITIES = ['environments', 'hosts', 'services', 'endpoints', 'snapshots'];

/**
 * Схемы полей для каждой сущности
 */
export const ENTITY_SCHEMAS = {
    environments: ['id', 'name', 'description'],
    hosts: ['id', 'name', 'ip', 'envId'],
    services: ['id', 'name', 'owner'],
    endpoints: ['id', 'serviceId', 'envId', 'url', 'method'],
    snapshots: ['endpointId', 'version', 'build', 'timestamp']
};

/**
 * Схема MappingProfile
 */
export const MAPPING_PROFILE_SCHEMA = {
    id: { required: true, type: 'string' },
    name: { required: true, type: 'string', maxLength: 200 },
    description: { required: false, type: 'string', maxLength: 1000 },
    entity: { required: true, type: 'string', enum: VALID_ENTITIES },
    mapping: { required: true, type: 'object' },
    createdAt: { required: true, type: 'string' },
    updatedAt: { required: true, type: 'string' },
    usageCount: { required: false, type: 'number' }
};

/**
 * Значения по умолчанию для MappingProfile
 */
export function createDefaultProfile() {
    return {
        name: '',
        description: '',
        entity: 'hosts',
        mapping: {},
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        usageCount: 0
    };
}

/**
 * Валидация MappingProfile
 * @param {Object} profile - объект Profile для валидации
 * @returns {Object} { valid: boolean, errors: Array<string> }
 */
export function validateProfile(profile) {
    const errors = [];

    if (!profile || typeof profile !== 'object') {
        return { valid: false, errors: ['Profile must be an object'] };
    }

    // Проверка обязательных полей
    if (!profile.name || typeof profile.name !== 'string' || profile.name.trim().length === 0) {
        errors.push('Profile name is required and must be a non-empty string');
    } else if (profile.name.length > 200) {
        errors.push('Profile name must not exceed 200 characters');
    }

    if (!profile.entity || !VALID_ENTITIES.includes(profile.entity)) {
        errors.push(`Profile entity must be one of: ${VALID_ENTITIES.join(', ')}`);
    }

    if (!profile.mapping || typeof profile.mapping !== 'object' || Array.isArray(profile.mapping)) {
        errors.push('Profile mapping must be an object');
    } else {
        // Проверяем, что все поля в mapping соответствуют схеме сущности
        const entitySchema = ENTITY_SCHEMAS[profile.entity];
        if (entitySchema) {
            Object.keys(profile.mapping).forEach(field => {
                if (!entitySchema.includes(field)) {
                    errors.push(`Field "${field}" is not valid for entity "${profile.entity}". Valid fields: ${entitySchema.join(', ')}`);
                }
            });
        }

        // Проверяем, что значения mapping - строки (названия колонок)
        Object.entries(profile.mapping).forEach(([field, column]) => {
            if (column && typeof column !== 'string') {
                errors.push(`Mapping value for field "${field}" must be a string (column name)`);
            }
        });
    }

    if (profile.description && typeof profile.description === 'string' && profile.description.length > 1000) {
        errors.push('Profile description must not exceed 1000 characters');
    }

    // Проверка дат
    if (profile.createdAt && !isValidISOString(profile.createdAt)) {
        errors.push('Profile createdAt must be a valid ISO string');
    }

    if (profile.updatedAt && !isValidISOString(profile.updatedAt)) {
        errors.push('Profile updatedAt must be a valid ISO string');
    }

    // Проверка usageCount
    if (profile.usageCount !== undefined && profile.usageCount !== null) {
        if (typeof profile.usageCount !== 'number' || profile.usageCount < 0) {
            errors.push('Profile usageCount must be a non-negative number');
        }
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

/**
 * Создание нового Profile с валидацией
 * @param {Object} profileData - данные для создания Profile
 * @param {string} profileId - ID профиля (если не указан, будет сгенерирован)
 * @returns {Object} валидированный Profile объект
 */
export function createProfile(profileData, profileId = null) {
    const profile = {
        ...createDefaultProfile(),
        ...profileData
    };

    if (profileId) {
        profile.id = profileId;
    }

    // Обновляем даты
    if (!profile.createdAt) {
        profile.createdAt = new Date().toISOString();
    }
    profile.updatedAt = new Date().toISOString();

    // Инициализируем usageCount, если не указан
    if (profile.usageCount === undefined || profile.usageCount === null) {
        profile.usageCount = 0;
    }

    // Валидация
    const validation = validateProfile(profile);
    if (!validation.valid) {
        throw new Error(`Invalid Profile: ${validation.errors.join(', ')}`);
    }

    return profile;
}

/**
 * Обновление Profile
 * @param {Object} existingProfile - существующий Profile
 * @param {Object} updates - обновления
 * @returns {Object} обновленный Profile
 */
export function updateProfile(existingProfile, updates) {
    const updatedProfile = {
        ...existingProfile,
        ...updates,
        id: existingProfile.id, // ID не изменяется
        createdAt: existingProfile.createdAt, // createdAt не изменяется
        updatedAt: new Date().toISOString() // Обновляем updatedAt
    };

    // Валидация
    const validation = validateProfile(updatedProfile);
    if (!validation.valid) {
        throw new Error(`Invalid Profile update: ${validation.errors.join(', ')}`);
    }

    return updatedProfile;
}

/**
 * Увеличение счетчика использования профиля
 * @param {Object} profile - профиль
 * @returns {Object} профиль с увеличенным usageCount
 */
export function incrementUsageCount(profile) {
    return {
        ...profile,
        usageCount: (profile.usageCount || 0) + 1,
        updatedAt: new Date().toISOString()
    };
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

