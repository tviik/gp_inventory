/* ============================
   CREDENTIAL MODEL (v0.11)
   ============================
   
   Модель данных для креденшалов.
   Валидация, создание, обновление Credential объектов.
   Шифрование паролей и SSH ключей.
*/

import { encryptData, decryptData, isCryptoAvailable } from '../security/crypto-utils.js';

// ============================
// SCHEMA
// ============================

/**
 * Схема Credential
 */
export const CREDENTIAL_SCHEMA = {
    id: { required: true, type: 'string' },
    name: { required: true, type: 'string', maxLength: 200 },
    type: { required: true, type: 'string', enum: ['technical', 'user'] },
    authType: { required: true, type: 'string', enum: ['password', 'ssh'] },
    username: { required: true, type: 'string', maxLength: 200 },
    password: { required: false, type: 'string' }, // зашифрован
    sshKey: { required: false, type: 'string' }, // зашифрован
    sshKeyPassphrase: { required: false, type: 'string' }, // зашифрован
    description: { required: false, type: 'string', maxLength: 1000 },
    createdAt: { required: true, type: 'string' },
    updatedAt: { required: true, type: 'string' },
    lastUsedAt: { required: false, type: 'string' },
    usageCount: { required: false, type: 'number' }
};

/**
 * Значения по умолчанию для Credential
 */
export function createDefaultCredential() {
    return {
        name: '',
        type: 'technical',
        authType: 'password',
        username: '',
        password: null,
        sshKey: null,
        sshKeyPassphrase: null,
        description: '',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        lastUsedAt: null,
        usageCount: 0
    };
}

/**
 * Валидация Credential
 * @param {Object} credential - объект Credential для валидации
 * @returns {Object} { valid: boolean, errors: Array<string> }
 */
export function validateCredential(credential) {
    const errors = [];

    // Проверка обязательных полей
    if (!credential.id || typeof credential.id !== 'string') {
        errors.push('ID обязателен и должен быть строкой');
    }

    if (!credential.name || typeof credential.name !== 'string' || credential.name.trim().length === 0) {
        errors.push('Название обязательно');
    }

    if (credential.name && credential.name.length > CREDENTIAL_SCHEMA.name.maxLength) {
        errors.push(`Название не должно превышать ${CREDENTIAL_SCHEMA.name.maxLength} символов`);
    }

    if (!credential.type || !CREDENTIAL_SCHEMA.type.enum.includes(credential.type)) {
        errors.push(`Тип должен быть одним из: ${CREDENTIAL_SCHEMA.type.enum.join(', ')}`);
    }

    if (!credential.authType || !CREDENTIAL_SCHEMA.authType.enum.includes(credential.authType)) {
        errors.push(`Тип аутентификации должен быть одним из: ${CREDENTIAL_SCHEMA.authType.enum.join(', ')}`);
    }

    if (!credential.username || typeof credential.username !== 'string' || credential.username.trim().length === 0) {
        errors.push('Имя пользователя обязательно');
    }

    // Проверка в зависимости от типа аутентификации
    if (credential.authType === 'password') {
        if (!credential.password || credential.password.trim().length === 0) {
            errors.push('Пароль обязателен для типа аутентификации "password"');
        }
    } else if (credential.authType === 'ssh') {
        if (!credential.sshKey || credential.sshKey.trim().length === 0) {
            errors.push('SSH ключ обязателен для типа аутентификации "ssh"');
        }
    }

    if (credential.description && credential.description.length > CREDENTIAL_SCHEMA.description.maxLength) {
        errors.push(`Описание не должно превышать ${CREDENTIAL_SCHEMA.description.maxLength} символов`);
    }

    if (!credential.createdAt || typeof credential.createdAt !== 'string') {
        errors.push('createdAt обязателен и должен быть ISO строкой');
    }

    if (!credential.updatedAt || typeof credential.updatedAt !== 'string') {
        errors.push('updatedAt обязателен и должен быть ISO строкой');
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

/**
 * Создание Credential с шифрованием паролей
 * @param {Object} data - данные креденшала (пароли в открытом виде)
 * @returns {Promise<Object>} созданный Credential (пароли зашифрованы)
 */
export async function createCredential(data) {
    if (!isCryptoAvailable()) {
        throw new Error('Web Crypto API не доступен. Шифрование невозможно.');
    }

    const credential = {
        ...createDefaultCredential(),
        ...data,
        id: data.id || `cred_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        createdAt: data.createdAt || new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };

    // Шифруем пароли и SSH ключи
    if (credential.password && !credential.password.startsWith('encrypted:')) {
        // Проверяем, не зашифрован ли уже
        credential.password = 'encrypted:' + await encryptData(credential.password);
    }

    if (credential.sshKey && !credential.sshKey.startsWith('encrypted:')) {
        credential.sshKey = 'encrypted:' + await encryptData(credential.sshKey);
    }

    if (credential.sshKeyPassphrase && !credential.sshKeyPassphrase.startsWith('encrypted:')) {
        credential.sshKeyPassphrase = 'encrypted:' + await encryptData(credential.sshKeyPassphrase);
    }

    const validation = validateCredential(credential);
    if (!validation.valid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
    }

    return credential;
}

/**
 * Обновление Credential с шифрованием паролей
 * @param {Object} credential - существующий Credential
 * @param {Object} updates - обновления (пароли в открытом виде, если изменяются)
 * @returns {Promise<Object>} обновленный Credential
 */
export async function updateCredential(credential, updates) {
    if (!isCryptoAvailable()) {
        throw new Error('Web Crypto API не доступен. Шифрование невозможно.');
    }

    const updated = {
        ...credential,
        ...updates,
        updatedAt: new Date().toISOString()
    };

    // Шифруем пароли и SSH ключи, если они изменяются и еще не зашифрованы
    if (updates.password !== undefined) {
        if (updates.password && !updates.password.startsWith('encrypted:')) {
            updated.password = 'encrypted:' + await encryptData(updates.password);
        } else if (updates.password === null || updates.password === '') {
            updated.password = null;
        } else {
            // Уже зашифрован, оставляем как есть
            updated.password = updates.password;
        }
    }

    if (updates.sshKey !== undefined) {
        if (updates.sshKey && !updates.sshKey.startsWith('encrypted:')) {
            updated.sshKey = 'encrypted:' + await encryptData(updates.sshKey);
        } else if (updates.sshKey === null || updates.sshKey === '') {
            updated.sshKey = null;
        } else {
            updated.sshKey = updates.sshKey;
        }
    }

    if (updates.sshKeyPassphrase !== undefined) {
        if (updates.sshKeyPassphrase && !updates.sshKeyPassphrase.startsWith('encrypted:')) {
            updated.sshKeyPassphrase = 'encrypted:' + await encryptData(updates.sshKeyPassphrase);
        } else if (updates.sshKeyPassphrase === null || updates.sshKeyPassphrase === '') {
            updated.sshKeyPassphrase = null;
        } else {
            updated.sshKeyPassphrase = updates.sshKeyPassphrase;
        }
    }

    const validation = validateCredential(updated);
    if (!validation.valid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
    }

    return updated;
}

/**
 * Расшифровка Credential для использования
 * @param {Object} credential - зашифрованный Credential
 * @returns {Promise<Object>} Credential с расшифрованными паролями
 */
export async function decryptCredential(credential) {
    if (!isCryptoAvailable()) {
        throw new Error('Web Crypto API не доступен. Расшифровка невозможна.');
    }

    const decrypted = { ...credential };

    if (decrypted.password && decrypted.password.startsWith('encrypted:')) {
        const encrypted = decrypted.password.substring('encrypted:'.length);
        decrypted.password = await decryptData(encrypted);
    }

    if (decrypted.sshKey && decrypted.sshKey.startsWith('encrypted:')) {
        const encrypted = decrypted.sshKey.substring('encrypted:'.length);
        decrypted.sshKey = await decryptData(encrypted);
    }

    if (decrypted.sshKeyPassphrase && decrypted.sshKeyPassphrase.startsWith('encrypted:')) {
        const encrypted = decrypted.sshKeyPassphrase.substring('encrypted:'.length);
        decrypted.sshKeyPassphrase = await decryptData(encrypted);
    }

    return decrypted;
}

/**
 * Увеличение счетчика использования
 * @param {Object} credential - Credential
 * @returns {Object} обновленный Credential
 */
export function incrementUsageCount(credential) {
    return {
        ...credential,
        usageCount: (credential.usageCount || 0) + 1,
        lastUsedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };
}

