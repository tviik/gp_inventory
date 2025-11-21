/* ============================
   CRYPTO UTILS (v0.11)
   ============================
   
   Утилиты для шифрования данных через Web Crypto API.
   Используется для безопасного хранения креденшалов.
*/

// ============================
// CONSTANTS
// ============================

const MASTER_KEY_STORAGE_KEY = 'credentials_master_key';
const ALGORITHM = 'AES-GCM';
const KEY_LENGTH = 256;
const IV_LENGTH = 12; // 96 bits для GCM

// ============================
// KEY MANAGEMENT
// ============================

/**
 * Генерация ключа шифрования
 * @returns {Promise<CryptoKey>}
 */
export async function generateEncryptionKey() {
    try {
        const key = await crypto.subtle.generateKey(
            {
                name: ALGORITHM,
                length: KEY_LENGTH
            },
            true, // extractable
            ['encrypt', 'decrypt']
        );
        return key;
    } catch (error) {
        console.error('[Crypto] Error generating key:', error);
        throw new Error('Failed to generate encryption key');
    }
}

/**
 * Получение или создание мастер-ключа из sessionStorage
 * @returns {Promise<CryptoKey>}
 */
export async function getOrCreateMasterKey() {
    try {
        // Проверяем, есть ли ключ в sessionStorage
        const keyData = sessionStorage.getItem(MASTER_KEY_STORAGE_KEY);
        
        if (keyData) {
            // Импортируем существующий ключ
            const keyBuffer = Uint8Array.from(JSON.parse(keyData));
            const key = await crypto.subtle.importKey(
                'raw',
                keyBuffer,
                {
                    name: ALGORITHM,
                    length: KEY_LENGTH
                },
                true, // extractable
                ['encrypt', 'decrypt']
            );
            return key;
        } else {
            // Генерируем новый ключ
            const key = await generateEncryptionKey();
            
            // Экспортируем ключ для сохранения
            const exportedKey = await crypto.subtle.exportKey('raw', key);
            const keyArray = Array.from(new Uint8Array(exportedKey));
            
            // Сохраняем в sessionStorage
            sessionStorage.setItem(MASTER_KEY_STORAGE_KEY, JSON.stringify(keyArray));
            
            return key;
        }
    } catch (error) {
        console.error('[Crypto] Error getting/creating master key:', error);
        throw new Error('Failed to get or create master key');
    }
}

/**
 * Очистка мастер-ключа из sessionStorage
 * Используется при выходе или для безопасности
 */
export function clearMasterKey() {
    sessionStorage.removeItem(MASTER_KEY_STORAGE_KEY);
}

// ============================
// ENCRYPTION / DECRYPTION
// ============================

/**
 * Шифрование данных
 * @param {string} data - данные для шифрования
 * @param {CryptoKey} key - ключ шифрования (опционально, если не указан - используется мастер-ключ)
 * @returns {Promise<string>} зашифрованные данные в формате base64
 */
export async function encryptData(data, key = null) {
    if (!data || typeof data !== 'string') {
        throw new Error('Data must be a non-empty string');
    }

    try {
        const encryptionKey = key || await getOrCreateMasterKey();
        
        // Генерируем случайный IV
        const iv = crypto.getRandomValues(new Uint8Array(IV_LENGTH));
        
        // Конвертируем данные в ArrayBuffer
        const dataBuffer = new TextEncoder().encode(data);
        
        // Шифруем
        const encryptedBuffer = await crypto.subtle.encrypt(
            {
                name: ALGORITHM,
                iv: iv
            },
            encryptionKey,
            dataBuffer
        );
        
        // Объединяем IV и зашифрованные данные
        const combined = new Uint8Array(iv.length + encryptedBuffer.byteLength);
        combined.set(iv, 0);
        combined.set(new Uint8Array(encryptedBuffer), iv.length);
        
        // Конвертируем в base64 для хранения
        const base64 = btoa(String.fromCharCode(...combined));
        
        return base64;
    } catch (error) {
        console.error('[Crypto] Error encrypting data:', error);
        throw new Error('Failed to encrypt data');
    }
}

/**
 * Расшифровка данных
 * @param {string} encryptedData - зашифрованные данные в формате base64
 * @param {CryptoKey} key - ключ шифрования (опционально, если не указан - используется мастер-ключ)
 * @returns {Promise<string>} расшифрованные данные
 */
export async function decryptData(encryptedData, key = null) {
    if (!encryptedData || typeof encryptedData !== 'string') {
        throw new Error('Encrypted data must be a non-empty string');
    }

    try {
        const decryptionKey = key || await getOrCreateMasterKey();
        
        // Декодируем из base64
        const combined = Uint8Array.from(atob(encryptedData), c => c.charCodeAt(0));
        
        // Извлекаем IV и зашифрованные данные
        const iv = combined.slice(0, IV_LENGTH);
        const encrypted = combined.slice(IV_LENGTH);
        
        // Расшифровываем
        const decryptedBuffer = await crypto.subtle.decrypt(
            {
                name: ALGORITHM,
                iv: iv
            },
            decryptionKey,
            encrypted
        );
        
        // Конвертируем в строку
        const decrypted = new TextDecoder().decode(decryptedBuffer);
        
        return decrypted;
    } catch (error) {
        console.error('[Crypto] Error decrypting data:', error);
        throw new Error('Failed to decrypt data. Key may be invalid or data corrupted.');
    }
}

/**
 * Проверка доступности Web Crypto API
 * @returns {boolean}
 */
export function isCryptoAvailable() {
    return typeof crypto !== 'undefined' && 
           typeof crypto.subtle !== 'undefined' &&
           typeof crypto.getRandomValues !== 'undefined';
}

