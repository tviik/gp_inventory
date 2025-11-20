/* ============================
   MIGRATION FROM LOCALSTORAGE
   ============================
   
   Миграция данных из localStorage в IndexedDB.
   Выполняется автоматически при первом запуске.
*/

import { STORES, ENTITIES, LEGACY_KEYS, MIGRATION_FLAG } from './constants.js';
import { createInventoryKey, generateId } from './indexeddb.js';

// ============================
// MIGRATION STATUS
// ============================

/**
 * Проверка, выполнена ли миграция
 */
export function isMigrationCompleted() {
    const flag = localStorage.getItem(MIGRATION_FLAG);
    return flag === 'true';
}

/**
 * Отметка о выполнении миграции
 */
export function markMigrationCompleted() {
    localStorage.setItem(MIGRATION_FLAG, 'true');
}

// ============================
// MIGRATION FUNCTIONS
// ============================

/**
 * Миграция инвентаря из localStorage
 */
async function migrateInventory(db) {
    const raw = localStorage.getItem(LEGACY_KEYS.INVENTORY);
    if (!raw) {
        console.log('[Migration] No inventory data in localStorage');
        return { migrated: 0, errors: 0 };
    }

    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (e) {
        console.error('[Migration] Failed to parse inventory data:', e);
        return { migrated: 0, errors: 1 };
    }

    const transaction = db.transaction([STORES.INVENTORY], 'readwrite');
    const store = transaction.objectStore(STORES.INVENTORY);

    let migrated = 0;
    let errors = 0;

    // Миграция каждой сущности
    for (const entity of Object.values(ENTITIES)) {
        const items = parsed[entity];
        if (!Array.isArray(items)) {
            continue;
        }

        for (const item of items) {
            if (!item || !item.id) {
                console.warn(`[Migration] Skipping invalid item in ${entity}:`, item);
                continue;
            }

            try {
                const key = createInventoryKey(entity, item.id);
                const record = {
                    id: key,
                    entity: entity,
                    data: item
                };

                await new Promise((resolve, reject) => {
                    const request = store.put(record);
                    request.onsuccess = () => resolve();
                    request.onerror = () => reject(request.error);
                });

                migrated++;
            } catch (error) {
                console.error(`[Migration] Error migrating ${entity} item ${item.id}:`, error);
                errors++;
            }
        }
    }

    return { migrated, errors };
}

/**
 * Миграция Version Matrix из localStorage
 */
async function migrateMatrix(db) {
    const transaction = db.transaction([STORES.MATRIX], 'readwrite');
    const store = transaction.objectStore(STORES.MATRIX);

    let migrated = 0;
    let errors = 0;

    // Перебираем все ключи localStorage
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (!key || !key.startsWith(LEGACY_KEYS.MATRIX_PREFIX)) {
            continue;
        }

        const cellId = key.substring(LEGACY_KEYS.MATRIX_PREFIX.length);
        const value = localStorage.getItem(key);

        if (!value) {
            continue;
        }

        try {
            const record = {
                id: cellId,
                value: value
            };

            await new Promise((resolve, reject) => {
                const request = store.put(record);
                request.onsuccess = () => resolve();
                request.onerror = () => reject(request.error);
            });

            migrated++;
        } catch (error) {
            console.error(`[Migration] Error migrating matrix cell ${cellId}:`, error);
            errors++;
        }
    }

    return { migrated, errors };
}

/**
 * Основная функция миграции
 * @param {IDBDatabase} db - экземпляр IndexedDB
 * @returns {Promise<Object>} результат миграции
 */
export async function migrateFromLocalStorage(db) {
    if (!db) {
        throw new Error('Database instance is required for migration');
    }

    if (isMigrationCompleted()) {
        console.log('[Migration] Migration already completed, skipping');
        return {
            completed: true,
            skipped: true,
            inventory: { migrated: 0, errors: 0 },
            matrix: { migrated: 0, errors: 0 }
        };
    }

    console.log('[Migration] Starting migration from localStorage to IndexedDB...');

    const results = {
        completed: false,
        skipped: false,
        inventory: { migrated: 0, errors: 0 },
        matrix: { migrated: 0, errors: 0 }
    };

    try {
        // Миграция инвентаря
        console.log('[Migration] Migrating inventory...');
        results.inventory = await migrateInventory(db);
        console.log(`[Migration] Inventory: ${results.inventory.migrated} items migrated, ${results.inventory.errors} errors`);

        // Миграция матрицы
        console.log('[Migration] Migrating version matrix...');
        results.matrix = await migrateMatrix(db);
        console.log(`[Migration] Matrix: ${results.matrix.migrated} cells migrated, ${results.matrix.errors} errors`);

        // Отмечаем миграцию как выполненную
        markMigrationCompleted();
        results.completed = true;

        const totalMigrated = results.inventory.migrated + results.matrix.migrated;
        const totalErrors = results.inventory.errors + results.matrix.errors;

        console.log(`[Migration] Migration completed: ${totalMigrated} items migrated, ${totalErrors} errors`);

        return results;
    } catch (error) {
        console.error('[Migration] Migration failed:', error);
        results.completed = false;
        results.error = error.message;
        return results;
    }
}

/**
 * Проверка наличия данных для миграции
 */
export function hasDataToMigrate() {
    // Проверяем инвентарь
    const inventory = localStorage.getItem(LEGACY_KEYS.INVENTORY);
    if (inventory) {
        try {
            const parsed = JSON.parse(inventory);
            const hasData = Object.values(ENTITIES).some(entity => {
                const items = parsed[entity];
                return Array.isArray(items) && items.length > 0;
            });
            if (hasData) {
                return true;
            }
        } catch (e) {
            // Игнорируем ошибки парсинга
        }
    }

    // Проверяем матрицу
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith(LEGACY_KEYS.MATRIX_PREFIX)) {
            return true;
        }
    }

    return false;
}

