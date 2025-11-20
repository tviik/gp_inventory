/* ============================
   DASHBOARD ENGINE
   ============================
   
   Движок для сбора статистики и аналитики по инвентарю и версиям.
*/

// ============================
// STATISTICS COLLECTION
// ============================

/**
 * Сбор общей статистики по инвентарю
 * @param {Object} inventory - данные инвентаря
 * @returns {Object} статистика
 */
export function collectOverviewStats(inventory) {
    return {
        environments: (inventory.environments || []).length,
        hosts: (inventory.hosts || []).length,
        services: (inventory.services || []).length,
        endpoints: (inventory.endpoints || []).length,
        snapshots: (inventory.snapshots || []).length
    };
}

/**
 * Распределение версий по окружениям
 * @param {Object} inventory - данные инвентаря
 * @returns {Object} распределение { envName: { version: count } }
 */
export function getVersionDistributionByEnvironment(inventory) {
    const distribution = {};
    
    const environments = inventory.environments || [];
    const endpoints = inventory.endpoints || [];
    const snapshots = inventory.snapshots || [];
    
    // Создаем маппинг endpointId -> envId
    const endpointToEnv = {};
    endpoints.forEach(ep => {
        endpointToEnv[ep.id] = ep.envId;
    });
    
    // Создаем маппинг envId -> envName
    const envIdToName = {};
    environments.forEach(env => {
        envIdToName[env.id] = env.name;
    });
    
    // Собираем версии по окружениям
    snapshots.forEach(snapshot => {
        const envId = endpointToEnv[snapshot.endpointId];
        if (!envId) return;
        
        const envName = envIdToName[envId] || envId;
        const version = snapshot.version || 'unknown';
        
        if (!distribution[envName]) {
            distribution[envName] = {};
        }
        
        if (!distribution[envName][version]) {
            distribution[envName][version] = 0;
        }
        
        distribution[envName][version]++;
    });
    
    return distribution;
}

/**
 * Распределение версий по сервисам
 * @param {Object} inventory - данные инвентаря
 * @returns {Object} распределение { serviceName: { version: count } }
 */
export function getVersionDistributionByService(inventory) {
    const distribution = {};
    
    const services = inventory.services || [];
    const endpoints = inventory.endpoints || [];
    const snapshots = inventory.snapshots || [];
    
    // Создаем маппинг endpointId -> serviceId
    const endpointToService = {};
    endpoints.forEach(ep => {
        endpointToService[ep.id] = ep.serviceId;
    });
    
    // Создаем маппинг serviceId -> serviceName
    const serviceIdToName = {};
    services.forEach(svc => {
        serviceIdToName[svc.id] = svc.name;
    });
    
    // Собираем версии по сервисам
    snapshots.forEach(snapshot => {
        const serviceId = endpointToService[snapshot.endpointId];
        if (!serviceId) return;
        
        const serviceName = serviceIdToName[serviceId] || serviceId;
        const version = snapshot.version || 'unknown';
        
        if (!distribution[serviceName]) {
            distribution[serviceName] = {};
        }
        
        if (!distribution[serviceName][version]) {
            distribution[serviceName][version] = 0;
        }
        
        distribution[serviceName][version]++;
    });
    
    return distribution;
}

/**
 * Выявление аномалий
 * @param {Object} inventory - данные инвентаря
 * @param {Object} options - опции { outdatedDays: 30 }
 * @returns {Object} аномалии
 */
export function detectAnomalies(inventory, options = {}) {
    const outdatedDays = options.outdatedDays || 30;
    const now = Date.now();
    const outdatedThreshold = now - (outdatedDays * 24 * 60 * 60 * 1000);
    
    const anomalies = {
        outdated: [],
        inconsistent: [],
        missing: []
    };
    
    const services = inventory.services || [];
    const endpoints = inventory.endpoints || [];
    const snapshots = inventory.snapshots || [];
    const environments = inventory.environments || [];
    
    // Создаем маппинги
    const endpointToService = {};
    const endpointToEnv = {};
    const serviceIdToName = {};
    const envIdToName = {};
    
    endpoints.forEach(ep => {
        endpointToService[ep.id] = ep.serviceId;
        endpointToEnv[ep.id] = ep.envId;
    });
    
    services.forEach(svc => {
        serviceIdToName[svc.id] = svc.name;
    });
    
    environments.forEach(env => {
        envIdToName[env.id] = env.name;
    });
    
    // Группируем snapshots по endpointId
    const snapshotsByEndpoint = {};
    snapshots.forEach(snapshot => {
        if (!snapshotsByEndpoint[snapshot.endpointId]) {
            snapshotsByEndpoint[snapshot.endpointId] = [];
        }
        snapshotsByEndpoint[snapshot.endpointId].push(snapshot);
    });
    
    // Проверяем отстающие версии
    snapshots.forEach(snapshot => {
        const timestamp = snapshot.timestamp ? new Date(snapshot.timestamp).getTime() : 0;
        if (timestamp > 0 && timestamp < outdatedThreshold) {
            const serviceId = endpointToService[snapshot.endpointId];
            const envId = endpointToEnv[snapshot.endpointId];
            
            anomalies.outdated.push({
                service: serviceIdToName[serviceId] || serviceId,
                environment: envIdToName[envId] || envId,
                version: snapshot.version || 'unknown',
                lastUpdate: snapshot.timestamp,
                daysAgo: Math.floor((now - timestamp) / (24 * 60 * 60 * 1000))
            });
        }
    });
    
    // Проверяем отсутствующие версии
    endpoints.forEach(endpoint => {
        if (!snapshotsByEndpoint[endpoint.id] || snapshotsByEndpoint[endpoint.id].length === 0) {
            const serviceId = endpointToService[endpoint.id];
            const envId = endpointToEnv[endpoint.id];
            
            anomalies.missing.push({
                service: serviceIdToName[serviceId] || serviceId,
                environment: envIdToName[envId] || envId,
                endpoint: endpoint.url || endpoint.id
            });
        }
    });
    
    // Проверяем несоответствия версий
    // Находим самую частую версию для каждого сервиса
    const serviceVersions = {};
    services.forEach(service => {
        const serviceEndpoints = endpoints.filter(ep => ep.serviceId === service.id);
        const versions = {};
        
        serviceEndpoints.forEach(ep => {
            const epSnapshots = snapshotsByEndpoint[ep.id] || [];
            epSnapshots.forEach(snapshot => {
                const version = snapshot.version || 'unknown';
                versions[version] = (versions[version] || 0) + 1;
            });
        });
        
        if (Object.keys(versions).length > 0) {
            const mostCommonVersion = Object.keys(versions).reduce((a, b) => 
                versions[a] > versions[b] ? a : b
            );
            
            serviceVersions[service.id] = {
                mostCommon: mostCommonVersion,
                all: versions
            };
        }
    });
    
    // Находим endpoints с версиями, отличающимися от наиболее частой
    snapshots.forEach(snapshot => {
        const serviceId = endpointToService[snapshot.endpointId];
        if (!serviceId || !serviceVersions[serviceId]) return;
        
        const version = snapshot.version || 'unknown';
        const mostCommon = serviceVersions[serviceId].mostCommon;
        
        if (version !== mostCommon) {
            const envId = endpointToEnv[snapshot.endpointId];
            
            anomalies.inconsistent.push({
                service: serviceIdToName[serviceId] || serviceId,
                environment: envIdToName[envId] || envId,
                version: version,
                expected: mostCommon
            });
        }
    });
    
    // Удаляем дубликаты
    anomalies.outdated = removeDuplicates(anomalies.outdated, ['service', 'environment', 'version']);
    anomalies.inconsistent = removeDuplicates(anomalies.inconsistent, ['service', 'environment', 'version']);
    anomalies.missing = removeDuplicates(anomalies.missing, ['service', 'environment', 'endpoint']);
    
    return anomalies;
}

/**
 * История изменений версий
 * @param {Object} inventory - данные инвентаря
 * @returns {Array} история изменений
 */
export function getVersionHistory(inventory) {
    const snapshots = inventory.snapshots || [];
    const endpoints = inventory.endpoints || [];
    const services = inventory.services || [];
    const environments = inventory.environments || [];
    
    // Создаем маппинги
    const endpointToService = {};
    const endpointToEnv = {};
    const serviceIdToName = {};
    const envIdToName = {};
    
    endpoints.forEach(ep => {
        endpointToService[ep.id] = ep.serviceId;
        endpointToEnv[ep.id] = ep.envId;
    });
    
    services.forEach(svc => {
        serviceIdToName[svc.id] = svc.name;
    });
    
    environments.forEach(env => {
        envIdToName[env.id] = env.name;
    });
    
    // Сортируем snapshots по времени
    const sortedSnapshots = snapshots
        .filter(s => s.timestamp)
        .map(s => ({
            ...s,
            timestamp: new Date(s.timestamp).getTime()
        }))
        .sort((a, b) => b.timestamp - a.timestamp);
    
    // Группируем по дате
    const history = [];
    sortedSnapshots.forEach(snapshot => {
        const serviceId = endpointToService[snapshot.endpointId];
        const envId = endpointToEnv[snapshot.endpointId];
        const date = new Date(snapshot.timestamp);
        
        history.push({
            date: date.toISOString().split('T')[0],
            timestamp: snapshot.timestamp,
            service: serviceIdToName[serviceId] || serviceId,
            environment: envIdToName[envId] || envId,
            version: snapshot.version || 'unknown',
            build: snapshot.build || ''
        });
    });
    
    return history;
}

// ============================
// HELPERS
// ============================

function removeDuplicates(array, keys) {
    const seen = new Set();
    return array.filter(item => {
        const key = keys.map(k => item[k]).join('::');
        if (seen.has(key)) {
            return false;
        }
        seen.add(key);
        return true;
    });
}

// ============================
// PUBLIC API
// ============================

export const dashboardEngine = {
    collectOverviewStats,
    getVersionDistributionByEnvironment,
    getVersionDistributionByService,
    detectAnomalies,
    getVersionHistory
};

export default dashboardEngine;

