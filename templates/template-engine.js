/* ============================
   TEMPLATE ENGINE
   ============================
   
   Движок для применения шаблонов к данным.
   Поддержка различных типов шаблонов: ssh, curl, sql, ansible, zabbix, bash, powershell.
*/

// ============================
// TEMPLATE PROCESSING
// ============================

/**
 * Извлечение переменных из шаблона
 * @param {string} template - шаблон с плейсхолдерами {variable}
 * @returns {Array<string>} массив имен переменных
 */
export function extractVariables(template) {
    if (!template || typeof template !== 'string') {
        return [];
    }

    const regex = /\{([^}]+)\}/g;
    const variables = [];
    let match;

    while ((match = regex.exec(template)) !== null) {
        const varName = match[1].trim();
        if (varName && !variables.includes(varName)) {
            variables.push(varName);
        }
    }

    return variables;
}

/**
 * Применение шаблона к одной строке данных
 * @param {string} template - шаблон
 * @param {Object} row - данные строки (Record<string, any>)
 * @returns {string} результат подстановки
 */
export function applyTemplate(template, row) {
    if (!template || typeof template !== 'string') {
        return '';
    }

    return template.replace(/\{([^}]+)\}/g, (match, varName) => {
        const key = varName.trim();
        const value = row[key];

        if (value === undefined || value === null) {
            return ''; // Пустая строка для отсутствующих значений
        }

        return String(value);
    });
}

/**
 * Применение шаблона к массиву строк
 * @param {string} template - шаблон
 * @param {Array<Object>} rows - массив строк данных
 * @returns {Array<string>} массив результатов
 */
export function applyTemplateToRows(template, rows) {
    if (!Array.isArray(rows)) {
        return [];
    }

    return rows.map(row => applyTemplate(template, row));
}

/**
 * Валидация шаблона
 * @param {string} template - шаблон для проверки
 * @returns {Object} { valid: boolean, errors: Array<string> }
 */
export function validateTemplate(template) {
    const errors = [];

    if (!template || typeof template !== 'string') {
        errors.push('Шаблон должен быть непустой строкой');
        return { valid: false, errors };
    }

    if (template.trim().length === 0) {
        errors.push('Шаблон не может быть пустым');
        return { valid: false, errors };
    }

    // Проверка на незакрытые фигурные скобки
    const openBraces = (template.match(/\{/g) || []).length;
    const closeBraces = (template.match(/\}/g) || []).length;

    if (openBraces !== closeBraces) {
        errors.push('Несоответствие открывающих и закрывающих фигурных скобок');
    }

    // Проверка на пустые плейсхолдеры
    const emptyPlaceholders = template.match(/\{\s*\}/g);
    if (emptyPlaceholders) {
        errors.push('Найдены пустые плейсхолдеры {}');
    }

    return {
        valid: errors.length === 0,
        errors
    };
}

// ============================
// TEMPLATE TYPES
// ============================

export const TEMPLATE_TYPES = {
    SSH: 'ssh',
    CURL: 'curl',
    SQL: 'sql',
    ANSIBLE: 'ansible',
    ZABBIX: 'zabbix',
    BASH: 'bash',
    POWERSHELL: 'powershell',
    GENERIC: 'generic'
};

export const TEMPLATE_CATEGORIES = {
    COMMANDS: 'commands',
    SCRIPTS: 'scripts',
    CONFIGS: 'configs'
};

/**
 * Определение типа шаблона по содержимому
 * @param {string} template - шаблон
 * @returns {string} тип шаблона
 */
export function detectTemplateType(template) {
    if (!template) return TEMPLATE_TYPES.GENERIC;

    const lower = template.toLowerCase();

    if (lower.includes('ssh') || lower.startsWith('ssh ')) {
        return TEMPLATE_TYPES.SSH;
    }
    if (lower.includes('curl') || lower.includes('http://') || lower.includes('https://')) {
        return TEMPLATE_TYPES.CURL;
    }
    if (lower.includes('select') || lower.includes('update') || lower.includes('insert') || lower.includes('delete')) {
        return TEMPLATE_TYPES.SQL;
    }
    if (lower.includes('ansible') || lower.includes('[all]') || lower.includes('hosts:')) {
        return TEMPLATE_TYPES.ANSIBLE;
    }
    if (lower.includes('zabbix') || lower.includes('userparameter') || lower.includes('#!/usr/bin/env bash')) {
        return TEMPLATE_TYPES.ZABBIX;
    }
    if (lower.includes('#!/bin/bash') || lower.includes('#!/usr/bin/env bash')) {
        return TEMPLATE_TYPES.BASH;
    }
    if (lower.includes('powershell') || lower.includes('$')) {
        return TEMPLATE_TYPES.POWERSHELL;
    }

    return TEMPLATE_TYPES.GENERIC;
}

/**
 * Определение категории шаблона
 * @param {string} template - шаблон
 * @returns {string} категория
 */
export function detectTemplateCategory(template) {
    if (!template) return TEMPLATE_CATEGORIES.COMMANDS;

    const lower = template.toLowerCase();

    if (lower.includes('#!/') || lower.includes('function ') || lower.includes('def ')) {
        return TEMPLATE_CATEGORIES.SCRIPTS;
    }
    if (lower.includes('userparameter') || lower.includes('[') && lower.includes(']')) {
        return TEMPLATE_CATEGORIES.CONFIGS;
    }

    return TEMPLATE_CATEGORIES.COMMANDS;
}

// ============================
// TEMPLATE HELPERS
// ============================

/**
 * Форматирование шаблона для отображения
 * @param {string} template - шаблон
 * @returns {string} отформатированный шаблон
 */
export function formatTemplate(template) {
    // Простое форматирование - можно расширить
    return template;
}

/**
 * Создание шаблона из примера
 * @param {string} type - тип шаблона
 * @returns {string} пример шаблона
 */
export function getTemplateExample(type) {
    const examples = {
        [TEMPLATE_TYPES.SSH]: 'ssh {host} -p {port} -l {user}',
        [TEMPLATE_TYPES.CURL]: 'curl -s "http://{ip}:{port}/version" -H "Accept: application/json"',
        [TEMPLATE_TYPES.SQL]: 'UPDATE brokers SET version="{version}" WHERE host="{host}";',
        [TEMPLATE_TYPES.ANSIBLE]: '[{env}]\n{host} ansible_host={ip}',
        [TEMPLATE_TYPES.ZABBIX]: 'UserParameter={item_key}[*],/usr/local/bin/check.sh "$1" "$2"',
        [TEMPLATE_TYPES.BASH]: '#!/bin/bash\nHOST="{host}"\nPORT="{port}"\necho "Checking $HOST:$PORT"',
        [TEMPLATE_TYPES.POWERSHELL]: '$host = "{host}"; $port = {port}; Invoke-WebRequest -Uri "http://$host:$port/version"',
        [TEMPLATE_TYPES.GENERIC]: '{column1} {column2} {column3}'
    };

    return examples[type] || examples[TEMPLATE_TYPES.GENERIC];
}

// ============================
// PUBLIC API
// ============================

export const templateEngine = {
    extractVariables,
    applyTemplate,
    applyTemplateToRows,
    validateTemplate,
    detectTemplateType,
    detectTemplateCategory,
    formatTemplate,
    getTemplateExample
};

export default templateEngine;

