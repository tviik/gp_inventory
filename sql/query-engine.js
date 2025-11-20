/* ============================
   SQL QUERY ENGINE
   ============================
   
   Движок выполнения SQL-запросов.
   Выполняет AST и возвращает результаты.
*/

// ============================
// VALUE HELPERS
// ============================

function getValue(row, column) {
    // Поддержка table.column
    if (column.includes('.')) {
        const [table, col] = column.split('.');
        // Если указана таблица, но в строке нет префикса, используем только колонку
        return row[col] ?? row[column] ?? null;
    }
    return row[column] ?? null;
}

function compareValues(a, b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    
    // Попытка числового сравнения
    const na = parseFloat(a);
    const nb = parseFloat(b);
    if (!Number.isNaN(na) && !Number.isNaN(nb)) {
        return na - nb;
    }
    
    // Строковое сравнение
    return String(a).localeCompare(String(b));
}

// ============================
// WHERE CONDITIONS
// ============================

function evaluateCondition(row, condition) {
    if (!condition) return true;
    
    switch (condition.type) {
        case 'AND':
            return evaluateCondition(row, condition.left) && 
                   evaluateCondition(row, condition.right);
        
        case 'OR':
            return evaluateCondition(row, condition.left) || 
                   evaluateCondition(row, condition.right);
        
        case 'NOT':
            return !evaluateCondition(row, condition.condition);
        
        case '=':
        case '==':
            return getValue(row, condition.column) == condition.value;
        
        case '!=':
        case '<>':
            return getValue(row, condition.column) != condition.value;
        
        case '>':
            return compareValues(getValue(row, condition.column), condition.value) > 0;
        
        case '>=':
            return compareValues(getValue(row, condition.column), condition.value) >= 0;
        
        case '<':
            return compareValues(getValue(row, condition.column), condition.value) < 0;
        
        case '<=':
            return compareValues(getValue(row, condition.column), condition.value) <= 0;
        
        case 'LIKE':
            const value = String(getValue(row, condition.column) || '').toLowerCase();
            const pattern = String(condition.value || '').toLowerCase()
                .replace(/%/g, '.*')
                .replace(/_/g, '.');
            const regex = new RegExp(`^${pattern}$`);
            return regex.test(value);
        
        case 'IN':
            const colValue = getValue(row, condition.column);
            return Array.isArray(condition.value) && condition.value.includes(colValue);
        
        default:
            console.warn(`Unknown condition type: ${condition.type}`);
            return true;
    }
}

// ============================
// SELECT COLUMNS
// ============================

function selectColumns(rows, columns, hasGroupBy = false) {
    if (columns.length === 1 && columns[0] === '*') {
        return rows;
    }
    
    // Если есть агрегации без GROUP BY, вычисляем их для всех строк
    const hasAggregations = columns.some(col => 
        typeof col === 'object' && col.type && ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(col.type)
    );
    
    if (hasAggregations && !hasGroupBy) {
        // Вычисляем агрегации для всех строк
        const result = {};
        columns.forEach(col => {
            if (typeof col === 'object' && col.type) {
                const alias = col.alias || `${col.type}_${col.column || '*'}`;
                switch (col.type) {
                    case 'COUNT':
                        result[alias] = col.column === '*' ? rows.length : 
                            rows.filter(r => getValue(r, col.column) != null).length;
                        break;
                    case 'SUM':
                        result[alias] = rows.reduce((sum, r) => {
                            const val = parseFloat(getValue(r, col.column));
                            return sum + (Number.isNaN(val) ? 0 : val);
                        }, 0);
                        break;
                    case 'AVG':
                        const sum = rows.reduce((s, r) => {
                            const val = parseFloat(getValue(r, col.column));
                            return s + (Number.isNaN(val) ? 0 : val);
                        }, 0);
                        result[alias] = rows.length > 0 ? sum / rows.length : 0;
                        break;
                    case 'MIN':
                        const minVals = rows.map(r => getValue(r, col.column)).filter(v => v != null);
                        result[alias] = minVals.length > 0 ? Math.min(...minVals.map(v => parseFloat(v) || v)) : null;
                        break;
                    case 'MAX':
                        const maxVals = rows.map(r => getValue(r, col.column)).filter(v => v != null);
                        result[alias] = maxVals.length > 0 ? Math.max(...maxVals.map(v => parseFloat(v) || v)) : null;
                        break;
                }
            } else if (typeof col === 'string') {
                result[col] = rows.length > 0 ? getValue(rows[0], col) : null;
            } else if (typeof col === 'object' && col.column) {
                const alias = col.alias || col.column;
                result[alias] = rows.length > 0 ? getValue(rows[0], col.column) : null;
            }
        });
        return [result];
    }
    
    // Обычный SELECT
    return rows.map(row => {
        const result = {};
        
        columns.forEach(col => {
            if (typeof col === 'string') {
                const value = getValue(row, col);
                result[col] = value;
            } else if (typeof col === 'object') {
                if (col.type) {
                    // Агрегация (должна быть обработана в GROUP BY)
                    const alias = col.alias || `${col.type}_${col.column || '*'}`;
                    result[alias] = getValue(row, alias);
                } else {
                    // Alias
                    const colName = col.column || col;
                    const alias = col.alias || colName;
                    result[alias] = getValue(row, colName);
                }
            }
        });
        
        return result;
    });
}

// ============================
// ORDER BY
// ============================

function sortRows(rows, orderBy) {
    if (!orderBy || orderBy.length === 0) {
        return rows;
    }
    
    return rows.slice().sort((a, b) => {
        for (const order of orderBy) {
            const aVal = getValue(a, order.column);
            const bVal = getValue(b, order.column);
            const comparison = compareValues(aVal, bVal);
            
            if (comparison !== 0) {
                return order.direction === 'DESC' ? -comparison : comparison;
            }
        }
        return 0;
    });
}

// ============================
// GROUP BY & AGGREGATIONS
// ============================

function getGroupKey(row, groupBy) {
    return groupBy.map(col => String(getValue(row, col) || '')).join('::');
}

function aggregate(group, groupByColumns, selectColumns) {
    const result = {};
    
    // Копируем ключи группировки
    groupByColumns.forEach(col => {
        result[col] = getValue(group[0], col);
    });
    
    // Вычисляем агрегации
    selectColumns.forEach(col => {
        if (typeof col === 'object' && col.type) {
            const alias = col.alias || `${col.type}_${col.column || '*'}`;
            
            switch (col.type) {
                case 'COUNT':
                    if (col.column === '*') {
                        result[alias] = group.length;
                    } else {
                        result[alias] = group.filter(r => getValue(r, col.column) != null).length;
                    }
                    break;
                
                case 'SUM':
                    result[alias] = group.reduce((sum, r) => {
                        const val = parseFloat(getValue(r, col.column));
                        return sum + (Number.isNaN(val) ? 0 : val);
                    }, 0);
                    break;
                
                case 'AVG':
                    const sum = group.reduce((s, r) => {
                        const val = parseFloat(getValue(r, col.column));
                        return s + (Number.isNaN(val) ? 0 : val);
                    }, 0);
                    result[alias] = group.length > 0 ? sum / group.length : 0;
                    break;
                
                case 'MIN':
                    const minVals = group.map(r => getValue(r, col.column)).filter(v => v != null);
                    result[alias] = minVals.length > 0 ? Math.min(...minVals.map(v => parseFloat(v) || v)) : null;
                    break;
                
                case 'MAX':
                    const maxVals = group.map(r => getValue(r, col.column)).filter(v => v != null);
                    result[alias] = maxVals.length > 0 ? Math.max(...maxVals.map(v => parseFloat(v) || v)) : null;
                    break;
            }
        }
    });
    
    return result;
}

function groupBy(rows, groupBy, columns) {
    if (!groupBy || groupBy.length === 0) {
        return rows;
    }
    
    const groups = new Map();
    
    rows.forEach(row => {
        const key = getGroupKey(row, groupBy);
        if (!groups.has(key)) {
            groups.set(key, []);
        }
        groups.get(key).push(row);
    });
    
    const results = [];
    groups.forEach(group => {
        results.push(aggregate(group, groupBy, columns));
    });
    
    return results;
}

// ============================
// JOIN
// ============================

function performJoin(leftRows, rightRows, joinSpec) {
    if (!joinSpec) {
        return leftRows;
    }
    
    const { on, type = 'INNER' } = joinSpec;
    const [leftTable, leftCol] = on.left.includes('.') ? on.left.split('.') : [null, on.left];
    const [rightTable, rightCol] = on.right.includes('.') ? on.right.split('.') : [null, on.right];
    
    const result = [];
    
    if (type === 'INNER' || type === 'LEFT') {
        leftRows.forEach(leftRow => {
            const leftValue = getValue(leftRow, leftCol);
            const matches = rightRows.filter(rightRow => {
                const rightValue = getValue(rightRow, rightCol);
                return leftValue == rightValue;
            });
            
            if (matches.length > 0) {
                matches.forEach(match => {
                    result.push({ ...leftRow, ...match });
                });
            } else if (type === 'LEFT') {
                // LEFT JOIN: добавляем даже без совпадений
                const rightEmpty = {};
                rightRows[0] && Object.keys(rightRows[0]).forEach(key => {
                    rightEmpty[key] = null;
                });
                result.push({ ...leftRow, ...rightEmpty });
            }
        });
    }
    
    return result;
}

// ============================
// QUERY EXECUTION
// ============================

/**
 * Выполнение SQL-запроса
 * @param {Object} ast - AST запроса
 * @param {Array} data - данные для запроса (массив объектов)
 * @param {Object} options - опции { rightData?, rightTable? }
 * @returns {Array} результаты запроса
 */
export function executeQuery(ast, data, options = {}) {
    if (!data || !Array.isArray(data) || data.length === 0) {
        return [];
    }
    
    let rows = data.slice();
    
    // JOIN
    if (ast.join && options.rightData) {
        rows = performJoin(rows, options.rightData, ast.join);
    }
    
    // WHERE
    if (ast.where) {
        rows = rows.filter(row => evaluateCondition(row, ast.where));
    }
    
    // GROUP BY
    if (ast.groupBy && ast.groupBy.length > 0) {
        rows = groupBy(rows, ast.groupBy, ast.columns);
    } else {
        // SELECT (с проверкой на агрегации)
        rows = selectColumns(rows, ast.columns, false);
    }
    
    // ORDER BY
    if (ast.orderBy) {
        rows = sortRows(rows, ast.orderBy);
    }
    
    // LIMIT
    if (ast.limit) {
        rows = rows.slice(0, ast.limit);
    }
    
    return rows;
}

export default { executeQuery };

