/* ============================
   IMPORTS
============================ */

import {
    init as initIndexedDB,
    isAvailable,
    getDB,
    getInventory,
    saveInventory,
    addInventoryItem,
    updateInventoryItem,
    deleteInventoryItem,
    saveFile,
    getFile,
    listFiles,
    deleteFile,
    saveSheet,
    getSheet,
    listSheets,
    getMatrixCell,
    setMatrixCell,
    getAllMatrix,
    saveTemplate,
    getTemplate,
    listTemplates,
    deleteTemplate,
    getTemplatesByType,
    getTemplatesByCategory
} from './storage/indexeddb.js';
import { migrateFromLocalStorage, hasDataToMigrate } from './storage/migration.js';
import {
    init as initSearchEngine,
    indexSheet,
    indexAllSheets,
    removeSheet,
    search as searchIndex,
    getIndexStats,
    isReady as isSearchReady
} from './search/search-engine.js';
import { parseQuery } from './sql/parser.js';
import { executeQuery } from './sql/query-engine.js';
import {
    extractVariables,
    applyTemplate,
    applyTemplateToRows,
    validateTemplate,
    detectTemplateType,
    detectTemplateCategory,
    getTemplateExample,
    TEMPLATE_TYPES
} from './templates/template-engine.js';
import {
    collectOverviewStats,
    getVersionDistributionByEnvironment,
    getVersionDistributionByService,
    detectAnomalies,
    getVersionHistory
} from './dashboard/dashboard-engine.js';
import {
    renderBarChart,
    renderVerticalBarChart,
    getStatusColor
} from './dashboard/chart-renderer.js';

/* ============================
   SCHEMAS & DEFAULT DATA
============================ */

const schemas = {
    environments: ["id", "name", "description"],
    hosts: ["id", "name", "ip", "envId"],
    services: ["id", "name", "owner"],
    endpoints: ["id", "serviceId", "envId", "url", "method"],
    snapshots: ["endpointId", "version", "build", "timestamp"]
};

const defaultData = {
    environments: [],
    hosts: [],
    services: [],
    endpoints: [],
    snapshots: []
};

/* ============================
   STORAGE
============================ */

// Инициализация IndexedDB при загрузке
let indexedDBReady = false;
(async () => {
    try {
        const result = await initIndexedDB();
        indexedDBReady = result.success && !result.fallback;
        console.log('[App] IndexedDB initialized:', indexedDBReady ? 'success' : 'fallback to localStorage');

        // Автоматическая миграция из localStorage
        if (indexedDBReady && hasDataToMigrate()) {
            try {
                const db = getDB();
                const migrationResult = await migrateFromLocalStorage(db);
                if (migrationResult.completed) {
                    console.log('[App] Migration completed successfully');
                } else if (migrationResult.skipped) {
                    console.log('[App] Migration already completed');
                } else {
                    console.warn('[App] Migration completed with errors:', migrationResult);
                }
            } catch (migrationError) {
                console.error('[App] Migration error:', migrationError);
            }
        }

        // Загружаем данные из IndexedDB после инициализации
        if (indexedDBReady) {
            try {
                data = await loadDataAsync();
                console.log('[App] Data loaded from IndexedDB');
                // Обновляем UI после загрузки
                renderAllTables();

                // Восстанавливаем последний загруженный Excel файл
                await restoreLastExcelFile();

                // Обновляем список файлов (отложенно, чтобы функция была определена)
                setTimeout(() => updateExcelFileList(), 100);
            } catch (error) {
                console.error('[App] Error loading data from IndexedDB:', error);
            }
        }
    } catch (error) {
        console.error('[App] IndexedDB initialization error:', error);
        indexedDBReady = false;
    }
})();

// Асинхронная загрузка данных из IndexedDB или localStorage (fallback)
async function loadDataAsync() {
    const result = JSON.parse(JSON.stringify(defaultData));

    if (indexedDBReady) {
        // Загружаем из IndexedDB
        try {
            for (const entity of Object.keys(defaultData)) {
                result[entity] = await getInventory(entity);
            }
        } catch (error) {
            console.error('[App] Error loading from IndexedDB, using fallback:', error);
            return loadDataFallback();
        }
    } else {
        // Fallback на localStorage
        return loadDataFallback();
    }

    return result;
}

// Fallback на localStorage (для обратной совместимости)
function loadDataFallback() {
    const raw = localStorage.getItem("versionInventory");
    if (!raw) {
        return JSON.parse(JSON.stringify(defaultData));
    }
    try {
        const parsed = JSON.parse(raw);
        const result = JSON.parse(JSON.stringify(defaultData));
        Object.keys(defaultData).forEach(key => {
            if (Array.isArray(parsed[key])) {
                result[key] = parsed[key];
            }
        });
        return result;
    } catch (e) {
        console.error("Ошибка парсинга versionInventory, сбрасываю на defaultData", e);
        return JSON.parse(JSON.stringify(defaultData));
    }
}

// Инициализация данных
let data = loadDataFallback(); // Временная инициализация, будет обновлена после загрузки IndexedDB

// Асинхронное сохранение данных в IndexedDB или localStorage (fallback)
async function saveDataAsync() {
    if (indexedDBReady) {
        try {
            // Сохраняем каждую сущность отдельно
            for (const entity of Object.keys(defaultData)) {
                await saveInventory(entity, data[entity] || []);
            }
        } catch (error) {
            console.error('[App] Error saving to IndexedDB, using fallback:', error);
            saveDataFallback();
        }
    } else {
        saveDataFallback();
    }
}

// Fallback на localStorage
function saveDataFallback() {
    localStorage.setItem("versionInventory", JSON.stringify(data));
}

// Синхронная версия для обратной совместимости (использует fallback)
function saveData() {
    saveDataFallback();
}

/* ============================
   HELPERS
============================ */

function findById(list, id) {
    return list.find(x => x.id === id);
}

function getTooltipForCell(entity, key, value) {
    if (!value) return "";

    if (key === "envId") {
        const env = findById(data.environments, value);
        return env ? `Environment: ${env.id} — ${env.name || ""}` : "";
    }
    if (key === "serviceId") {
        const svc = findById(data.services, value);
        return svc ? `Service: ${svc.id} — ${svc.name || ""}` : "";
    }
    if (key === "endpointId") {
        const ep = findById(data.endpoints, value);
        return ep ? `Endpoint: ${ep.id} — ${ep.url || ""}` : "";
    }
    if (key === "hostId") {
        const host = findById(data.hosts, value);
        return host ? `Host: ${host.id} — ${host.ip || ""}` : "";
    }
    return "";
}

function escapeHtml(str) {
    return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

/* простая эвристика подбора колонок Excel под поле сущности */
function guessColumnForField(field, columns) {
    const lcCols = columns.map(c => c.toLowerCase());

    const direct = lcCols.indexOf(field.toLowerCase());
    if (direct !== -1) return columns[direct];

    const candidates = {
        id: ["id", "host", "hostname", "name"],
        name: ["name", "service", "svc", "hostname"],
        ip: ["ip", "ipaddress", "ip_address", "addr", "address"],
        envId: ["env", "environment", "stage"],
        serviceId: ["service", "svc", "service_id", "svc_id"],
        endpointId: ["endpoint", "ep", "endpoint_id"],
        url: ["url", "uri", "endpoint", "path"],
        method: ["method", "httpmethod", "verb"],
        version: ["version", "ver", "iib_version", "app_version"],
        build: ["build", "build_number", "buildno"]
    };

    const list = candidates[field] || [];
    for (const pattern of list) {
        const idx = lcCols.findIndex(c => c === pattern || c.includes(pattern));
        if (idx !== -1) return columns[idx];
    }

    return "";
}

/* ============================
   TABS
============================ */

document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.onclick = () => {
        document.querySelectorAll(".tab-btn").forEach(x => x.classList.remove("active"));
        btn.classList.add("active");

        const id = btn.dataset.tab;
        document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
        const tab = document.getElementById(id);
        if (!tab) {
            console.warn(`[App] Tab with id "${id}" not found`);
            return;
        }
        tab.classList.add("active");

        if (id === "matrix") {
            renderMatrix();
        }
        if (id === "excel") {
            renderExcelSheet();
            renderExcelImportMapping();
            updateZbxColumnSelects();
        }
        if (id === "sql") {
            initSQLUI();
        }
        if (id === "templates") {
            initTemplatesUI();
        }
        if (id === "dashboard") {
            // Отложенный вызов, чтобы IndexedDB успел инициализироваться
            setTimeout(() => renderDashboard(), 100);
        }
    };
});

// активируем первую вкладку 
document.querySelector(".tab-btn").click();

/* ============================
   GENERIC TABLE RENDERER
============================ */

function renderTable(entity) {
    const table = document.querySelector(`table[data-entity="${entity}"]`);
    if (!table) return;

    const rows = data[entity] || [];
    const keys = schemas[entity];

    let html = "<tr>";
    keys.forEach(k => {
        html += `<th>${k}</th>`;
    });
    html += "<th></th></tr>";

    rows.forEach((row, i) => {
        html += "<tr>";
        keys.forEach(k => {
            const value = row[k] ?? "";
            const tooltip = getTooltipForCell(entity, k, value);
            const titleAttr = tooltip ? ` title="${tooltip.replace(/"/g, "&quot;")}"` : "";
            html += `<td contenteditable="true" data-key="${k}" data-index="${i}"${titleAttr}>${value}</td>`;
        });
        html += `<td><button class="del-row" data-index="${i}" data-entity="${entity}">✕</button></td>`;
        html += "</tr>";
    });

    table.innerHTML = html;

    table.querySelectorAll("td[contenteditable]").forEach(td => {
        td.oninput = async () => {
            const idx = Number(td.dataset.index);
            const key = td.dataset.key;
            const item = data[entity][idx];
            const newValue = td.innerText.trim();

            // Обновляем локальное состояние
            item[key] = newValue;

            if (entity === "snapshots" && key !== "timestamp" && !item.timestamp) {
                item.timestamp = new Date().toISOString();
            }

            // Сохраняем в IndexedDB или localStorage
            if (indexedDBReady && item.id) {
                try {
                    await updateInventoryItem(entity, item.id, { [key]: newValue });
                    if (entity === "snapshots" && key !== "timestamp" && !item.timestamp) {
                        await updateInventoryItem(entity, item.id, { timestamp: item.timestamp });
                    }
                } catch (error) {
                    console.error('[App] Error updating item:', error);
                    saveData(); // Fallback
                }
            } else {
                saveData(); // Fallback на localStorage
            }

            if (entity === "snapshots") {
                renderTable("snapshots");
            }
        };
    });

    table.querySelectorAll(".del-row").forEach(btn => {
        btn.onclick = async () => {
            const idx = Number(btn.dataset.index);
            if (!confirm("Удалить строку?")) return;

            const item = data[entity][idx];
            data[entity].splice(idx, 1);

            // Удаляем из IndexedDB или localStorage
            if (indexedDBReady && item && item.id) {
                try {
                    await deleteInventoryItem(entity, item.id);
                } catch (error) {
                    console.error('[App] Error deleting item:', error);
                    saveData(); // Fallback
                }
            } else {
                saveData(); // Fallback на localStorage
            }

            renderAllTables();
        };
    });
}

function renderAllTables() {
    ["environments", "hosts", "services", "endpoints", "snapshots"].forEach(renderTable);
}

renderAllTables();

/* ============================
   ADD ROW
============================ */

document.querySelectorAll(".add-row").forEach(btn => {
    btn.onclick = async () => {
        const entity = btn.dataset.entity;
        const keys = schemas[entity];

        const row = {};
        keys.forEach(k => {
            if (entity === "snapshots" && k === "timestamp") {
                row[k] = new Date().toISOString();
            } else {
                row[k] = "";
            }
        });

        // Генерируем ID если его нет
        if (!row.id && entity !== "snapshots") {
            row.id = `new_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
        }

        data[entity].push(row);

        // Сохраняем в IndexedDB или localStorage
        if (indexedDBReady && row.id) {
            try {
                await addInventoryItem(entity, row);
            } catch (error) {
                console.error('[App] Error adding item:', error);
                saveData(); // Fallback
            }
        } else {
            saveData(); // Fallback на localStorage
        }

        renderTable(entity);
    };
});

/* ============================
   CURL GENERATION
============================ */

document.getElementById("genCurl").onclick = () => {
    const lines = data.endpoints.map(e => {
        const method = (e.method || "GET").toUpperCase();
        const url = e.url || "";
        return `curl -X ${method} "${url}" -H "Accept: application/json"`;
    });
    document.getElementById("curlOutput").value = lines.join("\n");
};

/* ============================
   SNAPSHOT JSON IMPORT
============================ */

document.getElementById("applySnapshotJson").onclick = async () => {
    const text = document.getElementById("snapshotJson").value.trim();
    if (!text) {
        alert("Пустой ввод");
        return;
    }

    try {
        const payload = JSON.parse(text);
        const arr = Array.isArray(payload) ? payload : [payload];

        const itemsToAdd = [];
        arr.forEach(obj => {
            if (!obj.endpointId) return;
            const snapshot = {
                endpointId: obj.endpointId,
                version: obj.version || "",
                build: obj.build || "",
                timestamp: obj.timestamp || new Date().toISOString()
            };
            data.snapshots.push(snapshot);
            itemsToAdd.push(snapshot);
        });

        if (itemsToAdd.length === 0) {
            alert("Не найдено ни одного валидного объекта с endpointId");
            return;
        }

        // Сохраняем в IndexedDB или localStorage
        if (indexedDBReady) {
            try {
                for (const item of itemsToAdd) {
                    await addInventoryItem("snapshots", item);
                }
            } catch (error) {
                console.error('[App] Error adding snapshots:', error);
                saveData(); // Fallback
            }
        } else {
            saveData(); // Fallback на localStorage
        }

        renderTable("snapshots");
        alert(`Добавлено снапшотов: ${itemsToAdd.length}`);
    } catch (e) {
        console.error(e);
        alert("Неверный JSON");
    }
};

/* ============================
   VERSION MATRIX
============================ */

async function renderMatrix() {
    const table = document.getElementById("versionMatrix");
    const baselineSelect = document.getElementById("matrixBaseline");

    const envs = (data.environments || []).map(e => e.id).filter(Boolean);
    const svcs = (data.services || []).map(s => s.id).filter(Boolean);

    baselineSelect.innerHTML = envs.map(e => `<option value="${e}">${e}</option>`).join("");
    if (!baselineSelect.value && envs.length > 0) {
        baselineSelect.value = envs[0];
    }

    // Загружаем все значения матрицы из IndexedDB
    let matrixData = {};
    if (indexedDBReady) {
        try {
            matrixData = await getAllMatrix();
        } catch (error) {
            console.error('[App] Error loading matrix:', error);
        }
    } else {
        // Fallback: загружаем из localStorage
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key && key.startsWith("matrix_")) {
                const cellId = key.substring(7); // убираем "matrix_"
                matrixData[cellId] = localStorage.getItem(key);
            }
        }
    }

    let html = "<tr><th>Service \\ Env</th>";
    envs.forEach(e => {
        html += `<th>${e}</th>`;
    });
    html += "</tr>";

    svcs.forEach(svc => {
        html += `<tr><td>${svc}</td>`;
        envs.forEach(env => {
            const cellId = `${svc}__${env}`;
            const stored = matrixData[cellId] || "";
            html += `<td contenteditable="true" data-cell="${cellId}">${stored}</td>`;
        });
        html += "</tr>";
    });

    table.innerHTML = html;

    table.querySelectorAll("td[contenteditable]").forEach(td => {
        td.oninput = async () => {
            const cellId = td.dataset.cell;
            const [svc, env] = cellId.split("__");
            const value = td.innerText.trim();

            // Сохраняем в IndexedDB или localStorage
            if (indexedDBReady) {
                try {
                    await setMatrixCell(svc, env, value);
                } catch (error) {
                    console.error('[App] Error saving matrix cell:', error);
                    // Fallback на localStorage
                    if (value) {
                        localStorage.setItem("matrix_" + cellId, value);
                    } else {
                        localStorage.removeItem("matrix_" + cellId);
                    }
                }
            } else {
                // Fallback на localStorage
                if (value) {
                    localStorage.setItem("matrix_" + cellId, value);
                } else {
                    localStorage.removeItem("matrix_" + cellId);
                }
            }

            highlightDiffs();
        };
    });

    highlightDiffs();
}

function highlightDiffs() {
    const baselineEnv = document.getElementById("matrixBaseline").value;
    if (!baselineEnv) return;

    const cells = document.querySelectorAll("#versionMatrix td[contenteditable]");
    const baselineVals = {};

    cells.forEach(td => {
        td.classList.remove("baseline", "diff");
    });

    cells.forEach(td => {
        const [svc, env] = td.dataset.cell.split("__");
        if (env === baselineEnv) {
            baselineVals[svc] = td.innerText.trim();
            td.classList.add("baseline");
        }
    });

    cells.forEach(td => {
        const [svc, env] = td.dataset.cell.split("__");
        const val = td.innerText.trim();
        if (env === baselineEnv) return;
        if (!baselineVals[svc]) return;
        if (!val) return;
        if (val !== baselineVals[svc]) {
            td.classList.add("diff");
        }
    });
}

document.getElementById("matrixBaseline").onchange = () => {
    highlightDiffs();
};

/* ============================
   EXPORT / IMPORT
============================ */

const entities = ["environments", "hosts", "services", "endpoints", "snapshots"];

["exportJsonSelect", "exportCsvSelect"].forEach(id => {
    const sel = document.getElementById(id);
    sel.innerHTML = entities.map(e => `<option value="${e}">${e}</option>`).join("");
});

// JSON EXPORT
document.getElementById("exportJsonBtn").onclick = () => {
    const e = document.getElementById("exportJsonSelect").value;
    const arr = data[e] || [];
    document.getElementById("exportJsonArea").value = JSON.stringify(arr, null, 2);
};

// CSV EXPORT
document.getElementById("exportCsvBtn").onclick = () => {
    const e = document.getElementById("exportCsvSelect").value;
    const rows = data[e] || [];
    if (!rows.length) {
        document.getElementById("exportCsvArea").value = "";
        return;
    }
    const keys = schemas[e];
    let csv = keys.join(";") + "\n";
    rows.forEach(r => {
        csv += keys.map(k => (r[k] ?? "")).join(";") + "\n";
    });
    document.getElementById("exportCsvArea").value = csv;
};

// FULL JSON IMPORT
document.getElementById("importJsonBtn").onclick = async () => {
    const text = document.getElementById("importJsonArea").value.trim();
    if (!text) {
        alert("Пустой ввод");
        return;
    }
    if (!confirm("Полностью заменить все данные (environments/hosts/services/endpoints/snapshots)?")) {
        return;
    }

    try {
        const newData = JSON.parse(text);
        const shape = Object.keys(defaultData);

        shape.forEach(key => {
            data[key] = Array.isArray(newData[key]) ? newData[key] : [];
        });

        // Сохраняем в IndexedDB или localStorage
        if (indexedDBReady) {
            try {
                for (const entity of shape) {
                    await saveInventory(entity, data[entity] || []);
                }
            } catch (error) {
                console.error('[App] Error importing data:', error);
                saveData(); // Fallback
            }
        } else {
            saveData(); // Fallback на localStorage
        }

        renderAllTables();
        alert("Импорт выполнен");
    } catch (e) {
        console.error(e);
        alert("Ошибка JSON");
    }
};

/* ============================
   EXCEL WORKSPACE
============================ */

const excelState = {
    fileId: null, // ID файла в IndexedDB
    fileName: null, // Имя файла
    workbook: null, // XLSX workbook (только в памяти)
    sheets: {}, // name -> { columns: [...], rows: [...] }
    activeSheetName: null,
    filterText: ""
};

const excelFileInput = document.getElementById("excelFileInput");
const excelSheetSelect = document.getElementById("excelSheetSelect");
const excelFilterInput = document.getElementById("excelFilterInput");
const excelColumnsList = document.getElementById("excelColumnsList");
const excelDataTable = document.getElementById("excelDataTable");
const excelTemplateInput = document.getElementById("excelTemplateInput");
const excelOutputArea = document.getElementById("excelOutputArea");
const excelApplyTemplateBtn = document.getElementById("excelApplyTemplateBtn");
const excelInsertSelectedColumnsBtn = document.getElementById("excelInsertSelectedColumnsBtn");

// Import mapping
const excelImportEntitySelect = document.getElementById("excelImportEntity");
const excelImportMappingTable = document.getElementById("excelImportMappingTable");
const excelImportApplyBtn = document.getElementById("excelImportApplyBtn");

// Zabbix builder
const zbxHostColSelect = document.getElementById("zbxHostCol");
const zbxPortColSelect = document.getElementById("zbxPortCol");
const zbxItemKeyInput = document.getElementById("zbxItemKey");
const zbxScriptPathInput = document.getElementById("zbxScriptPath");
const zbxBuildBtn = document.getElementById("zbxBuildBtn");
const zbxScriptArea = document.getElementById("zbxScriptArea");
const zbxUserParamArea = document.getElementById("zbxUserParamArea");
const zbxInventoryArea = document.getElementById("zbxInventoryArea");

excelFileInput.addEventListener("change", handleExcelFile);
excelSheetSelect.addEventListener("change", () => {
    excelState.activeSheetName = excelSheetSelect.value || null;
    renderExcelSheet();
    renderExcelImportMapping();
    updateZbxColumnSelects();
});
if (excelFilterInput) {
    excelFilterInput.addEventListener("input", () => {
        excelState.filterText = excelFilterInput.value.toLowerCase();
        renderExcelSheet();
    });
}

// Глобальный поиск в Excel Workspace
const excelGlobalSearchInput = document.getElementById("excelGlobalSearchInput");
const excelGlobalSearchBtn = document.getElementById("excelGlobalSearchBtn");
const excelGlobalSearchResults = document.getElementById("excelGlobalSearchResults");

if (excelGlobalSearchBtn && excelGlobalSearchInput) {
    excelGlobalSearchBtn.addEventListener("click", async () => {
        const query = excelGlobalSearchInput.value.trim();
        if (!query) {
            if (excelGlobalSearchResults) excelGlobalSearchResults.innerHTML = "";
            return;
        }

        if (!isSearchReady()) {
            alert("Поиск не готов. Сначала постройте индекс.");
            return;
        }

        try {
            if (excelGlobalSearchResults) {
                excelGlobalSearchResults.innerHTML = "<p>Поиск...</p>";
            }

            const results = await searchIndex(query, { limit: 20 });

            if (results.error) {
                if (excelGlobalSearchResults) {
                    excelGlobalSearchResults.innerHTML = `<p class="error">Ошибка: ${results.error}</p>`;
                }
                return;
            }

            if (!excelGlobalSearchResults) return;

            if (results.results.length === 0) {
                excelGlobalSearchResults.innerHTML = "<p class='no-results'>Совпадений не найдено</p>";
                return;
            }

            // Группируем результаты
            const grouped = {};
            results.results.forEach(result => {
                const key = `${result.fileName}::${result.sheetName}`;
                if (!grouped[key]) {
                    grouped[key] = {
                        fileName: result.fileName,
                        sheetName: result.sheetName,
                        fileId: result.fileId,
                        results: []
                    };
                }
                grouped[key].results.push(result);
            });

            let html = `<div class="search-results-header">Найдено: ${results.totalMatches} совпадений</div>`;
            Object.values(grouped).forEach(group => {
                html += `<div class="search-result-item">
                    <strong>${escapeHtml(group.fileName)} → ${escapeHtml(group.sheetName)}</strong>
                    <button class="btn-small" onclick="loadExcelFileAndGoTo('${group.fileId}', '${group.sheetName}', ${group.results[0].rowIndex})">
                        Перейти (${group.results.length} совпадений)
                    </button>
                </div>`;
            });

            excelGlobalSearchResults.innerHTML = html;
        } catch (error) {
            console.error('[App] Global search error:', error);
            if (excelGlobalSearchResults) {
                excelGlobalSearchResults.innerHTML = `<p class="error">Ошибка поиска: ${error.message}</p>`;
            }
        }
    });

    if (excelGlobalSearchInput) {
        excelGlobalSearchInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") {
                excelGlobalSearchBtn.click();
            }
        });
    }
}

// Загрузка файла и переход к строке
window.loadExcelFileAndGoTo = async function (fileId, sheetName, rowIndex) {
    await loadExcelFile(fileId);

    // Выбираем нужный лист
    if (excelSheetSelect) {
        excelSheetSelect.value = sheetName;
        excelState.activeSheetName = sheetName;
        renderExcelSheet();
    }

    // Прокручиваем к строке
    setTimeout(() => {
        const table = excelDataTable;
        if (table) {
            const rows = table.querySelectorAll("tr");
            if (rows[rowIndex + 1]) { // +1 для заголовка
                rows[rowIndex + 1].scrollIntoView({ behavior: 'smooth', block: 'center' });
                rows[rowIndex + 1].classList.add('highlight-row');
                setTimeout(() => {
                    rows[rowIndex + 1].classList.remove('highlight-row');
                }, 2000);
            }
        }
    }, 300);
};

excelApplyTemplateBtn.addEventListener("click", buildExcelOutput);
excelInsertSelectedColumnsBtn.addEventListener("click", insertSelectedColumnsIntoTemplate);

excelImportEntitySelect.addEventListener("change", () => {
    renderExcelImportMapping();
});

excelImportApplyBtn.addEventListener("click", applyExcelImportToInventory);

zbxBuildBtn.addEventListener("click", buildZabbixConfig);

// Zabbix Builder v2 - Загрузка пресета
const zbxPresetSelect = document.getElementById("zbxPresetSelect");
const zbxLoadPresetBtn = document.getElementById("zbxLoadPresetBtn");
const zbxSavePresetBtn = document.getElementById("zbxSavePresetBtn");

if (zbxLoadPresetBtn) {
    zbxLoadPresetBtn.addEventListener("click", () => {
        const presetId = zbxPresetSelect ? zbxPresetSelect.value : 'custom';
        if (presetId === 'custom' || !zabbixPresets[presetId]) {
            alert("Выберите пресет");
            return;
        }

        const preset = zabbixPresets[presetId];
        if (zbxItemKeyInput) zbxItemKeyInput.value = preset.itemKey;
        if (zbxScriptPathInput) zbxScriptPathInput.value = preset.scriptPath;

        alert(`Пресет "${preset.name}" загружен`);
    });
}

if (zbxSavePresetBtn) {
    zbxSavePresetBtn.addEventListener("click", async () => {
        const name = prompt("Введите название пресета:");
        if (!name) return;

        const itemKey = zbxItemKeyInput ? zbxItemKeyInput.value.trim() : '';
        const scriptPath = zbxScriptPathInput ? zbxScriptPathInput.value.trim() : '';
        const script = zbxScriptArea ? zbxScriptArea.value : '';

        if (!itemKey || !scriptPath || !script) {
            alert("Заполните все поля перед сохранением");
            return;
        }

        try {
            const templateData = {
                name: `Zabbix: ${name}`,
                type: 'zabbix',
                category: 'scripts',
                description: `Пресет Zabbix: ${name}`,
                template: script,
                variables: ['host', 'port']
            };

            await saveTemplate(templateData);
            alert("Пресет сохранен как шаблон");
        } catch (error) {
            console.error('[App] Error saving preset:', error);
            alert("Ошибка сохранения пресета");
        }
    });
}

async function handleExcelFile(e) {
    const files = e.target.files;
    if (!files || files.length === 0) return;

    // Поддержка множественной загрузки
    for (let i = 0; i < files.length; i++) {
        const file = files[i];
        await processExcelFile(file);
    }

    // Обновляем список файлов
    await updateExcelFileList();
}

async function processExcelFile(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = async (ev) => {
            const dataArr = ev.target.result;
            try {
                const wb = XLSX.read(dataArr, { type: "array" });
                excelState.workbook = wb;
                excelState.sheets = {};

                // Сохраняем файл в IndexedDB
                const fileId = await saveFile({
                    name: file.name,
                    type: file.name.split('.').pop() || 'xlsx',
                    size: file.size,
                    uploadedAt: new Date().toISOString(),
                    metadata: {
                        sheetCount: wb.SheetNames.length,
                        sheetNames: wb.SheetNames
                    }
                });

                excelState.fileId = fileId;
                excelState.fileName = file.name;

                // Обрабатываем и сохраняем каждый лист
                for (const name of wb.SheetNames) {
                    const ws = wb.Sheets[name];
                    const rows = XLSX.utils.sheet_to_json(ws, { defval: "" });
                    let columns = [];
                    rows.forEach(r => {
                        Object.keys(r).forEach(k => {
                            if (!columns.includes(k)) columns.push(k);
                        });
                    });

                    const sheetData = { columns, rows };
                    excelState.sheets[name] = sheetData;

                    // Сохраняем лист в IndexedDB
                    try {
                        await saveSheet(fileId, name, sheetData);
                    } catch (error) {
                        console.error(`[App] Error saving sheet ${name}:`, error);
                    }
                }

                // Обновляем UI только для последнего загруженного файла
                if (excelState.fileId === fileId) {
                    excelSheetSelect.innerHTML = wb.SheetNames
                        .map(name => `<option value="${name}">${name}</option>`)
                        .join("");

                    excelState.activeSheetName = wb.SheetNames[0] || null;
                    excelState.filterText = "";
                    excelFilterInput.value = "";
                    excelTemplateInput.value = "";
                    excelOutputArea.value = "";
                    zbxScriptArea.value = "";
                    zbxUserParamArea.value = "";
                    zbxInventoryArea.value = "";

                    renderExcelSheet();
                    renderExcelImportMapping();
                    updateZbxColumnSelects();
                }

                console.log(`[App] Excel file saved: ${file.name} (ID: ${fileId})`);

                // Автоматическая индексация листов
                if (isSearchReady()) {
                    try {
                        for (const name of wb.SheetNames) {
                            const sheetData = excelState.sheets[name];
                            if (sheetData) {
                                await indexSheet(fileId, file.name, name, sheetData);
                            }
                        }
                        console.log(`[App] Excel file indexed: ${file.name}`);
                    } catch (error) {
                        console.error('[App] Error indexing Excel file:', error);
                    }
                }

                resolve(fileId);
            } catch (err) {
                console.error(err);
                alert(`Ошибка чтения файла Excel: ${file.name}`);
                reject(err);
            }
        };
        reader.onerror = () => {
            reject(new Error(`Ошибка чтения файла: ${file.name}`));
        };
        reader.readAsArrayBuffer(file);
    });
}

// Обновление списка загруженных файлов
async function updateExcelFileList() {
    if (!indexedDBReady) return;

    try {
        const files = await listFiles();
        const fileListElement = document.getElementById("excelFileList");
        if (fileListElement) {
            if (files.length === 0) {
                fileListElement.innerHTML = '<p class="hint">Нет загруженных файлов</p>';
            } else {
                let html = '<h4>Загруженные файлы:</h4><ul class="file-list">';
                files.forEach(file => {
                    const isActive = excelState.fileId === file.id;
                    html += `<li class="${isActive ? 'active' : ''}">
                        <span>${escapeHtml(file.name)}</span>
                        <button class="btn-small" onclick="loadExcelFile('${file.id}')">Загрузить</button>
                        <button class="btn-small" onclick="deleteExcelFile('${file.id}')">Удалить</button>
                    </li>`;
                });
                html += '</ul>';
                fileListElement.innerHTML = html;
            }
        }
    } catch (error) {
        console.error('[App] Error updating file list:', error);
    }
}

// Загрузка файла из списка
window.loadExcelFile = async function (fileId) {
    try {
        const file = await getFile(fileId);
        if (!file) {
            alert("Файл не найден");
            return;
        }

        const sheets = await listSheets(fileId);
        if (sheets.length === 0) {
            alert("В файле нет листов");
            return;
        }

        // Загружаем все листы
        excelState.fileId = fileId;
        excelState.fileName = file.name;
        excelState.sheets = {};

        for (const sheetName of sheets) {
            const sheetData = await getSheet(fileId, sheetName);
            if (sheetData) {
                excelState.sheets[sheetName] = sheetData;
            }
        }

        excelState.activeSheetName = sheets[0];

        excelSheetSelect.innerHTML = sheets
            .map(name => `<option value="${name}">${name}</option>`)
            .join("");

        excelSheetSelect.value = sheets[0];
        excelState.filterText = "";
        excelFilterInput.value = "";

        renderExcelSheet();
        renderExcelImportMapping();
        updateZbxColumnSelects();
        updateExcelFileList();

        console.log(`[App] Excel file loaded: ${file.name}`);
    } catch (error) {
        console.error('[App] Error loading Excel file:', error);
        alert("Ошибка загрузки файла");
    }
};

// Удаление файла
window.deleteExcelFile = async function (fileId) {
    if (!confirm("Удалить этот файл?")) return;

    try {
        // Удаляем из поиска
        const sheets = await listSheets(fileId);
        for (const sheetName of sheets) {
            await removeSheet(fileId, sheetName);
        }

        // Удаляем файл
        await deleteFile(fileId);

        // Если это текущий файл, очищаем состояние
        if (excelState.fileId === fileId) {
            excelState.fileId = null;
            excelState.fileName = null;
            excelState.sheets = {};
            excelState.activeSheetName = null;
            excelSheetSelect.innerHTML = "";
            renderExcelSheet();
        }

        await updateExcelFileList();
        console.log(`[App] Excel file deleted: ${fileId}`);
    } catch (error) {
        console.error('[App] Error deleting Excel file:', error);
        alert("Ошибка удаления файла");
    }
};

function getActiveExcelSheet() {
    if (!excelState.activeSheetName) return null;
    return excelState.sheets[excelState.activeSheetName] || null;
}

/**
 * Восстановление последнего загруженного Excel файла из IndexedDB
 */
async function restoreLastExcelFile() {
    if (!indexedDBReady) {
        return;
    }

    try {
        const files = await listFiles();
        if (files.length === 0) {
            return;
        }

        // Берем последний загруженный файл
        const lastFile = files.sort((a, b) => {
            return new Date(b.uploadedAt) - new Date(a.uploadedAt);
        })[0];

        excelState.fileId = lastFile.id;
        excelState.fileName = lastFile.name;

        // Загружаем листы
        const sheetNames = await listSheets(lastFile.id);
        excelState.sheets = {};

        for (const sheetName of sheetNames) {
            const sheetData = await getSheet(lastFile.id, sheetName);
            if (sheetData) {
                excelState.sheets[sheetName] = sheetData;
            }
        }

        // Обновляем UI
        if (sheetNames.length > 0) {
            excelSheetSelect.innerHTML = sheetNames
                .map(name => `<option value="${name}">${name}</option>`)
                .join("");

            excelState.activeSheetName = sheetNames[0] || null;
            excelState.filterText = "";
            excelFilterInput.value = "";

            renderExcelSheet();
            renderExcelImportMapping();
            updateZbxColumnSelects();

            console.log(`[App] Excel file restored: ${lastFile.name} (${sheetNames.length} sheets)`);
        }
    } catch (error) {
        console.error('[App] Error restoring Excel file:', error);
    }
}

function renderExcelSheet() {
    const sheet = getActiveExcelSheet();
    excelColumnsList.innerHTML = "";
    excelDataTable.innerHTML = "";

    if (!sheet) return;

    const columns = sheet.columns;
    const rows = sheet.rows;

    // колонки (чекбоксы для билдера строк)
    let colsHtml = "";
    columns.forEach(col => {
        colsHtml += `<li>
            <label><input type="checkbox" data-col="${col}" checked> ${col}</label>
        </li>`;
    });
    excelColumnsList.innerHTML = colsHtml;

    // таблица предпросмотра
    let html = "<tr>";
    columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += "</tr>";

    const filterText = excelState.filterText;

    const filteredRows = rows.filter(row => {
        if (!filterText) return true;
        return columns.some(col => {
            const val = (row[col] ?? "").toString().toLowerCase();
            return val.includes(filterText);
        });
    });

    const maxRows = 200;
    filteredRows.slice(0, maxRows).forEach(row => {
        html += "<tr>";
        columns.forEach(col => {
            const val = row[col] ?? "";
            html += `<td>${escapeHtml(val.toString())}</td>`;
        });
        html += "</tr>";
    });

    excelDataTable.innerHTML = html;
}

function buildExcelOutput() {
    const sheet = getActiveExcelSheet();
    if (!sheet) {
        alert("Сначала загрузите Excel и выберите лист");
        return;
    }
    const template = excelTemplateInput.value;
    if (!template.trim()) {
        alert("Шаблон пустой");
        return;
    }

    const columns = sheet.columns;
    const rows = sheet.rows;
    const filterText = excelState.filterText;

    const filteredRows = rows.filter(row => {
        if (!filterText) return true;
        return columns.some(col => {
            const val = (row[col] ?? "").toString().toLowerCase();
            return val.includes(filterText);
        });
    });

    const lines = filteredRows.map(row => applyTemplateToRow(template, row));
    excelOutputArea.value = lines.join("\n");
}

function applyTemplateToRow(template, row) {
    return template.replace(/\{([^}]+)\}/g, (match, colName) => {
        const key = colName.trim();
        const val = row[key];
        return (val === undefined || val === null) ? "" : String(val);
    });
}

function insertSelectedColumnsIntoTemplate() {
    const checkboxes = excelColumnsList.querySelectorAll('input[type="checkbox"]:checked');
    if (!checkboxes.length) {
        alert("Нет выбранных колонок");
        return;
    }
    const cols = Array.from(checkboxes).map(ch => `{${ch.dataset.col}}`);
    const toInsert = cols.join(" ");

    const textarea = excelTemplateInput;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const value = textarea.value;

    textarea.value = value.slice(0, start) + toInsert + value.slice(end);
    textarea.focus();
    textarea.selectionStart = textarea.selectionEnd = start + toInsert.length;
}

/* ============================
   EXCEL → INVENTORY IMPORT
============================ */

function renderExcelImportMapping() {
    const sheet = getActiveExcelSheet();
    excelImportMappingTable.innerHTML = "";
    if (!sheet) return;

    const entity = excelImportEntitySelect.value;
    const fields = schemas[entity];
    const columns = sheet.columns;

    let html = "<tr><th>Поле сущности</th><th>Колонка Excel</th></tr>";
    fields.forEach(field => {
        const guessed = guessColumnForField(field, columns);
        html += `<tr>
            <td>${field}</td>
            <td>
                <select data-field="${field}">
                    <option value="">(пусто)</option>
                    ${columns
                .map(c => `<option value="${c}" ${guessed === c ? "selected" : ""}>${c}</option>`)
                .join("")}
                </select>
            </td>
        </tr>`;
    });

    excelImportMappingTable.innerHTML = html;
}

async function applyExcelImportToInventory() {
    const sheet = getActiveExcelSheet();
    if (!sheet) {
        alert("Сначала загрузите Excel и выберите лист");
        return;
    }

    const entity = excelImportEntitySelect.value;
    const fields = schemas[entity];
    const mapping = {};

    excelImportMappingTable.querySelectorAll("select[data-field]").forEach(sel => {
        const field = sel.dataset.field;
        mapping[field] = sel.value || "";
    });

    const columns = sheet.columns;
    const rows = sheet.rows;
    const filterText = excelState.filterText;

    const filteredRows = rows.filter(row => {
        if (!filterText) return true;
        return columns.some(col => {
            const val = (row[col] ?? "").toString().toLowerCase();
            return val.includes(filterText);
        });
    });

    if (!filteredRows.length) {
        alert("Нет строк для импорта (проверить фильтр)");
        return;
    }

    let imported = 0;
    const itemsToAdd = [];

    filteredRows.forEach(row => {
        const newRow = {};
        fields.forEach(f => {
            const col = mapping[f];
            if (col) {
                newRow[f] = row[col] ?? "";
            } else {
                if (entity === "snapshots" && f === "timestamp") {
                    newRow[f] = new Date().toISOString();
                } else {
                    newRow[f] = "";
                }
            }
        });

        // Генерируем ID если его нет
        if (!newRow.id && entity !== "snapshots") {
            newRow.id = `import_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
        }

        data[entity].push(newRow);
        itemsToAdd.push(newRow);
        imported++;
    });

    // Сохраняем в IndexedDB или localStorage
    if (indexedDBReady) {
        try {
            for (const item of itemsToAdd) {
                if (item.id) {
                    await addInventoryItem(entity, item);
                }
            }
        } catch (error) {
            console.error('[App] Error importing items:', error);
            saveData(); // Fallback
        }
    } else {
        saveData(); // Fallback на localStorage
    }

    renderAllTables();
    alert(`Импортировано строк: ${imported} в сущность ${entity}`);
}

/* ============================
   ZABBIX BUILDER
============================ */

function updateZbxColumnSelects() {
    const sheet = getActiveExcelSheet();
    let options = '<option value="">(не использовать)</option>';

    if (sheet) {
        sheet.columns.forEach(col => {
            options += `<option value="${col}">${col}</option>`;
        });
    }

    zbxHostColSelect.innerHTML = options;
    zbxPortColSelect.innerHTML = options;

    if (sheet) {
        const guessedHost = guessColumnForField("id", sheet.columns) || guessColumnForField("name", sheet.columns);
        const guessedPort = guessColumnForField("port", sheet.columns);

        if (guessedHost) zbxHostColSelect.value = guessedHost;
        if (guessedPort) zbxPortColSelect.value = guessedPort;
    }
}

// Zabbix Builder v2 - Пресеты
const zabbixPresets = {
    'iib-broker': {
        name: 'IIB Broker',
        description: 'Мониторинг IBM Integration Bus брокера',
        scriptTemplate: `#!/usr/bin/env bash
# Скрипт для проверки версии IIB брокера
HOST="$1"
PORT="\${2:-7800}"

# Опрос IIB через REST API
OUT=$(curl -s "http://$HOST:$PORT/rest/v1/version" 2>/dev/null || echo "")
if [ -n "$OUT" ]; then
    echo "$OUT" | jq -r '.version' 2>/dev/null || echo "$OUT"
else
    echo ""
fi
`,
        itemKey: 'iib.version',
        scriptPath: '/usr/local/bin/iib_version.sh'
    },
    'monolith': {
        name: 'Monolith Service',
        description: 'Мониторинг монолитного сервиса через Spring Actuator',
        scriptTemplate: `#!/usr/bin/env bash
# Скрипт для проверки версии монолитного сервиса
HOST="$1"
PORT="\${2:-8080}"

# Опрос через HTTP endpoint (Spring Actuator)
OUT=$(curl -s "http://$HOST:$PORT/actuator/info" 2>/dev/null || echo "")
if [ -n "$OUT" ]; then
    echo "$OUT" | jq -r '.build.version' 2>/dev/null || echo "$OUT"
else
    echo ""
fi
`,
        itemKey: 'monolith.version',
        scriptPath: '/usr/local/bin/monolith_version.sh'
    },
    'generic-http': {
        name: 'Generic HTTP Service',
        description: 'Универсальный HTTP сервис',
        scriptTemplate: `#!/usr/bin/env bash
# Скрипт для проверки версии HTTP сервиса
HOST="$1"
PORT="\${2:-8080}"

# Опрос через HTTP endpoint
OUT=$(curl -s "http://$HOST:$PORT/version" 2>/dev/null || echo "")
echo "$OUT"
`,
        itemKey: 'http.version',
        scriptPath: '/usr/local/bin/http_version.sh'
    }
};

function buildZabbixConfig() {
    const sheet = getActiveExcelSheet();
    if (!sheet) {
        alert("Сначала загрузите Excel и выберите лист");
        return;
    }

    const hostCol = zbxHostColSelect.value;
    const portCol = zbxPortColSelect.value;
    const itemKey = (zbxItemKeyInput.value || "custom.check").trim();
    const scriptPath = (zbxScriptPathInput.value || "/usr/local/bin/custom_check.sh").trim();

    if (!hostCol) {
        alert("Нужно выбрать колонку host");
        return;
    }

    // Используем пресет если выбран
    const presetId = zbxPresetSelect ? zbxPresetSelect.value : 'custom';
    let script = '';

    if (presetId !== 'custom' && zabbixPresets[presetId]) {
        script = zabbixPresets[presetId].scriptTemplate;
    } else {
        // Стандартный шаблон
        script = `#!/usr/bin/env bash
# ${scriptPath}
# Скрипт-обёртка для Zabbix-агента.
# Ожидает параметры:
#   $1 - host (например, имя/адрес брокера)
#   $2 - port (опционально)

HOST="$1"
PORT="\${2:-8080}"

# TODO: Заменить на реальную команду опроса брокера / монолита.
# Ниже пример через curl, адаптируй под свой случай:

# OUT=$(curl -s "http://$HOST:$PORT/version" || echo "")
# # здесь можно добавить парсинг JSON / текста
# echo "$OUT"

echo "IMPLEMENT_ME"
`;
    }

    zbxScriptArea.value = script;

    const userParam = `# Вставить в zabbix_agentd.conf на целевом хосте
UserParameter=${itemKey}[*],${scriptPath} "$1" "$2"`;

    zbxUserParamArea.value = userParam;

    const columns = sheet.columns;
    const rows = sheet.rows;
    const filterText = excelState.filterText;

    const filteredRows = rows.filter(row => {
        if (!filterText) return true;
        return columns.some(col => {
            const val = (row[col] ?? "").toString().toLowerCase();
            return val.includes(filterText);
        });
    });

    let csv = "host;port\n";
    filteredRows.forEach(row => {
        const host = row[hostCol] ?? "";
        const port = portCol ? (row[portCol] ?? "") : "";
        if (host) {
            csv += `${host};${port}\n`;
        }
    });

    zbxInventoryArea.value = csv;
}

/* ============================
   SEARCH ENGINE INITIALIZATION
============================ */

let searchEngineInitialized = false;

// Инициализация Search Engine
(async () => {
    try {
        const result = await initSearchEngine();
        searchEngineInitialized = result.success && result.ready;
        console.log('[App] Search Engine initialized:', searchEngineInitialized);
    } catch (error) {
        console.error('[App] Search Engine initialization error:', error);
        searchEngineInitialized = false;
    }
})();

/* ============================
   SQL / BI ENGINE
============================ */

let sqlQueryResults = [];

function initSQLUI() {
    const queryInput = document.getElementById("sqlQueryInput");
    const executeBtn = document.getElementById("sqlExecuteBtn");
    const clearBtn = document.getElementById("sqlClearBtn");
    const examplesBtn = document.getElementById("sqlExamplesBtn");
    const sourceSheetSelect = document.getElementById("sqlSourceSheet");
    const joinSheetSelect = document.getElementById("sqlJoinSheet");
    const resultsInfo = document.getElementById("sqlResultsInfo");
    const resultsTable = document.getElementById("sqlResultsTable");
    const exportCsvBtn = document.getElementById("sqlExportCsvBtn");
    const exportJsonBtn = document.getElementById("sqlExportJsonBtn");

    if (!queryInput) return;

    // Обновление списка листов
    async function updateSheetLists() {
        if (!indexedDBReady) return;

        try {
            const files = await listFiles();
            const allSheets = [];

            for (const file of files) {
                const sheetNames = await listSheets(file.id);
                sheetNames.forEach(sheetName => {
                    allSheets.push({
                        fileId: file.id,
                        fileName: file.name,
                        sheetName: sheetName,
                        displayName: `${file.name} → ${sheetName}`
                    });
                });
            }

            if (sourceSheetSelect) {
                sourceSheetSelect.innerHTML = '<option value="">Выберите лист...</option>';
                allSheets.forEach(sheet => {
                    sourceSheetSelect.innerHTML +=
                        `<option value="${sheet.fileId}::${sheet.sheetName}" data-file-id="${sheet.fileId}" data-sheet-name="${sheet.sheetName}">
                            ${sheet.displayName}
                        </option>`;
                });
            }

            if (joinSheetSelect) {
                joinSheetSelect.innerHTML = '<option value="">Нет</option>';
                allSheets.forEach(sheet => {
                    joinSheetSelect.innerHTML +=
                        `<option value="${sheet.fileId}::${sheet.sheetName}" data-file-id="${sheet.fileId}" data-sheet-name="${sheet.sheetName}">
                            ${sheet.displayName}
                        </option>`;
                });
            }
        } catch (error) {
            console.error('[App] Error updating sheet lists:', error);
        }
    }

    // Выполнение запроса
    async function executeSQLQuery() {
        const query = queryInput.value.trim();
        if (!query) {
            alert("Введите SQL запрос");
            return;
        }

        const sourceOption = sourceSheetSelect?.selectedOptions[0];
        if (!sourceOption || !sourceOption.value) {
            alert("Выберите источник данных (лист)");
            return;
        }

        const [fileId, sheetName] = sourceOption.value.split('::');

        try {
            // Загружаем данные
            const sheetData = await getSheet(fileId, sheetName);
            if (!sheetData || !sheetData.rows) {
                alert("Не удалось загрузить данные листа");
                return;
            }

            let rightData = null;
            if (joinSheetSelect?.value) {
                const [rightFileId, rightSheetName] = joinSheetSelect.value.split('::');
                const rightSheetData = await getSheet(rightFileId, rightSheetName);
                if (rightSheetData && rightSheetData.rows) {
                    rightData = rightSheetData.rows;
                }
            }

            // Парсим запрос
            const ast = parseQuery(query);

            // Выполняем запрос
            const startTime = performance.now();
            const results = executeQuery(ast, sheetData.rows, {
                rightData: rightData,
                rightTable: joinSheetSelect?.value ? joinSheetSelect.value.split('::')[1] : null
            });
            const execTime = performance.now() - startTime;

            sqlQueryResults = results;

            // Отображаем результаты
            if (resultsInfo) {
                resultsInfo.textContent =
                    `Найдено строк: ${results.length} (время выполнения: ${Math.round(execTime)} ms)`;
            }

            renderSQLResults(results);

        } catch (error) {
            console.error('[App] SQL query error:', error);
            if (resultsInfo) {
                resultsInfo.textContent = `Ошибка: ${error.message}`;
            }
            if (resultsTable) {
                resultsTable.innerHTML = `<p class="error">Ошибка выполнения запроса: ${error.message}</p>`;
            }
        }
    }

    // Отображение результатов
    function renderSQLResults(results) {
        if (!resultsTable) return;

        if (results.length === 0) {
            resultsTable.innerHTML = "<p class='no-results'>Результатов не найдено</p>";
            return;
        }

        // Определяем колонки
        const columns = Object.keys(results[0]);

        let html = "<table class='sql-results-table'><thead><tr>";
        columns.forEach(col => {
            html += `<th>${escapeHtml(col)}</th>`;
        });
        html += "</tr></thead><tbody>";

        results.forEach(row => {
            html += "<tr>";
            columns.forEach(col => {
                const value = row[col];
                html += `<td>${escapeHtml(value != null ? String(value) : '')}</td>`;
            });
            html += "</tr>";
        });

        html += "</tbody></table>";
        resultsTable.innerHTML = html;
    }

    // Экспорт CSV
    function exportCsv() {
        if (sqlQueryResults.length === 0) {
            alert("Нет данных для экспорта");
            return;
        }

        const columns = Object.keys(sqlQueryResults[0]);
        let csv = columns.join(';') + '\n';

        sqlQueryResults.forEach(row => {
            csv += columns.map(col => {
                const val = row[col];
                const str = val != null ? String(val) : '';
                // Экранируем если содержит ; или "
                if (str.includes(';') || str.includes('"')) {
                    return '"' + str.replace(/"/g, '""') + '"';
                }
                return str;
            }).join(';') + '\n';
        });

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = 'sql_query_result.csv';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    // Экспорт JSON
    function exportJson() {
        if (sqlQueryResults.length === 0) {
            alert("Нет данных для экспорта");
            return;
        }

        const json = JSON.stringify(sqlQueryResults, null, 2);
        const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = 'sql_query_result.json';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    // Примеры запросов
    function showExamples() {
        const examples = [
            {
                name: "Простой SELECT",
                query: 'SELECT host, ip, env WHERE env = "prod" ORDER BY host'
            },
            {
                name: "Группировка с агрегацией",
                query: 'SELECT env, COUNT(*) as count, MAX(version) as max_version GROUP BY env ORDER BY count DESC'
            },
            {
                name: "Фильтр с несколькими условиями",
                query: 'SELECT * WHERE env = "prod" AND version > "1.0" ORDER BY host'
            },
            {
                name: "LIKE поиск",
                query: 'SELECT host, ip WHERE host LIKE "%prod%" ORDER BY host'
            }
        ];

        const exampleText = examples.map((ex, i) =>
            `${i + 1}. ${ex.name}:\n${ex.query}`
        ).join('\n\n');

        if (queryInput) {
            queryInput.value = examples[0].query;
        }

        alert(`Примеры запросов:\n\n${exampleText}\n\n\nВыбран первый пример.`);
    }

    // События
    if (executeBtn) executeBtn.addEventListener("click", executeSQLQuery);
    if (clearBtn) {
        clearBtn.addEventListener("click", () => {
            if (queryInput) queryInput.value = "";
            if (resultsTable) resultsTable.innerHTML = "";
            if (resultsInfo) resultsInfo.textContent = "";
            sqlQueryResults = [];
        });
    }
    if (examplesBtn) examplesBtn.addEventListener("click", showExamples);
    if (exportCsvBtn) exportCsvBtn.addEventListener("click", exportCsv);
    if (exportJsonBtn) exportJsonBtn.addEventListener("click", exportJson);
    if (queryInput) {
        queryInput.addEventListener("keydown", (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                executeSQLQuery();
            }
        });
    }

    // Инициализация
    updateSheetLists();
}

/* ============================
   TEMPLATES & SCRIPT BUILDER
============================ */

let currentTemplateId = null;

function initTemplatesUI() {
    const newBtn = document.getElementById("templateNewBtn");
    const saveBtn = document.getElementById("templateSaveBtn");
    const deleteBtn = document.getElementById("templateDeleteBtn");
    const testBtn = document.getElementById("templateTestBtn");
    const filterType = document.getElementById("templateFilterType");
    const nameInput = document.getElementById("templateNameInput");
    const typeSelect = document.getElementById("templateTypeSelect");
    const categorySelect = document.getElementById("templateCategorySelect");
    const descriptionInput = document.getElementById("templateDescriptionInput");
    const contentInput = document.getElementById("templateContentInput");
    const templatesList = document.getElementById("templatesList");

    if (!templatesList) return;

    // Загрузка списка шаблонов
    async function loadTemplates() {
        try {
            let templates = await listTemplates();

            // Фильтрация по типу
            if (filterType && filterType.value) {
                templates = templates.filter(t => t.type === filterType.value);
            }

            // Сортировка по использованию и дате
            templates.sort((a, b) => {
                if (b.usageCount !== a.usageCount) {
                    return b.usageCount - a.usageCount;
                }
                return new Date(b.updatedAt) - new Date(a.updatedAt);
            });

            renderTemplatesList(templates);
        } catch (error) {
            console.error('[App] Error loading templates:', error);
        }
    }

    // Отображение списка шаблонов
    function renderTemplatesList(templates) {
        if (!templatesList) return;

        if (templates.length === 0) {
            templatesList.innerHTML = "<p class='no-results'>Нет сохраненных шаблонов</p>";
            return;
        }

        let html = "";
        templates.forEach(template => {
            html += `<div class="template-item ${currentTemplateId === template.id ? 'active' : ''}" 
                         data-template-id="${template.id}">
                <div class="template-item-header">
                    <strong>${escapeHtml(template.name)}</strong>
                    <span class="template-type-badge">${template.type}</span>
                </div>
                <div class="template-item-description">${escapeHtml(template.description || '')}</div>
                <div class="template-item-meta">
                    <span>Использований: ${template.usageCount || 0}</span>
                    <span>Обновлен: ${new Date(template.updatedAt).toLocaleDateString()}</span>
                </div>
            </div>`;
        });

        templatesList.innerHTML = html;

        // Обработчики кликов
        templatesList.querySelectorAll('.template-item').forEach(item => {
            item.addEventListener('click', () => {
                const templateId = item.dataset.templateId;
                loadTemplate(templateId);
            });
        });
    }

    // Загрузка шаблона для редактирования
    async function loadTemplate(templateId) {
        try {
            const template = await getTemplate(templateId);
            if (!template) {
                alert("Шаблон не найден");
                return;
            }

            currentTemplateId = templateId;

            if (nameInput) nameInput.value = template.name || '';
            if (typeSelect) typeSelect.value = template.type || 'generic';
            if (categorySelect) categorySelect.value = template.category || 'commands';
            if (descriptionInput) descriptionInput.value = template.description || '';
            if (contentInput) contentInput.value = template.template || '';

            loadTemplates(); // Обновляем список для подсветки активного
        } catch (error) {
            console.error('[App] Error loading template:', error);
            alert("Ошибка загрузки шаблона");
        }
    }

    // Сохранение шаблона
    async function saveTemplateHandler() {
        if (!nameInput || !contentInput) return;

        const name = nameInput.value.trim();
        const content = contentInput.value.trim();

        if (!name) {
            alert("Введите название шаблона");
            return;
        }

        if (!content) {
            alert("Введите содержимое шаблона");
            return;
        }

        // Валидация шаблона
        const validation = validateTemplate(content);
        if (!validation.valid) {
            alert("Ошибки в шаблоне:\n" + validation.errors.join('\n'));
            return;
        }

        try {
            const variables = extractVariables(content);
            const type = typeSelect ? typeSelect.value : detectTemplateType(content);
            const category = categorySelect ? categorySelect.value : detectTemplateCategory(content);

            const templateData = {
                name,
                type,
                category,
                description: descriptionInput ? descriptionInput.value.trim() : '',
                template: content,
                variables
            };

            await saveTemplate(templateData, currentTemplateId);
            currentTemplateId = null;

            // Очистка формы
            if (nameInput) nameInput.value = '';
            if (typeSelect) typeSelect.value = 'generic';
            if (categorySelect) categorySelect.value = 'commands';
            if (descriptionInput) descriptionInput.value = '';
            if (contentInput) contentInput.value = '';

            alert("Шаблон сохранен");
            loadTemplates();
        } catch (error) {
            console.error('[App] Error saving template:', error);
            alert("Ошибка сохранения шаблона");
        }
    }

    // Удаление шаблона
    async function deleteTemplateHandler() {
        if (!currentTemplateId) {
            alert("Выберите шаблон для удаления");
            return;
        }

        if (!confirm("Удалить этот шаблон?")) {
            return;
        }

        try {
            await deleteTemplate(currentTemplateId);
            currentTemplateId = null;

            // Очистка формы
            if (nameInput) nameInput.value = '';
            if (typeSelect) typeSelect.value = 'generic';
            if (categorySelect) categorySelect.value = 'commands';
            if (descriptionInput) descriptionInput.value = '';
            if (contentInput) contentInput.value = '';

            alert("Шаблон удален");
            loadTemplates();
        } catch (error) {
            console.error('[App] Error deleting template:', error);
            alert("Ошибка удаления шаблона");
        }
    }

    // Тест шаблона
    function testTemplate() {
        if (!contentInput) return;

        const template = contentInput.value.trim();
        if (!template) {
            alert("Введите шаблон для теста");
            return;
        }

        // Пример данных для теста
        const testData = {
            host: 'example.com',
            port: '8080',
            ip: '192.168.1.1',
            version: '1.0.0',
            env: 'prod'
        };

        const result = applyTemplate(template, testData);
        alert(`Результат теста:\n\n${result}`);
    }

    // Создание нового шаблона
    function newTemplate() {
        currentTemplateId = null;

        if (nameInput) nameInput.value = '';
        if (typeSelect) typeSelect.value = 'generic';
        if (categorySelect) categorySelect.value = 'commands';
        if (descriptionInput) descriptionInput.value = '';
        if (contentInput) contentInput.value = '';

        loadTemplates();
    }

    // Загрузка примера по типу
    if (typeSelect) {
        typeSelect.addEventListener('change', () => {
            if (!contentInput || contentInput.value.trim()) return; // Не перезаписываем если есть содержимое

            const example = getTemplateExample(typeSelect.value);
            if (example) {
                contentInput.value = example;
            }
        });
    }

    // События
    if (newBtn) newBtn.addEventListener("click", newTemplate);
    if (saveBtn) saveBtn.addEventListener("click", saveTemplateHandler);
    if (deleteBtn) deleteBtn.addEventListener("click", deleteTemplateHandler);
    if (testBtn) testBtn.addEventListener("click", testTemplate);
    if (filterType) {
        filterType.addEventListener("change", loadTemplates);
    }

    // Инициализация
    loadTemplates();
}

/* ============================
   DASHBOARD & ANALYTICS
============================ */

async function renderDashboard() {
    if (!indexedDBReady) {
        console.warn('[Dashboard] IndexedDB not ready');
        return;
    }

    try {
        // Загружаем данные инвентаря
        const inventory = {
            environments: await getInventory('environments'),
            hosts: await getInventory('hosts'),
            services: await getInventory('services'),
            endpoints: await getInventory('endpoints'),
            snapshots: await getInventory('snapshots')
        };

        // Overview Cards
        renderOverviewCards(inventory);

        // Version Distribution
        renderVersionDistribution(inventory);

        // Anomalies
        renderAnomalies(inventory);

        // History
        renderVersionHistory(inventory);

    } catch (error) {
        console.error('[Dashboard] Error rendering dashboard:', error);
    }
}

function renderOverviewCards(inventory) {
    const cardsContainer = document.getElementById("dashboardCards");
    if (!cardsContainer) return;

    const stats = collectOverviewStats(inventory);

    cardsContainer.innerHTML = `
        <div class="dashboard-card">
            <div class="dashboard-card-title">Environments</div>
            <div class="dashboard-card-value">${stats.environments}</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Hosts</div>
            <div class="dashboard-card-value">${stats.hosts}</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Services</div>
            <div class="dashboard-card-value">${stats.services}</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Endpoints</div>
            <div class="dashboard-card-value">${stats.endpoints}</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Snapshots</div>
            <div class="dashboard-card-value">${stats.snapshots}</div>
        </div>
    `;
}

function renderVersionDistribution(inventory) {
    // По окружениям
    const envDistribution = getVersionDistributionByEnvironment(inventory);
    const envChart = document.getElementById("versionByEnvChart");
    if (envChart) {
        const chartData = convertDistributionToChartData(envDistribution);
        renderBarChart(envChart, chartData);
    }

    // По сервисам
    const serviceDistribution = getVersionDistributionByService(inventory);
    const serviceChart = document.getElementById("versionByServiceChart");
    if (serviceChart) {
        const chartData = convertDistributionToChartData(serviceDistribution);
        renderBarChart(serviceChart, chartData);
    }
}

function convertDistributionToChartData(distribution) {
    // Получаем все уникальные версии
    const allVersions = new Set();
    Object.values(distribution).forEach(versions => {
        Object.keys(versions).forEach(version => allVersions.add(version));
    });

    const labels = Object.keys(distribution);
    const datasets = Array.from(allVersions).map((version, index) => ({
        label: version,
        values: labels.map(label => distribution[label][version] || 0),
        color: getColorForVersion(version, index)
    }));

    return { labels, datasets };
}

function getColorForVersion(version, index) {
    const colors = [
        '#4CAF50', '#2196F3', '#FF9800', '#F44336',
        '#9C27B0', '#00BCD4', '#FFEB3B', '#795548'
    ];
    return colors[index % colors.length];
}

function renderAnomalies(inventory) {
    const anomaliesContainer = document.getElementById("dashboardAnomalies");
    if (!anomaliesContainer) return;

    const anomalies = detectAnomalies(inventory, { outdatedDays: 30 });

    let html = '';

    // Отстающие версии
    if (anomalies.outdated.length > 0) {
        html += `<div class="anomaly-section">
            <h4>Outdated Versions (${anomalies.outdated.length})</h4>
            <table class="anomaly-table">
                <thead>
                    <tr>
                        <th>Service</th>
                        <th>Environment</th>
                        <th>Version</th>
                        <th>Last Update</th>
                        <th>Days Ago</th>
                    </tr>
                </thead>
                <tbody>`;

        anomalies.outdated.slice(0, 10).forEach(item => {
            html += `<tr>
                <td>${escapeHtml(item.service)}</td>
                <td>${escapeHtml(item.environment)}</td>
                <td>${escapeHtml(item.version)}</td>
                <td>${new Date(item.lastUpdate).toLocaleDateString()}</td>
                <td>${item.daysAgo}</td>
            </tr>`;
        });

        html += `</tbody></table></div>`;
    }

    // Несоответствия
    if (anomalies.inconsistent.length > 0) {
        html += `<div class="anomaly-section">
            <h4>Inconsistent Versions (${anomalies.inconsistent.length})</h4>
            <table class="anomaly-table">
                <thead>
                    <tr>
                        <th>Service</th>
                        <th>Environment</th>
                        <th>Version</th>
                        <th>Expected</th>
                    </tr>
                </thead>
                <tbody>`;

        anomalies.inconsistent.slice(0, 10).forEach(item => {
            html += `<tr>
                <td>${escapeHtml(item.service)}</td>
                <td>${escapeHtml(item.environment)}</td>
                <td>${escapeHtml(item.version)}</td>
                <td>${escapeHtml(item.expected)}</td>
            </tr>`;
        });

        html += `</tbody></table></div>`;
    }

    // Отсутствующие версии
    if (anomalies.missing.length > 0) {
        html += `<div class="anomaly-section">
            <h4>Missing Versions (${anomalies.missing.length})</h4>
            <table class="anomaly-table">
                <thead>
                    <tr>
                        <th>Service</th>
                        <th>Environment</th>
                        <th>Endpoint</th>
                    </tr>
                </thead>
                <tbody>`;

        anomalies.missing.slice(0, 10).forEach(item => {
            html += `<tr>
                <td>${escapeHtml(item.service)}</td>
                <td>${escapeHtml(item.environment)}</td>
                <td>${escapeHtml(item.endpoint)}</td>
            </tr>`;
        });

        html += `</tbody></table></div>`;
    }

    if (!html) {
        html = '<p class="no-results">No anomalies detected</p>';
    }

    anomaliesContainer.innerHTML = html;
}

function renderVersionHistory(inventory) {
    const historyContainer = document.getElementById("dashboardHistory");
    if (!historyContainer) return;

    const history = getVersionHistory(inventory);

    if (history.length === 0) {
        historyContainer.innerHTML = '<p class="no-results">No version history available</p>';
        return;
    }

    // Группируем по дате
    const historyByDate = {};
    history.forEach(item => {
        if (!historyByDate[item.date]) {
            historyByDate[item.date] = [];
        }
        historyByDate[item.date].push(item);
    });

    let html = '<table class="history-table"><thead><tr><th>Date</th><th>Service</th><th>Environment</th><th>Version</th><th>Build</th></tr></thead><tbody>';

    Object.keys(historyByDate).sort().reverse().slice(0, 50).forEach(date => {
        historyByDate[date].forEach(item => {
            html += `<tr>
                <td>${item.date}</td>
                <td>${escapeHtml(item.service)}</td>
                <td>${escapeHtml(item.environment)}</td>
                <td>${escapeHtml(item.version)}</td>
                <td>${escapeHtml(item.build || '')}</td>
            </tr>`;
        });
    });

    html += '</tbody></table>';
    historyContainer.innerHTML = html;
}
