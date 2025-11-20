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
    getTemplatesByCategory,
    saveDataset,
    getDataset,
    listDatasets,
    deleteDataset,
    saveJob,
    getJob,
    listJobs,
    getJobsByStatus,
    deleteJob,
    saveScanResult,
    getScanResult,
    getScanResultsByJob,
    getScanResultsByEndpoint,
    listScanResults,
    deleteScanResult,
    deleteScanResultsByJob
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
import {
    createJob,
    updateJob,
    validateJob,
    createDefaultJob
} from './scan/job-model.js';
import {
    createScanResult,
    extractVersionFromResponse
} from './scan/result-model.js';

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
        if (indexedDBReady) {
            console.log('[App] IndexedDB initialized successfully');
        } else {
            console.warn('[App] IndexedDB initialization failed, using localStorage fallback');
            if (result.error) {
                console.warn('[App] Initialization error:', result.error);
            }
            console.warn('[App] ⚠️  Fallback limitations:');
            console.warn('[App]   - Excel files will not persist after page reload');
            console.warn('[App]   - Templates, Datasets, Jobs will not be saved');
            console.warn('[App]   - Only inventory data (environments, hosts, services, endpoints, snapshots) will be saved');
        }

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
            updateDatasetSelect();
        }
        if (id === "sql") {
            initSQLUI();
        }
        if (id === "templates") {
            initTemplatesUI();
        }
        if (id === "datasets") {
            initDatasetsUI();
        }
        if (id === "scan") {
            initScanUI();
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
const excelDatasetSelect = document.getElementById("excelDatasetSelect");
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
    // Сбрасываем Dataset при выборе листа
    if (excelDatasetSelect) excelDatasetSelect.value = '';
    renderExcelSheet();
    renderExcelImportMapping();
    updateZbxColumnSelects();
});

// Обработчик выбора Dataset
if (excelDatasetSelect) {
    excelDatasetSelect.addEventListener("change", async () => {
        const datasetId = excelDatasetSelect.value;
        if (datasetId) {
            await loadDatasetToExcelWorkspace(datasetId);
            await renderExcelImportMapping(); // Обновляем маппинг для импорта
        } else {
            // Сбрасываем на обычный режим
            excelState.activeSheetName = excelSheetSelect.value || null;
            renderExcelSheet();
            await renderExcelImportMapping(); // Обновляем маппинг для импорта
        }
    });
}
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

// Обновление списка Datasets в Excel Workspace
async function updateDatasetSelect() {
    if (!excelDatasetSelect) return;

    // Ждем инициализации IndexedDB (максимум 1 секунда)
    if (!indexedDBReady && !isAvailable()) {
        let attempts = 0;
        while (attempts < 10 && !indexedDBReady && !isAvailable()) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }

        if (!indexedDBReady && !isAvailable()) {
            // Просто оставляем пустой список, не логируем как ошибку
            excelDatasetSelect.innerHTML = '<option value="">Нет</option>';
            return;
        }
    }

    try {
        const datasets = await listDatasets();
        excelDatasetSelect.innerHTML = '<option value="">Нет</option>';
        datasets.forEach(dataset => {
            const option = document.createElement('option');
            option.value = dataset.id;
            option.textContent = dataset.name;
            excelDatasetSelect.appendChild(option);
        });
    } catch (error) {
        console.error('[Excel] Error updating dataset select:', error);
    }
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

// Загрузка Dataset в Excel Workspace
async function loadDatasetToExcelWorkspace(datasetId) {
    try {
        const dataset = await getDataset(datasetId);
        if (!dataset) {
            alert("Dataset не найден");
            return;
        }

        const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
        if (!sheet || !sheet.rows) {
            alert("Нет данных в Dataset");
            return;
        }

        // Применяем выбранные колонки и переименования
        const selectedColumnNames = Object.keys(dataset.columns);
        const columnMapping = dataset.columns;

        const transformedRows = sheet.rows.map(row => {
            const newRow = {};
            selectedColumnNames.forEach(origCol => {
                const customName = columnMapping[origCol];
                newRow[customName] = row[origCol] ?? '';
            });
            return newRow;
        });

        const transformedColumns = Object.values(columnMapping);

        // Рендерим данные Dataset
        renderDatasetInExcelWorkspace(transformedColumns, transformedRows);
    } catch (error) {
        console.error('[Excel] Error loading dataset:', error);
        alert("Ошибка загрузки Dataset");
    }
}

// Рендеринг Dataset в Excel Workspace
function renderDatasetInExcelWorkspace(columns, rows) {
    excelColumnsList.innerHTML = "";
    excelDataTable.innerHTML = "";

    if (!columns || columns.length === 0) return;

    // Колонки (чекбоксы)
    let colsHtml = "";
    columns.forEach(col => {
        colsHtml += `<li>
            <label><input type="checkbox" data-col="${col}" checked> ${col}</label>
        </li>`;
    });
    excelColumnsList.innerHTML = colsHtml;

    // Добавляем обработчики на чекбоксы
    excelColumnsList.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
        checkbox.addEventListener('change', () => {
            toggleExcelColumnVisibility(checkbox.dataset.col, checkbox.checked);
        });
    });

    // Таблица предпросмотра
    let html = "<tr>";
    columns.forEach(col => {
        html += `<th data-col="${col}">${col}</th>`;
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
            html += `<td data-col="${col}">${escapeHtml(val.toString())}</td>`;
        });
        html += "</tr>";
    });

    excelDataTable.innerHTML = html;
}

function renderExcelSheet() {
    // Если выбран Dataset, не рендерим обычный лист
    if (excelDatasetSelect && excelDatasetSelect.value) {
        return;
    }

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

    // Добавляем обработчики на чекбоксы для управления видимостью колонок
    excelColumnsList.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
        checkbox.addEventListener('change', () => {
            toggleExcelColumnVisibility(checkbox.dataset.col, checkbox.checked);
        });
    });

    // таблица предпросмотра
    let html = "<tr>";
    columns.forEach(col => {
        html += `<th data-col="${col}">${col}</th>`;
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
            html += `<td data-col="${col}">${escapeHtml(val.toString())}</td>`;
        });
        html += "</tr>";
    });

    excelDataTable.innerHTML = html;
}

/**
 * Управление видимостью колонок в таблице Excel на основе состояния чекбоксов
 * @param {string} columnName - Название колонки
 * @param {boolean} visible - Видима ли колонка
 */
function toggleExcelColumnVisibility(columnName, visible) {
    if (!excelDataTable) return;

    // Находим все th и td с data-col равным columnName
    const elements = excelDataTable.querySelectorAll(`th[data-col="${columnName}"], td[data-col="${columnName}"]`);
    elements.forEach(el => {
        el.style.display = visible ? '' : 'none';
    });
}

async function buildExcelOutput() {
    const template = excelTemplateInput.value;
    if (!template.trim()) {
        alert("Шаблон пустой");
        return;
    }

    let rows = [];
    let columns = [];

    // Проверяем, используется ли Dataset
    if (excelDatasetSelect && excelDatasetSelect.value) {
        try {
            const dataset = await getDataset(excelDatasetSelect.value);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
            if (!sheet || !sheet.rows) {
                alert("Нет данных в Dataset");
                return;
            }

            // Применяем выбранные колонки и переименования
            const selectedColumnNames = Object.keys(dataset.columns);
            const columnMapping = dataset.columns;

            rows = sheet.rows.map(row => {
                const newRow = {};
                selectedColumnNames.forEach(origCol => {
                    const customName = columnMapping[origCol];
                    newRow[customName] = row[origCol] ?? '';
                });
                return newRow;
            });

            columns = Object.values(columnMapping);
        } catch (error) {
            console.error('[Excel] Error loading dataset:', error);
            alert("Ошибка загрузки Dataset");
            return;
        }
    } else {
        // Используем обычный Excel лист
        const sheet = getActiveExcelSheet();
        if (!sheet) {
            alert("Сначала загрузите Excel и выберите лист, или выберите Dataset");
            return;
        }

        columns = sheet.columns;
        rows = sheet.rows;
    }

    // Применяем фильтр
    const filterText = excelState.filterText;
    const filteredRows = rows.filter(row => {
        if (!filterText) return true;
        return columns.some(col => {
            const val = (row[col] ?? "").toString().toLowerCase();
            return val.includes(filterText);
        });
    });

    // Применяем шаблон
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

async function renderExcelImportMapping() {
    excelImportMappingTable.innerHTML = "";

    let columns = [];

    // Проверяем, используется ли Dataset
    if (excelDatasetSelect && excelDatasetSelect.value) {
        try {
            const dataset = await getDataset(excelDatasetSelect.value);
            if (!dataset) {
                return;
            }

            // Используем переименованные колонки из Dataset
            columns = Object.values(dataset.columns);
        } catch (error) {
            console.error('[Excel] Error loading dataset for mapping:', error);
            return;
        }
    } else {
        // Используем обычный Excel лист
        const sheet = getActiveExcelSheet();
        if (!sheet) return;
        columns = sheet.columns;
    }

    const entity = excelImportEntitySelect.value;
    const fields = schemas[entity];

    let html = "<tr><th>Поле сущности</th><th>Колонка Excel/Dataset</th></tr>";
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
    const entity = excelImportEntitySelect.value;
    const fields = schemas[entity];
    const mapping = {};

    excelImportMappingTable.querySelectorAll("select[data-field]").forEach(sel => {
        const field = sel.dataset.field;
        mapping[field] = sel.value || "";
    });

    let rows = [];
    let columns = [];

    // Проверяем, используется ли Dataset
    if (excelDatasetSelect && excelDatasetSelect.value) {
        try {
            const dataset = await getDataset(excelDatasetSelect.value);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
            if (!sheet || !sheet.rows) {
                alert("Нет данных в Dataset");
                return;
            }

            // Применяем выбранные колонки и переименования
            const selectedColumnNames = Object.keys(dataset.columns);
            const columnMapping = dataset.columns;

            rows = sheet.rows.map(row => {
                const newRow = {};
                selectedColumnNames.forEach(origCol => {
                    const customName = columnMapping[origCol];
                    newRow[customName] = row[origCol] ?? '';
                });
                return newRow;
            });

            columns = Object.values(columnMapping);
        } catch (error) {
            console.error('[Excel] Error loading dataset:', error);
            alert("Ошибка загрузки Dataset");
            return;
        }
    } else {
        // Используем обычный Excel лист
        const sheet = getActiveExcelSheet();
        if (!sheet) {
            alert("Сначала загрузите Excel и выберите лист, или выберите Dataset");
            return;
        }

        columns = sheet.columns;
        rows = sheet.rows;
    }

    // Применяем фильтр
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
   DATASETS (Block B)
============================ */

let currentDatasetId = null;

function initDatasetsUI() {
    const newBtn = document.getElementById("datasetNewBtn");
    const saveBtn = document.getElementById("datasetSaveBtn");
    const deleteBtn = document.getElementById("datasetDeleteBtn");
    const previewBtn = document.getElementById("datasetPreviewBtn");
    const nameInput = document.getElementById("datasetNameInput");
    const descriptionInput = document.getElementById("datasetDescriptionInput");
    const sourceFileSelect = document.getElementById("datasetSourceFile");
    const sourceSheetSelect = document.getElementById("datasetSourceSheet");
    const columnsList = document.getElementById("datasetColumnsList");
    const datasetsList = document.getElementById("datasetsList");
    const previewTable = document.getElementById("datasetPreviewTable");

    if (!datasetsList) return;

    // Загрузка списка Datasets
    async function loadDatasets() {
        // Ждем инициализации IndexedDB (максимум 1 секунда)
        if (!indexedDBReady && !isAvailable()) {
            let attempts = 0;
            while (attempts < 10 && !indexedDBReady && !isAvailable()) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }

            if (!indexedDBReady && !isAvailable()) {
                // Просто показываем пустой список
                renderDatasetsList([]);
                return;
            }
        }

        try {
            const datasets = await listDatasets();
            datasets.sort((a, b) => new Date(b.updatedAt || b.createdAt) - new Date(a.updatedAt || a.createdAt));
            renderDatasetsList(datasets);
        } catch (error) {
            console.error('[Datasets] Error loading datasets:', error);
            renderDatasetsList([]);
        }
    }

    // Отображение списка Datasets
    function renderDatasetsList(datasets) {
        if (!datasetsList) return;

        if (datasets.length === 0) {
            datasetsList.innerHTML = "<p class='no-results'>Нет сохраненных Datasets</p>";
            return;
        }

        let html = "";
        datasets.forEach(dataset => {
            html += `<div class="dataset-item ${currentDatasetId === dataset.id ? 'active' : ''}" 
                         data-dataset-id="${dataset.id}">
                <div class="dataset-item-name">${escapeHtml(dataset.name)}</div>
                <div class="dataset-item-description">${escapeHtml(dataset.description || '')}</div>
            </div>`;
        });

        datasetsList.innerHTML = html;

        // Обработчики кликов
        datasetsList.querySelectorAll('.dataset-item').forEach(item => {
            item.addEventListener('click', () => {
                const datasetId = item.dataset.datasetId;
                loadDataset(datasetId);
            });
        });
    }

    // Загрузка Dataset для редактирования
    async function loadDataset(datasetId) {
        try {
            const dataset = await getDataset(datasetId);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            currentDatasetId = datasetId;
            if (nameInput) nameInput.value = dataset.name || '';
            if (descriptionInput) descriptionInput.value = dataset.description || '';

            // Загружаем файлы и листы
            await updateFileAndSheetLists(dataset.sourceFileId, dataset.sourceSheetName);

            // Загружаем колонки
            await loadColumnsForSheet(dataset.sourceFileId, dataset.sourceSheetName, dataset.columns);

            // Автоматически показываем предпросмотр
            await previewDataset();

            // Обновляем список
            loadDatasets();
        } catch (error) {
            console.error('[Datasets] Error loading dataset:', error);
            alert("Ошибка загрузки Dataset");
        }
    }

    // Обновление списков файлов и листов
    async function updateFileAndSheetLists(selectedFileId = null, selectedSheetName = null) {
        try {
            const files = await listFiles();

            if (sourceFileSelect) {
                sourceFileSelect.innerHTML = '<option value="">Выберите файл...</option>';
                files.forEach(file => {
                    const option = document.createElement('option');
                    option.value = file.id;
                    option.textContent = file.name;
                    if (selectedFileId && file.id === selectedFileId) {
                        option.selected = true;
                    }
                    sourceFileSelect.appendChild(option);
                });
            }

            // При изменении файла обновляем список листов
            if (sourceFileSelect) {
                sourceFileSelect.addEventListener('change', async (e) => {
                    const fileId = e.target.value;
                    if (fileId) {
                        await updateSheetList(fileId);
                    } else {
                        if (sourceSheetSelect) {
                            sourceSheetSelect.innerHTML = '<option value="">Выберите лист...</option>';
                        }
                        if (columnsList) columnsList.innerHTML = '';
                    }
                });
            }

            // Если выбран файл, обновляем листы
            if (selectedFileId) {
                await updateSheetList(selectedFileId, selectedSheetName);
            }
        } catch (error) {
            console.error('[Datasets] Error updating file/sheet lists:', error);
        }
    }

    // Обновление списка листов
    async function updateSheetList(fileId, selectedSheetName = null) {
        try {
            const sheets = await listSheets(fileId);

            if (sourceSheetSelect) {
                sourceSheetSelect.innerHTML = '<option value="">Выберите лист...</option>';
                sheets.forEach(sheetName => {
                    const option = document.createElement('option');
                    option.value = sheetName;
                    option.textContent = sheetName;
                    if (selectedSheetName && sheetName === selectedSheetName) {
                        option.selected = true;
                    }
                    sourceSheetSelect.appendChild(option);
                });
            }

            // При изменении листа загружаем колонки (обработчик добавляется один раз)
            if (sourceSheetSelect && !sourceSheetSelect.dataset.listenerAdded) {
                sourceSheetSelect.addEventListener('change', async (e) => {
                    const sheetName = e.target.value;
                    if (sheetName && sourceFileSelect && sourceFileSelect.value) {
                        await loadColumnsForSheet(sourceFileSelect.value, sheetName);
                    } else {
                        if (columnsList) columnsList.innerHTML = '';
                    }
                });
                sourceSheetSelect.dataset.listenerAdded = 'true';
            }

            // Если выбран лист, загружаем колонки
            if (selectedSheetName) {
                await loadColumnsForSheet(fileId, selectedSheetName);
            }
        } catch (error) {
            console.error('[Datasets] Error updating sheet list:', error);
        }
    }

    // Загрузка колонок для листа
    async function loadColumnsForSheet(fileId, sheetName, savedColumns = null) {
        try {
            const sheet = await getSheet(fileId, sheetName);
            if (!sheet || !sheet.columns) {
                if (columnsList) columnsList.innerHTML = '<p class="hint">Лист не найден или не содержит колонок</p>';
                return;
            }

            if (!columnsList) return;

            let html = '';
            sheet.columns.forEach(col => {
                // savedColumns - это объект { originalName: customName } или null
                const isSelected = savedColumns ? (col in savedColumns) : true;
                const customName = savedColumns && savedColumns[col] ? savedColumns[col] : '';
                html += `
                    <div class="dataset-column-item">
                        <input type="checkbox" data-column="${col}" ${isSelected ? 'checked' : ''}>
                        <span>${escapeHtml(col)}</span>
                        <input type="text" data-column="${col}" placeholder="Новое имя (опционально)" value="${escapeHtml(customName)}">
                    </div>
                `;
            });

            columnsList.innerHTML = html;
        } catch (error) {
            console.error('[Datasets] Error loading columns:', error);
            if (columnsList) columnsList.innerHTML = '<p class="hint">Ошибка загрузки колонок</p>';
        }
    }

    // Сохранение Dataset
    async function saveCurrentDataset() {
        if (!nameInput || !nameInput.value.trim()) {
            alert("Введите название Dataset");
            return;
        }

        if (!sourceFileSelect || !sourceFileSelect.value) {
            alert("Выберите файл-источник");
            return;
        }

        if (!sourceSheetSelect || !sourceSheetSelect.value) {
            alert("Выберите лист");
            return;
        }

        try {
            // Собираем выбранные колонки
            const selectedColumns = {};
            const columnCheckboxes = columnsList.querySelectorAll('input[type="checkbox"]:checked');

            if (columnCheckboxes.length === 0) {
                alert("Выберите хотя бы одну колонку");
                return;
            }

            columnCheckboxes.forEach(checkbox => {
                const colName = checkbox.dataset.column;
                const customNameInput = columnsList.querySelector(`input[type="text"][data-column="${colName}"]`);
                const customName = customNameInput ? customNameInput.value.trim() : '';
                selectedColumns[colName] = customName || colName;
            });

            const dataset = {
                id: currentDatasetId || undefined,
                name: nameInput.value.trim(),
                description: descriptionInput.value.trim() || '',
                sourceFileId: sourceFileSelect.value,
                sourceSheetName: sourceSheetSelect.value,
                columns: selectedColumns,
                filters: [] // Фильтры будут добавлены позже
            };

            const savedId = await saveDataset(dataset);
            currentDatasetId = savedId;

            alert("Dataset сохранен");
            loadDatasets();
        } catch (error) {
            console.error('[Datasets] Error saving dataset:', error);
            alert("Ошибка сохранения Dataset");
        }
    }

    // Удаление Dataset
    async function deleteCurrentDataset() {
        if (!currentDatasetId) {
            alert("Выберите Dataset для удаления");
            return;
        }

        if (!confirm("Удалить этот Dataset?")) {
            return;
        }

        try {
            await deleteDataset(currentDatasetId);
            currentDatasetId = null;
            if (nameInput) nameInput.value = '';
            if (descriptionInput) descriptionInput.value = '';
            if (sourceFileSelect) sourceFileSelect.value = '';
            if (sourceSheetSelect) sourceSheetSelect.innerHTML = '<option value="">Выберите лист...</option>';
            if (columnsList) columnsList.innerHTML = '';
            if (previewTable) previewTable.innerHTML = '<p class="hint">Выберите Dataset для предпросмотра</p>';

            loadDatasets();
            alert("Dataset удален");
        } catch (error) {
            console.error('[Datasets] Error deleting dataset:', error);
            alert("Ошибка удаления Dataset");
        }
    }

    // Создание нового Dataset
    function createNewDataset() {
        currentDatasetId = null;
        if (nameInput) nameInput.value = '';
        if (descriptionInput) descriptionInput.value = '';
        if (sourceFileSelect) sourceFileSelect.value = '';
        if (sourceSheetSelect) sourceSheetSelect.innerHTML = '<option value="">Выберите лист...</option>';
        if (columnsList) columnsList.innerHTML = '';
        if (previewTable) previewTable.innerHTML = '<p class="hint">Выберите Dataset для предпросмотра</p>';
        loadDatasets();
        updateFileAndSheetLists();
    }

    // Предпросмотр Dataset
    async function previewDataset() {
        if (!currentDatasetId) {
            alert("Выберите Dataset для предпросмотра");
            return;
        }

        try {
            const dataset = await getDataset(currentDatasetId);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            // Загружаем данные листа
            const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
            if (!sheet || !sheet.rows) {
                if (previewTable) previewTable.innerHTML = '<p class="hint">Лист не найден или пуст</p>';
                return;
            }

            // Применяем выбранные колонки
            const selectedColumnNames = Object.keys(dataset.columns);
            const columnMapping = dataset.columns; // { originalName: customName }

            // Формируем данные для таблицы
            const tableData = sheet.rows.map(row => {
                const newRow = {};
                selectedColumnNames.forEach(origCol => {
                    const customName = columnMapping[origCol];
                    newRow[customName] = row[origCol] ?? '';
                });
                return newRow;
            });

            // Рендерим таблицу
            renderDatasetPreview(tableData, Object.values(columnMapping));
        } catch (error) {
            console.error('[Datasets] Error previewing dataset:', error);
            alert("Ошибка предпросмотра Dataset");
        }
    }

    // Рендеринг таблицы предпросмотра
    function renderDatasetPreview(data, columnNames) {
        if (!previewTable) return;

        if (data.length === 0) {
            previewTable.innerHTML = '<p class="hint">Нет данных для отображения</p>';
            return;
        }

        let html = '<table><thead><tr>';
        columnNames.forEach(col => {
            html += `<th>${escapeHtml(col)}</th>`;
        });
        html += '</tr></thead><tbody>';

        const maxRows = 500; // Ограничение для производительности
        data.slice(0, maxRows).forEach(row => {
            html += '<tr>';
            columnNames.forEach(col => {
                const val = row[col] ?? '';
                html += `<td>${escapeHtml(val.toString())}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';

        if (data.length > maxRows) {
            html += `<p class="hint">Показано ${maxRows} из ${data.length} строк</p>`;
        }

        previewTable.innerHTML = html;
    }

    // Экспорт Dataset в CSV
    async function exportDatasetToCsv() {
        if (!currentDatasetId) {
            alert("Выберите Dataset для экспорта");
            return;
        }

        try {
            const dataset = await getDataset(currentDatasetId);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
            if (!sheet || !sheet.rows) {
                alert("Нет данных для экспорта");
                return;
            }

            const selectedColumnNames = Object.keys(dataset.columns);
            const columnMapping = dataset.columns;

            // Формируем CSV
            const delimiter = ';';
            const safe = (val) => {
                if (val == null) return '';
                const s = String(val);
                if (s.includes(delimiter) || s.includes('"') || /\r|\n/.test(s)) {
                    return '"' + s.replace(/"/g, '""') + '"';
                }
                return s;
            };

            const header = Object.values(columnMapping).map(col => safe(col)).join(delimiter);
            const body = sheet.rows.map(row => {
                return selectedColumnNames.map(origCol => safe(row[origCol] ?? '')).join(delimiter);
            }).join('\n');

            const csv = header + '\n' + body;
            const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `${dataset.name || 'dataset'}.csv`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        } catch (error) {
            console.error('[Datasets] Error exporting to CSV:', error);
            alert("Ошибка экспорта в CSV");
        }
    }

    // Экспорт Dataset в JSON
    async function exportDatasetToJson() {
        if (!currentDatasetId) {
            alert("Выберите Dataset для экспорта");
            return;
        }

        try {
            const dataset = await getDataset(currentDatasetId);
            if (!dataset) {
                alert("Dataset не найден");
                return;
            }

            const sheet = await getSheet(dataset.sourceFileId, dataset.sourceSheetName);
            if (!sheet || !sheet.rows) {
                alert("Нет данных для экспорта");
                return;
            }

            const selectedColumnNames = Object.keys(dataset.columns);
            const columnMapping = dataset.columns;

            // Формируем JSON данные
            const jsonData = sheet.rows.map(row => {
                const newRow = {};
                selectedColumnNames.forEach(origCol => {
                    const customName = columnMapping[origCol];
                    newRow[customName] = row[origCol] ?? '';
                });
                return newRow;
            });

            const json = JSON.stringify(jsonData, null, 2);
            const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `${dataset.name || 'dataset'}.json`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        } catch (error) {
            console.error('[Datasets] Error exporting to JSON:', error);
            alert("Ошибка экспорта в JSON");
        }
    }

    // Обработчики событий
    if (newBtn) newBtn.addEventListener('click', createNewDataset);
    if (saveBtn) saveBtn.addEventListener('click', saveCurrentDataset);
    if (deleteBtn) deleteBtn.addEventListener('click', deleteCurrentDataset);
    if (previewBtn) previewBtn.addEventListener('click', previewDataset);

    const exportCsvBtn = document.getElementById("datasetExportCsvBtn");
    const exportJsonBtn = document.getElementById("datasetExportJsonBtn");
    if (exportCsvBtn) exportCsvBtn.addEventListener('click', exportDatasetToCsv);
    if (exportJsonBtn) exportJsonBtn.addEventListener('click', exportDatasetToJson);

    // Инициализация
    loadDatasets();
    updateFileAndSheetLists();
}

/* ============================
   DASHBOARD & ANALYTICS
============================ */

// Состояние фильтров Dashboard
let dashboardFilters = {
    environment: '',
    service: ''
};

async function renderDashboard() {
    // Ждем инициализации IndexedDB (максимум 2 секунды)
    if (!indexedDBReady && !isAvailable()) {
        // Пробуем подождать немного
        let attempts = 0;
        while (attempts < 20 && !indexedDBReady && !isAvailable()) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
    }

    try {
        // Загружаем данные инвентаря (getInventory автоматически использует fallback)
        const inventory = {
            environments: await getInventory('environments'),
            hosts: await getInventory('hosts'),
            services: await getInventory('services'),
            endpoints: await getInventory('endpoints'),
            snapshots: await getInventory('snapshots')
        };

        // Загружаем данные по опросам (Jobs и ScanResults)
        let scanStats = null;
        try {
            const jobs = await listJobs();
            const scanResults = await listScanResults();
            
            // Статистика по опросам
            const successfulScans = scanResults.filter(r => r.status === 'success').length;
            const failedScans = scanResults.filter(r => r.status === 'error' || r.status === 'timeout').length;
            const recentScans = scanResults.filter(r => {
                const scanDate = new Date(r.timestamp);
                const dayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
                return scanDate > dayAgo;
            }).length;

            scanStats = {
                jobs: jobs.length,
                activeJobs: jobs.filter(j => j.status === 'active').length,
                totalScans: scanResults.length,
                successfulScans,
                failedScans,
                recentScans
            };
        } catch (error) {
            console.warn('[Dashboard] Error loading scan stats:', error);
            scanStats = null;
        }

        // Инициализируем фильтры (заполняем select'ы)
        initDashboardFilters(inventory);

        // Применяем фильтры к данным
        const filteredInventory = applyDashboardFilters(inventory);

        // Overview Cards
        renderOverviewCards(filteredInventory, scanStats);

        // Version Distribution
        renderVersionDistribution(filteredInventory);

        // Anomalies
        renderAnomalies(filteredInventory);

        // History
        renderVersionHistory(filteredInventory);

    } catch (error) {
        console.error('[Dashboard] Error rendering dashboard:', error);
    }
}

function renderOverviewCards(inventory, scanStats = null) {
    const cardsContainer = document.getElementById("dashboardCards");
    if (!cardsContainer) return;

    const stats = collectOverviewStats(inventory);

    let html = `
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

    // Добавляем статистику по опросам, если доступна
    if (scanStats) {
        html += `
        <div class="dashboard-card">
            <div class="dashboard-card-title">Scan Jobs</div>
            <div class="dashboard-card-value">${scanStats.jobs}</div>
            <div class="dashboard-card-subtitle">${scanStats.activeJobs} active</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Total Scans</div>
            <div class="dashboard-card-value">${scanStats.totalScans}</div>
            <div class="dashboard-card-subtitle">${scanStats.recentScans} last 24h</div>
        </div>
        <div class="dashboard-card">
            <div class="dashboard-card-title">Success Rate</div>
            <div class="dashboard-card-value">${scanStats.totalScans > 0 ? Math.round((scanStats.successfulScans / scanStats.totalScans) * 100) : 0}%</div>
            <div class="dashboard-card-subtitle">${scanStats.successfulScans}/${scanStats.totalScans}</div>
        </div>
        `;
    }

    cardsContainer.innerHTML = html;
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

/**
 * Инициализация фильтров Dashboard (заполнение select'ов)
 */
function initDashboardFilters(inventory) {
    const envSelect = document.getElementById("dashboardFilterEnvironment");
    const serviceSelect = document.getElementById("dashboardFilterService");

    if (!envSelect || !serviceSelect) return;

    // Заполняем окружения
    envSelect.innerHTML = '<option value="">Все окружения</option>';
    (inventory.environments || []).forEach(env => {
        const option = document.createElement('option');
        option.value = env.id;
        option.textContent = env.name || env.id;
        if (env.id === dashboardFilters.environment) {
            option.selected = true;
        }
        envSelect.appendChild(option);
    });

    // Заполняем сервисы
    serviceSelect.innerHTML = '<option value="">Все сервисы</option>';
    (inventory.services || []).forEach(service => {
        const option = document.createElement('option');
        option.value = service.id;
        option.textContent = service.name || service.id;
        if (service.id === dashboardFilters.service) {
            option.selected = true;
        }
        serviceSelect.appendChild(option);
    });

    // Обработчики событий
    envSelect.addEventListener('change', (e) => {
        dashboardFilters.environment = e.target.value;
        renderDashboard();
    });

    serviceSelect.addEventListener('change', (e) => {
        dashboardFilters.service = e.target.value;
        renderDashboard();
    });

    const resetBtn = document.getElementById("dashboardResetFilters");
    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            dashboardFilters.environment = '';
            dashboardFilters.service = '';
            renderDashboard();
        });
    }
}

/**
 * Применение фильтров к данным инвентаря
 */
function applyDashboardFilters(inventory) {
    if (!dashboardFilters.environment && !dashboardFilters.service) {
        return inventory; // Нет фильтров
    }

    const filtered = {
        environments: inventory.environments || [],
        hosts: inventory.hosts || [],
        services: inventory.services || [],
        endpoints: [],
        snapshots: []
    };

    // Фильтруем endpoints по окружению и сервису
    const endpoints = (inventory.endpoints || []).filter(ep => {
        if (dashboardFilters.environment && ep.envId !== dashboardFilters.environment) {
            return false;
        }
        if (dashboardFilters.service && ep.serviceId !== dashboardFilters.service) {
            return false;
        }
        return true;
    });

    filtered.endpoints = endpoints;

    // Получаем ID отфильтрованных endpoints
    const filteredEndpointIds = new Set(endpoints.map(ep => ep.id));

    // Фильтруем snapshots по отфильтрованным endpoints
    filtered.snapshots = (inventory.snapshots || []).filter(snapshot => {
        return filteredEndpointIds.has(snapshot.endpointId);
    });

    return filtered;
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

/* ============================
   SCAN / JOBS UI (v0.9)
============================ */

let currentJobId = null;

function initScanUI() {
    const newJobBtn = document.getElementById("scanNewJobBtn");
    const saveJobBtn = document.getElementById("scanJobSaveBtn");
    const deleteJobBtn = document.getElementById("scanJobDeleteBtn");
    const runJobBtn = document.getElementById("scanJobRunBtn");
    const jobNameInput = document.getElementById("scanJobNameInput");
    const jobDescriptionInput = document.getElementById("scanJobDescriptionInput");
    const jobTypeSelect = document.getElementById("scanJobTypeSelect");
    const jobTemplateSelect = document.getElementById("scanJobTemplateSelect");
    const jobStatusSelect = document.getElementById("scanJobStatusSelect");
    const jobsList = document.getElementById("scanJobsList");
    const targetsPanel = document.getElementById("scanJobTargetsPanel");

    if (!jobsList) return;

    // Загрузка списка Jobs
    async function loadJobs() {
        // Ждем инициализации IndexedDB (максимум 1 секунда)
        if (!indexedDBReady && !isAvailable()) {
            let attempts = 0;
            while (attempts < 10 && !indexedDBReady && !isAvailable()) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }

            if (!indexedDBReady && !isAvailable()) {
                renderJobsList([]);
                return;
            }
        }

        try {
            const jobs = await listJobs();
            jobs.sort((a, b) => new Date(b.updatedAt || b.createdAt) - new Date(a.updatedAt || a.createdAt));
            renderJobsList(jobs);
        } catch (error) {
            console.error('[Scan] Error loading jobs:', error);
            renderJobsList([]);
        }
    }

    // Отображение списка Jobs
    function renderJobsList(jobs) {
        if (!jobsList) return;

        if (jobs.length === 0) {
            jobsList.innerHTML = "<p class='no-results'>Нет сохраненных Jobs</p>";
            return;
        }

        let html = "";
        jobs.forEach(job => {
            const statusClass = job.status === 'active' ? 'active' : 'paused';
            html += `<div class="scan-job-item ${currentJobId === job.id ? 'active' : ''}" 
                         data-job-id="${job.id}">
                <div class="scan-job-item-name">${escapeHtml(job.name)}</div>
                <div class="scan-job-item-meta">
                    <span class="scan-job-item-status ${statusClass}">${job.status}</span>
                    <span>${job.type}</span>
                    <span>${job.targetIds.length} targets</span>
                </div>
            </div>`;
        });

        jobsList.innerHTML = html;

        // Обработчики кликов
        jobsList.querySelectorAll('.scan-job-item').forEach(item => {
            item.addEventListener('click', () => {
                const jobId = item.dataset.jobId;
                loadJobForEdit(jobId);
            });
        });
    }

    // Загрузка Job для редактирования
    async function loadJobForEdit(jobId) {
        try {
            const job = await getJob(jobId);
            if (!job) {
                alert("Job не найден");
                return;
            }

            currentJobId = jobId;
            if (jobNameInput) jobNameInput.value = job.name || '';
            if (jobDescriptionInput) jobDescriptionInput.value = job.description || '';
            if (jobTypeSelect) jobTypeSelect.value = job.type || 'endpoints';
            if (jobStatusSelect) jobStatusSelect.value = job.status || 'active';
            if (jobTemplateSelect) jobTemplateSelect.value = job.templateId || '';

            // Загружаем списки шаблонов и целей
            await populateJobTemplates();
            await populateJobTargets(job.type, job.targetIds);

            // Обновляем список
            loadJobs();
        } catch (error) {
            console.error('[Scan] Error loading job:', error);
            alert("Ошибка загрузки Job");
        }
    }

    // Заполнение списка шаблонов
    async function populateJobTemplates() {
        try {
            const templates = await listTemplates();
            if (!jobTemplateSelect) return;

            jobTemplateSelect.innerHTML = '<option value="">Нет (стандартный формат)</option>';

            // Загружаем текущий Job, если есть
            let currentTemplateId = null;
            if (currentJobId) {
                try {
                    const job = await getJob(currentJobId);
                    if (job && job.templateId) {
                        currentTemplateId = job.templateId;
                    }
                } catch (error) {
                    console.error('[Scan] Error loading job for template selection:', error);
                }
            }

            templates.forEach(template => {
                const option = document.createElement('option');
                option.value = template.id;
                option.textContent = template.name || template.id;
                if (currentTemplateId === template.id) {
                    option.selected = true;
                }
                jobTemplateSelect.appendChild(option);
            });
        } catch (error) {
            console.error('[Scan] Error loading templates:', error);
        }
    }

    // Заполнение списка целей (endpoints или hosts)
    async function populateJobTargets(jobType, selectedIds = []) {
        if (!targetsPanel) return;

        try {
            const inventory = await getInventory(jobType === 'endpoints' ? 'endpoints' : 'hosts');
            if (!inventory || inventory.length === 0) {
                targetsPanel.innerHTML = `<p class="hint">Нет доступных ${jobType === 'endpoints' ? 'endpoints' : 'hosts'}</p>`;
                return;
            }

            let html = '';
            inventory.forEach(item => {
                const isChecked = selectedIds.includes(item.id);
                const label = jobType === 'endpoints'
                    ? `${item.url || item.id} (${item.method || 'GET'})`
                    : `${item.name || item.id} (${item.ip || ''})`;

                html += `<div class="scan-job-target-item">
                    <input type="checkbox" 
                           data-target-id="${item.id}" 
                           ${isChecked ? 'checked' : ''}>
                    <span>${escapeHtml(label)}</span>
                </div>`;
            });

            targetsPanel.innerHTML = html;
        } catch (error) {
            console.error('[Scan] Error loading targets:', error);
            targetsPanel.innerHTML = `<p class="hint">Ошибка загрузки ${jobType}</p>`;
        }
    }

    // Обновление списка целей при изменении типа
    if (jobTypeSelect) {
        jobTypeSelect.addEventListener('change', (e) => {
            const jobType = e.target.value;
            populateJobTargets(jobType, []);
        });
    }

    // Создание нового Job
    async function newJob() {
        currentJobId = null;
        if (jobNameInput) jobNameInput.value = '';
        if (jobDescriptionInput) jobDescriptionInput.value = '';
        if (jobTypeSelect) jobTypeSelect.value = 'endpoints';
        if (jobStatusSelect) jobStatusSelect.value = 'active';
        if (jobTemplateSelect) jobTemplateSelect.value = '';
        await populateJobTemplates();
        await populateJobTargets('endpoints', []);
        loadJobs();
    }

    // Сохранение Job
    async function saveJobHandler() {
        try {
            if (!jobNameInput || !jobNameInput.value.trim()) {
                alert("Введите название Job");
                return;
            }

            // Собираем выбранные цели
            const selectedTargets = [];
            if (targetsPanel) {
                const checkedBoxes = targetsPanel.querySelectorAll('input[type="checkbox"]:checked');
                checkedBoxes.forEach(checkbox => {
                    selectedTargets.push(checkbox.dataset.targetId);
                });
            }

            if (selectedTargets.length === 0) {
                alert("Выберите хотя бы одну цель");
                return;
            }

            const jobData = {
                name: jobNameInput.value.trim(),
                description: jobDescriptionInput.value.trim() || '',
                type: jobTypeSelect ? jobTypeSelect.value : 'endpoints',
                targetIds: selectedTargets,
                templateId: jobTemplateSelect && jobTemplateSelect.value ? jobTemplateSelect.value : null,
                status: jobStatusSelect ? jobStatusSelect.value : 'active'
            };

            let savedJob;
            if (currentJobId) {
                // Обновление существующего Job
                const existingJob = await getJob(currentJobId);
                if (!existingJob) {
                    alert("Job не найден");
                    return;
                }
                savedJob = updateJob(existingJob, jobData);
            } else {
                // Создание нового Job
                savedJob = createJob(jobData);
            }

            const savedId = await saveJob(savedJob);
            currentJobId = savedId;

            alert("Job сохранен");
            loadJobs();
        } catch (error) {
            console.error('[Scan] Error saving job:', error);
            alert(`Ошибка сохранения Job: ${error.message}`);
        }
    }

    // Удаление Job
    async function deleteJobHandler() {
        if (!currentJobId) {
            alert("Выберите Job для удаления");
            return;
        }

        if (!confirm("Удалить этот Job?")) {
            return;
        }

        try {
            await deleteJob(currentJobId);
            currentJobId = null;
            if (jobNameInput) jobNameInput.value = '';
            if (jobDescriptionInput) jobDescriptionInput.value = '';
            if (targetsPanel) targetsPanel.innerHTML = '';
            loadJobs();
        } catch (error) {
            console.error('[Scan] Error deleting job:', error);
            alert("Ошибка удаления Job");
        }
    }

    // Запуск Job (пока только генерация команд)
    async function runJobHandler() {
        if (!currentJobId) {
            alert("Выберите Job для запуска");
            return;
        }

        try {
            const job = await getJob(currentJobId);
            if (!job) {
                alert("Job не найден");
                return;
            }

            // TODO: Реализовать генерацию команд и отображение результатов
            alert("Функция запуска Job будет реализована в следующем этапе");
        } catch (error) {
            console.error('[Scan] Error running job:', error);
            alert("Ошибка запуска Job");
        }
    }

    // Привязка обработчиков
    if (newJobBtn) {
        newJobBtn.addEventListener('click', newJob);
    }
    if (saveJobBtn) {
        saveJobBtn.addEventListener('click', saveJobHandler);
    }
    if (deleteJobBtn) {
        deleteJobBtn.addEventListener('click', deleteJobHandler);
    }
    if (runJobBtn) {
        runJobBtn.addEventListener('click', runJobHandler);
    }

    // ===== РЕЗУЛЬТАТЫ ОПРОСОВ =====

    const importResultsBtn = document.getElementById("scanImportResultsBtn");
    const importResultsTextarea = document.getElementById("scanImportResultsTextarea");
    const resultsTable = document.getElementById("scanResultsTable");
    const resultsJobFilter = document.getElementById("scanResultsJobFilter");
    const resultsStatusFilter = document.getElementById("scanResultsStatusFilter");
    const resultsSearchInput = document.getElementById("scanResultsSearchInput");
    const resultsRefreshBtn = document.getElementById("scanResultsRefreshBtn");

    // Загрузка и отображение результатов
    async function loadScanResults() {
        // Ждем инициализации IndexedDB
        if (!indexedDBReady && !isAvailable()) {
            let attempts = 0;
            while (attempts < 10 && !indexedDBReady && !isAvailable()) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            if (!indexedDBReady && !isAvailable()) {
                renderScanResults([]);
                return;
            }
        }

        try {
            let results = await listScanResults();

            // Применяем фильтры
            const jobFilter = resultsJobFilter ? resultsJobFilter.value : '';
            const statusFilter = resultsStatusFilter ? resultsStatusFilter.value : '';
            const searchText = resultsSearchInput ? resultsSearchInput.value.toLowerCase() : '';

            if (jobFilter) {
                results = results.filter(r => r.jobId === jobFilter);
            }
            if (statusFilter) {
                results = results.filter(r => r.status === statusFilter);
            }
            if (searchText) {
                results = results.filter(r => {
                    const endpointId = (r.endpointId || '').toLowerCase();
                    const hostId = (r.hostId || '').toLowerCase();
                    return endpointId.includes(searchText) || hostId.includes(searchText);
                });
            }

            // Сортируем по timestamp (новые первыми)
            results.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

            renderScanResults(results);
        } catch (error) {
            console.error('[Scan] Error loading results:', error);
            renderScanResults([]);
        }
    }

    // Отображение таблицы результатов
    async function renderScanResults(results) {
        if (!resultsTable) return;

        if (results.length === 0) {
            resultsTable.innerHTML = "<p class='no-results'>Нет результатов опросов</p>";
            return;
        }

        // Загружаем Jobs для отображения названий
        const jobsMap = new Map();
        try {
            const jobs = await listJobs();
            jobs.forEach(job => jobsMap.set(job.id, job));
        } catch (error) {
            console.error('[Scan] Error loading jobs for results:', error);
        }

        // Загружаем Inventory для отображения названий endpoints/hosts
        const endpointsMap = new Map();
        const hostsMap = new Map();
        try {
            const endpoints = await getInventory('endpoints');
            endpoints.forEach(ep => endpointsMap.set(ep.id, ep));
            const hosts = await getInventory('hosts');
            hosts.forEach(host => hostsMap.set(host.id, host));
        } catch (error) {
            console.error('[Scan] Error loading inventory for results:', error);
        }

        let html = '<table class="scan-results-table"><thead><tr>';
        html += '<th>Timestamp</th>';
        html += '<th>Job</th>';
        html += '<th>Target</th>';
        html += '<th>Status</th>';
        html += '<th>Version</th>';
        html += '<th>Build</th>';
        html += '<th>Duration</th>';
        html += '<th>Error</th>';
        html += '</tr></thead><tbody>';

        results.forEach(result => {
            const job = jobsMap.get(result.jobId);
            const jobName = job ? job.name : result.jobId;

            let targetName = '';
            if (result.endpointId) {
                const ep = endpointsMap.get(result.endpointId);
                targetName = ep ? (ep.url || ep.id) : result.endpointId;
            } else if (result.hostId) {
                const host = hostsMap.get(result.hostId);
                targetName = host ? (host.name || host.id) : result.hostId;
            }

            const timestamp = new Date(result.timestamp).toLocaleString('ru-RU');
            const statusClass = result.status;
            const duration = result.duration ? `${result.duration}ms` : '-';
            const error = result.error ? escapeHtml(result.error.substring(0, 50)) : '';

            html += `<tr>
                <td>${timestamp}</td>
                <td>${escapeHtml(jobName)}</td>
                <td>${escapeHtml(targetName)}</td>
                <td><span class="scan-result-status ${statusClass}">${result.status}</span></td>
                <td>${escapeHtml(result.version || '-')}</td>
                <td>${escapeHtml(result.build || '-')}</td>
                <td>${duration}</td>
                <td title="${escapeHtml(result.error || '')}">${error}</td>
            </tr>`;
        });

        html += '</tbody></table>';
        resultsTable.innerHTML = html;
    }

    // Заполнение фильтров
    async function populateResultsFilters() {
        try {
            const jobs = await listJobs();

            if (resultsJobFilter) {
                resultsJobFilter.innerHTML = '<option value="">Все Jobs</option>';
                jobs.forEach(job => {
                    const option = document.createElement('option');
                    option.value = job.id;
                    option.textContent = job.name;
                    resultsJobFilter.appendChild(option);
                });
            }
        } catch (error) {
            console.error('[Scan] Error populating filters:', error);
        }
    }

    // Импорт результатов из JSON
    async function importScanResults() {
        if (!importResultsTextarea || !importResultsTextarea.value.trim()) {
            alert("Вставьте JSON с результатами опросов");
            return;
        }

        try {
            const jsonText = importResultsTextarea.value.trim();
            const results = JSON.parse(jsonText);

            if (!Array.isArray(results)) {
                alert("JSON должен быть массивом результатов");
                return;
            }

            let imported = 0;
            let errors = 0;
            const createdSnapshots = [];

            for (const resultData of results) {
                try {
                    // Извлекаем version/build из response, если есть
                    let version = resultData.version;
                    let build = resultData.build;

                    if (resultData.response && !version && !build) {
                        const extracted = extractVersionFromResponse(resultData.response);
                        version = extracted.version;
                        build = extracted.build;
                    }

                    // Создаем ScanResult с валидацией
                    const scanResult = createScanResult({
                        ...resultData,
                        version,
                        build
                    });

                    // Сохраняем результат
                    await saveScanResult(scanResult);
                    imported++;

                    // Если успешный опрос endpoint, создаем Snapshot
                    if (scanResult.status === 'success' && scanResult.endpointId && scanResult.version) {
                        try {
                            const snapshot = {
                                endpointId: scanResult.endpointId,
                                version: scanResult.version,
                                build: scanResult.build || null,
                                timestamp: scanResult.timestamp
                            };
                            await addInventoryItem('snapshots', snapshot);
                            createdSnapshots.push(snapshot);
                        } catch (snapshotError) {
                            console.warn('[Scan] Error creating snapshot:', snapshotError);
                        }
                    }
                } catch (error) {
                    console.error('[Scan] Error importing result:', error, resultData);
                    errors++;
                }
            }

            // Очищаем textarea
            if (importResultsTextarea) {
                importResultsTextarea.value = '';
            }

            let message = `Импортировано результатов: ${imported}`;
            if (errors > 0) {
                message += `, ошибок: ${errors}`;
            }
            if (createdSnapshots.length > 0) {
                message += `\nСоздано снапшотов: ${createdSnapshots.length}`;
            }
            alert(message);

            // Обновляем таблицу результатов
            loadScanResults();
            populateResultsFilters();

            // Обновляем Dashboard, если он открыт
            const dashboardTab = document.getElementById("dashboard");
            if (dashboardTab && dashboardTab.classList.contains("active")) {
                setTimeout(() => renderDashboard(), 500);
            }
        } catch (error) {
            console.error('[Scan] Error importing results:', error);
            alert(`Ошибка импорта: ${error.message}`);
        }
    }

    // Привязка обработчиков для результатов
    if (importResultsBtn) {
        importResultsBtn.addEventListener('click', importScanResults);
    }
    if (resultsRefreshBtn) {
        resultsRefreshBtn.addEventListener('click', loadScanResults);
    }
    if (resultsJobFilter) {
        resultsJobFilter.addEventListener('change', loadScanResults);
    }
    if (resultsStatusFilter) {
        resultsStatusFilter.addEventListener('change', loadScanResults);
    }
    if (resultsSearchInput) {
        resultsSearchInput.addEventListener('input', loadScanResults);
    }

    // Инициализация при открытии вкладки
    loadJobs();
    populateJobTemplates();
    populateJobTargets('endpoints', []);
    loadScanResults();
    populateResultsFilters();
}
