// ui-table.js
// Universal table component for Middleware Light Tower v2
// API:
//   const table = createTable({
//     container: HTMLElement,
//     columns: [{ key, label, width?, align?, formatter? }],
//     data: Array<Object>,
//     enableSearch: true|false,
//     enableColumnToggle: true|false,
//     enableExport: true|false,
//     pageSize?: number,
//     onRowClick?: (row) => void
//   });
//
//   table.updateData(newData);
//
// CSV export:
//   exportToCsv(filename, columns, rows)
//   - Delimiter: ';' (good for RU/enterprise Excel)

export function exportToCsv(filename, columns, rows) {
  // Columns: [{ key, label }]
  const delimiter = ';'; // chosen delimiter
  const safe = (val) => {
    if (val == null) return '';
    const s = String(val);
    // Quote if contains delimiter or quote or newline
    if (s.includes(delimiter) || s.includes('"') || /\r|\n/.test(s)) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };

  const header = columns.map(c => safe(c.label)).join(delimiter);
  const body = (rows || []).map(row =>
    columns.map(c => safe(row[c.key])).join(delimiter)
  ).join('\n');

  const csv = header + '\n' + body;
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);

  const link = document.createElement('a');
  link.href = url;
  link.download = filename + '.csv';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

// ==========================================================================
let tableIdCounter = 0;

export function createTable(options) {
  const {
    container,
    columns,
    data,
    enableSearch = true,
    enableColumnToggle = true,
    enableExport = false,
    pageSize = 15,
    onRowClick
  } = options || {};

  if (!container) {
    throw new Error('createTable: container is required');
  }

  const state = {
    id: ++tableIdCounter,
    columns: (columns || []).map(col => ({
      ...col,
      visible: col.visible !== false
    })),
    fullData: Array.isArray(data) ? data.slice() : [],
    filteredData: [],
    pageSize,
    currentPage: 1,
    sortKey: null,
    sortDir: 'asc', // 'asc' | 'desc'
    searchTerm: ''
  };

  // DOM skeleton
  const root = document.createElement('div');
  root.className = 'table-root';

  const topBar = document.createElement('div');
  topBar.className = 'table-topbar';

  const leftControls = document.createElement('div');
  leftControls.className = 'table-controls-left';

  const rightControls = document.createElement('div');
  rightControls.className = 'table-controls-right';

  topBar.appendChild(leftControls);
  topBar.appendChild(rightControls);

  // Column visibility
  let columnToggleEl = null;
  if (enableColumnToggle) {
    columnToggleEl = document.createElement('div');
    columnToggleEl.className = 'column-toggle';

    state.columns.forEach(col => {
      const label = document.createElement('label');
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.checked = col.visible;
      checkbox.dataset.colKey = col.key;
      checkbox.addEventListener('change', () => {
        col.visible = checkbox.checked;
        renderHeader();
        renderBody();
      });
      const span = document.createElement('span');
      span.textContent = col.label;
      label.appendChild(checkbox);
      label.appendChild(span);
      columnToggleEl.appendChild(label);
    });

    leftControls.appendChild(columnToggleEl);
  }

  // Search
  let searchInput = null;
  if (enableSearch) {
    const searchWrap = document.createElement('div');
    searchWrap.className = 'table-search-wrap';

    searchInput = document.createElement('input');
    searchInput.type = 'search';
    searchInput.placeholder = 'Search...';
    searchInput.className = 'field-input';
    searchInput.addEventListener('input', () => {
      state.searchTerm = searchInput.value.trim().toLowerCase();
      state.currentPage = 1;
      applyFilterAndSort();
      renderBody();
      renderPagination();
    });

    searchWrap.appendChild(searchInput);
    rightControls.appendChild(searchWrap);
  }

  // Export button
  if (enableExport) {
    const exportBtn = document.createElement('button');
    exportBtn.className = 'btn-ghost btn-compact';
    exportBtn.textContent = 'Export CSV';
    exportBtn.addEventListener('click', () => {
      const visibleColumns = state.columns.filter(c => c.visible).map(c => ({
        key: c.key,
        label: c.label
      }));
      // Export all filteredData (not only current page)
      exportToCsv('export', visibleColumns, state.filteredData);
    });
    rightControls.appendChild(exportBtn);
  }

  // Table
  const tableContainer = document.createElement('div');
  tableContainer.className = 'table-container';

  const table = document.createElement('table');
  table.className = 'ui-table';
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  table.appendChild(thead);
  table.appendChild(tbody);
  tableContainer.appendChild(table);

  // Pagination
  const pagination = document.createElement('div');
  pagination.className = 'table-pagination';

  const prevBtn = document.createElement('button');
  prevBtn.textContent = 'Prev';
  const pageInfo = document.createElement('span');
  pageInfo.className = 'table-page-info';
  const nextBtn = document.createElement('button');
  nextBtn.textContent = 'Next';

  pagination.appendChild(prevBtn);
  pagination.appendChild(pageInfo);
  pagination.appendChild(nextBtn);

  prevBtn.addEventListener('click', () => {
    if (state.currentPage > 1) {
      state.currentPage -= 1;
      renderBody();
      renderPagination();
    }
  });

  nextBtn.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(state.filteredData.length / state.pageSize));
    if (state.currentPage < totalPages) {
      state.currentPage += 1;
      renderBody();
      renderPagination();
    }
  });

  root.appendChild(topBar);
  root.appendChild(tableContainer);
  root.appendChild(pagination);
  container.innerHTML = '';
  container.appendChild(root);

  // ---------------------------------------------------------------------------
  function applyFilterAndSort() {
    const term = state.searchTerm;
    let rows = state.fullData.slice();

    if (term) {
      const visibleKeys = state.columns.map(c => c.key);
      rows = rows.filter(row => {
        return visibleKeys.some(key => {
          const v = row[key];
          if (v == null) return false;
          return String(v).toLowerCase().includes(term);
        });
      });
    }

    if (state.sortKey) {
      const key = state.sortKey;
      const dir = state.sortDir === 'desc' ? -1 : 1;
      rows.sort((a, b) => {
        const va = a[key];
        const vb = b[key];
        if (va == null && vb == null) return 0;
        if (va == null) return -1 * dir;
        if (vb == null) return 1 * dir;
        // numeric detect
        const na = parseFloat(va);
        const nb = parseFloat(vb);
        if (!Number.isNaN(na) && !Number.isNaN(nb) && String(na) === String(va).replace(',', '.') && String(nb) === String(vb).replace(',', '.')) {
          if (na < nb) return -1 * dir;
          if (na > nb) return 1 * dir;
          return 0;
        }
        const sa = String(va).toLowerCase();
        const sb = String(vb).toLowerCase();
        if (sa < sb) return -1 * dir;
        if (sa > sb) return 1 * dir;
        return 0;
      });
    }

    state.filteredData = rows;
  }

  function renderHeader() {
    thead.innerHTML = '';
    const tr = document.createElement('tr');

    state.columns.forEach(col => {
      if (!col.visible) return;
      const th = document.createElement('th');
      if (col.width) th.style.width = col.width;
      if (col.align) th.style.textAlign = col.align;
      th.textContent = col.label;

      th.addEventListener('click', () => {
        if (state.sortKey === col.key) {
          state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortKey = col.key;
          state.sortDir = 'asc';
        }
        applyFilterAndSort();
        state.currentPage = 1;
        renderBody();
        renderPagination();
        updateSortIndicators();
      });

      tr.appendChild(th);
    });

    thead.appendChild(tr);
    updateSortIndicators();
  }

  function updateSortIndicators() {
    const ths = thead.querySelectorAll('th');
    let visibleIdx = -1;
    ths.forEach(th => {
      th.textContent = state.columns.filter(c => c.visible)[++visibleIdx].label;
    });

    // Re-add arrow for sorted column
    if (!state.sortKey) return;
    visibleIdx = -1;
    ths.forEach(th => {
      const col = state.columns.filter(c => c.visible)[++visibleIdx];
      if (!col) return;
      if (col.key === state.sortKey) {
        th.textContent = col.label + (state.sortDir === 'asc' ? ' ▲' : ' ▼');
      }
    });
  }

  function renderBody() {
    tbody.innerHTML = '';
    const start = (state.currentPage - 1) * state.pageSize;
    const end = start + state.pageSize;
    const rows = state.filteredData.slice(start, end);

    rows.forEach(row => {
      const tr = document.createElement('tr');
      state.columns.forEach(col => {
        if (!col.visible) return;
        const td = document.createElement('td');
        if (col.align) td.style.textAlign = col.align;
        const val = typeof col.formatter === 'function'
          ? col.formatter(row[col.key], row)
          : row[col.key];

        // XSS guard — always textContent
        td.textContent = val == null ? '' : String(val);
        tr.appendChild(td);
      });

      if (typeof onRowClick === 'function') {
        tr.style.cursor = 'pointer';
        tr.addEventListener('click', () => onRowClick(row));
      }

      tbody.appendChild(tr);
    });
  }

  function renderPagination() {
    const total = state.filteredData.length;
    const totalPages = Math.max(1, Math.ceil(total / state.pageSize));
    if (state.currentPage > totalPages) {
      state.currentPage = totalPages;
    }
    pageInfo.textContent = `Page ${state.currentPage} / ${totalPages} · ${total} rows`;
    prevBtn.disabled = state.currentPage <= 1;
    nextBtn.disabled = state.currentPage >= totalPages;
  }

  function updateData(newData) {
    state.fullData = Array.isArray(newData) ? newData.slice() : [];
    state.currentPage = 1;
    applyFilterAndSort();
    renderHeader();
    renderBody();
    renderPagination();
  }

  // Initial render
  applyFilterAndSort();
  renderHeader();
  renderBody();
  renderPagination();

  return {
    updateData
  };
}