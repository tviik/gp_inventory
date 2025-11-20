/* ============================
   CHART RENDERER
   ============================
   
   Простой рендерер для bar charts без внешних библиотек.
   Использует Canvas для отрисовки.
*/

// ============================
// BAR CHART
// ============================

/**
 * Отрисовка горизонтального bar chart
 * @param {HTMLCanvasElement} canvas - canvas элемент
 * @param {Object} data - данные { labels: [], datasets: [{ label, values, color }] }
 * @param {Object} options - опции { width, height, padding }
 */
export function renderBarChart(canvas, data, options = {}) {
    const ctx = canvas.getContext('2d');
    const width = options.width || canvas.width || 800;
    const height = options.height || canvas.height || 400;
    const padding = options.padding || { top: 20, right: 20, bottom: 40, left: 100 };
    
    canvas.width = width;
    canvas.height = height;
    
    // Очистка
    ctx.clearRect(0, 0, width, height);
    
    if (!data || !data.labels || data.labels.length === 0) {
        ctx.fillStyle = '#999';
        ctx.font = '14px Arial';
        ctx.fillText('Нет данных', width / 2 - 50, height / 2);
        return;
    }
    
    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;
    const barHeight = chartHeight / data.labels.length;
    const barSpacing = barHeight * 0.1;
    const actualBarHeight = barHeight - barSpacing;
    
    // Находим максимальное значение
    let maxValue = 0;
    data.datasets.forEach(dataset => {
        dataset.values.forEach(val => {
            if (val > maxValue) maxValue = val;
        });
    });
    
    if (maxValue === 0) maxValue = 1;
    
    // Отрисовка осей
    ctx.strokeStyle = '#ccc';
    ctx.lineWidth = 1;
    
    // Вертикальная ось
    ctx.beginPath();
    ctx.moveTo(padding.left, padding.top);
    ctx.lineTo(padding.left, height - padding.bottom);
    ctx.stroke();
    
    // Горизонтальная ось
    ctx.beginPath();
    ctx.moveTo(padding.left, height - padding.bottom);
    ctx.lineTo(width - padding.right, height - padding.bottom);
    ctx.stroke();
    
    // Отрисовка меток на оси Y
    ctx.fillStyle = '#333';
    ctx.font = '12px Arial';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    
    data.labels.forEach((label, index) => {
        const y = padding.top + index * barHeight + barHeight / 2;
        ctx.fillText(label, padding.left - 10, y);
    });
    
    // Отрисовка меток на оси X
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    
    const tickCount = 5;
    for (let i = 0; i <= tickCount; i++) {
        const value = (maxValue / tickCount) * i;
        const x = padding.left + (chartWidth / tickCount) * i;
        const y = height - padding.bottom;
        
        ctx.fillText(Math.round(value).toString(), x, y + 5);
        
        // Линия сетки
        ctx.strokeStyle = '#eee';
        ctx.beginPath();
        ctx.moveTo(x, padding.top);
        ctx.lineTo(x, height - padding.bottom);
        ctx.stroke();
    }
    
    // Отрисовка столбцов
    data.datasets.forEach((dataset, datasetIndex) => {
        const color = dataset.color || getColorForDataset(datasetIndex);
        
        data.labels.forEach((label, labelIndex) => {
            const value = dataset.values[labelIndex] || 0;
            const barWidth = (value / maxValue) * chartWidth;
            const y = padding.top + labelIndex * barHeight + barSpacing / 2;
            
            // Столбец
            ctx.fillStyle = color;
            ctx.fillRect(padding.left, y, barWidth, actualBarHeight);
            
            // Значение на столбце
            if (barWidth > 30) {
                ctx.fillStyle = '#fff';
                ctx.font = '11px Arial';
                ctx.textAlign = 'left';
                ctx.textBaseline = 'middle';
                ctx.fillText(value.toString(), padding.left + barWidth / 2, y + actualBarHeight / 2);
            }
        });
    });
    
    // Легенда
    if (data.datasets.length > 1) {
        renderLegend(ctx, data.datasets, width - padding.right - 150, padding.top);
    }
}

/**
 * Отрисовка вертикального bar chart
 * @param {HTMLCanvasElement} canvas - canvas элемент
 * @param {Object} data - данные { labels: [], datasets: [{ label, values, color }] }
 * @param {Object} options - опции { width, height, padding }
 */
export function renderVerticalBarChart(canvas, data, options = {}) {
    const ctx = canvas.getContext('2d');
    const width = options.width || canvas.width || 800;
    const height = options.height || canvas.height || 400;
    const padding = options.padding || { top: 20, right: 20, bottom: 60, left: 40 };
    
    canvas.width = width;
    canvas.height = height;
    
    // Очистка
    ctx.clearRect(0, 0, width, height);
    
    if (!data || !data.labels || data.labels.length === 0) {
        ctx.fillStyle = '#999';
        ctx.font = '14px Arial';
        ctx.fillText('Нет данных', width / 2 - 50, height / 2);
        return;
    }
    
    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;
    const barWidth = chartWidth / data.labels.length;
    const barSpacing = barWidth * 0.1;
    const actualBarWidth = barWidth - barSpacing;
    
    // Находим максимальное значение
    let maxValue = 0;
    data.datasets.forEach(dataset => {
        dataset.values.forEach(val => {
            if (val > maxValue) maxValue = val;
        });
    });
    
    if (maxValue === 0) maxValue = 1;
    
    // Отрисовка осей
    ctx.strokeStyle = '#ccc';
    ctx.lineWidth = 1;
    
    // Вертикальная ось
    ctx.beginPath();
    ctx.moveTo(padding.left, padding.top);
    ctx.lineTo(padding.left, height - padding.bottom);
    ctx.stroke();
    
    // Горизонтальная ось
    ctx.beginPath();
    ctx.moveTo(padding.left, height - padding.bottom);
    ctx.lineTo(width - padding.right, height - padding.bottom);
    ctx.stroke();
    
    // Отрисовка меток на оси X
    ctx.fillStyle = '#333';
    ctx.font = '11px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    
    data.labels.forEach((label, index) => {
        const x = padding.left + index * barWidth + barWidth / 2;
        const y = height - padding.bottom;
        ctx.save();
        ctx.translate(x, y + 5);
        ctx.rotate(-Math.PI / 4);
        ctx.fillText(label, 0, 0);
        ctx.restore();
    });
    
    // Отрисовка меток на оси Y
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    
    const tickCount = 5;
    for (let i = 0; i <= tickCount; i++) {
        const value = (maxValue / tickCount) * i;
        const y = height - padding.bottom - (chartHeight / tickCount) * i;
        const x = padding.left;
        
        ctx.fillText(Math.round(value).toString(), x - 5, y);
        
        // Линия сетки
        ctx.strokeStyle = '#eee';
        ctx.beginPath();
        ctx.moveTo(padding.left, y);
        ctx.lineTo(width - padding.right, y);
        ctx.stroke();
    }
    
    // Отрисовка столбцов
    data.datasets.forEach((dataset, datasetIndex) => {
        const color = dataset.color || getColorForDataset(datasetIndex);
        
        data.labels.forEach((label, labelIndex) => {
            const value = dataset.values[labelIndex] || 0;
            const barHeight = (value / maxValue) * chartHeight;
            const x = padding.left + labelIndex * barWidth + barSpacing / 2;
            const y = height - padding.bottom - barHeight;
            
            // Столбец
            ctx.fillStyle = color;
            ctx.fillRect(x, y, actualBarWidth, barHeight);
            
            // Значение на столбце
            if (barHeight > 20) {
                ctx.fillStyle = '#333';
                ctx.font = '10px Arial';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'bottom';
                ctx.fillText(value.toString(), x + actualBarWidth / 2, y - 2);
            }
        });
    });
    
    // Легенда
    if (data.datasets.length > 1) {
        renderLegend(ctx, data.datasets, width - padding.right - 150, padding.top);
    }
}

// ============================
// LEGEND
// ============================

function renderLegend(ctx, datasets, x, y) {
    const legendItemHeight = 20;
    const legendItemSpacing = 5;
    
    datasets.forEach((dataset, index) => {
        const itemY = y + index * (legendItemHeight + legendItemSpacing);
        const color = dataset.color || getColorForDataset(index);
        const label = dataset.label || `Dataset ${index + 1}`;
        
        // Цветной квадрат
        ctx.fillStyle = color;
        ctx.fillRect(x, itemY, 15, 15);
        
        // Текст
        ctx.fillStyle = '#333';
        ctx.font = '12px Arial';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(label, x + 20, itemY + 7.5);
    });
}

// ============================
// HELPERS
// ============================

function getColorForDataset(index) {
    const colors = [
        '#4CAF50', // зеленый
        '#2196F3', // синий
        '#FF9800', // оранжевый
        '#F44336', // красный
        '#9C27B0', // фиолетовый
        '#00BCD4', // голубой
        '#FFEB3B', // желтый
        '#795548'  // коричневый
    ];
    return colors[index % colors.length];
}

/**
 * Получение цвета по состоянию версии
 * @param {string} status - статус (ok, warning, error)
 * @returns {string} цвет
 */
export function getStatusColor(status) {
    const colors = {
        ok: '#4CAF50',
        warning: '#FF9800',
        error: '#F44336',
        unknown: '#9E9E9E'
    };
    return colors[status] || colors.unknown;
}

// ============================
// PUBLIC API
// ============================

export const chartRenderer = {
    renderBarChart,
    renderVerticalBarChart,
    getStatusColor
};

export default chartRenderer;

