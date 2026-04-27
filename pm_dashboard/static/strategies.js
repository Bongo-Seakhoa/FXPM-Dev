var strategyState = {
  rows: [],
  filtered: [],
  pageSize: 20,
  currentPage: 1,
  totalPages: 1,
  preselect: null,
  preselectApplied: false,
  selectedRowId: ""
};

var strategyElements = {
  count: document.getElementById("strategy-count"),
  validCount: document.getElementById("strategy-valid-count"),
  invalidCount: document.getElementById("strategy-invalid-count"),
  body: document.getElementById("strategy-body"),
  details: document.getElementById("strategy-details"),
  params: document.getElementById("strategy-params"),
  train: document.getElementById("strategy-train"),
  val: document.getElementById("strategy-val"),
  includeInvalid: document.getElementById("strategy-include-invalid"),
  searchInput: document.getElementById("strategy-search"),
  pageInfo: document.getElementById("strategy-page-info"),
  prevBtn: document.getElementById("strategy-prev"),
  nextBtn: document.getElementById("strategy-next"),
  filters: {
    symbol: document.getElementById("strategy-filter-symbol"),
    timeframe: document.getElementById("strategy-filter-timeframe"),
    regime: document.getElementById("strategy-filter-regime"),
    strategy: document.getElementById("strategy-filter-name")
  }
};

function parseQueryParams() {
  var params = {};
  if (!window.location || !window.location.search) return params;
  var pairs = window.location.search.substring(1).split("&");
  for (var i = 0; i < pairs.length; i += 1) {
    var part = pairs[i];
    if (!part) continue;
    var idx = part.indexOf("=");
    var key = decodeURIComponent(idx >= 0 ? part.slice(0, idx) : part);
    var value = decodeURIComponent(idx >= 0 ? part.slice(idx + 1) : "");
    if (key) params[key] = value;
  }
  return params;
}

function parsePreselect() {
  var params = parseQueryParams();
  var preselect = {};
  if (params.symbol) preselect.symbol = params.symbol;
  if (params.timeframe) preselect.timeframe = params.timeframe;
  if (params.regime) preselect.regime = params.regime;
  if (params.strategy) preselect.strategy = params.strategy;
  if (params.strategy_name && !preselect.strategy) preselect.strategy = params.strategy_name;
  return preselect;
}

function hasPreselect(preselect) {
  return Boolean(preselect && (preselect.symbol || preselect.timeframe || preselect.regime || preselect.strategy));
}

function updateFilterOptions() {
  var options = {
    symbol: PMCommon.uniqueValues(strategyState.rows, "symbol"),
    timeframe: PMCommon.uniqueValues(strategyState.rows, "timeframe"),
    regime: PMCommon.uniqueValues(strategyState.rows, "regime"),
    strategy: PMCommon.uniqueValues(strategyState.rows, "strategy_name")
  };

  for (var key in strategyElements.filters) {
    if (!Object.prototype.hasOwnProperty.call(strategyElements.filters, key)) continue;
    var select = strategyElements.filters[key];
    if (!select) continue;
    var current = select.value;
    select.innerHTML = '<option value="">All</option>' + options[key].map(function (value) {
      return '<option value="' + PMCommon.escapeHtml(value) + '">' + PMCommon.escapeHtml(value) + "</option>";
    }).join("");
    if (options[key].indexOf(current) !== -1) select.value = current;
  }
}

function applyFilters() {
  var searchTerm = strategyElements.searchInput && strategyElements.searchInput.value
    ? strategyElements.searchInput.value.trim().toLowerCase()
    : "";

  strategyState.filtered = strategyState.rows.filter(function (row) {
    if (strategyElements.filters.symbol && strategyElements.filters.symbol.value && row.symbol !== strategyElements.filters.symbol.value) return false;
    if (strategyElements.filters.timeframe && strategyElements.filters.timeframe.value && row.timeframe !== strategyElements.filters.timeframe.value) return false;
    if (strategyElements.filters.regime && strategyElements.filters.regime.value && row.regime !== strategyElements.filters.regime.value) return false;
    if (strategyElements.filters.strategy && strategyElements.filters.strategy.value && row.strategy_name !== strategyElements.filters.strategy.value) return false;

    if (searchTerm) {
      var text = [row.symbol, row.timeframe, row.regime, row.strategy_name].join(" ").toLowerCase();
      if (text.indexOf(searchTerm) === -1) return false;
    }

    return true;
  });

  strategyState.currentPage = 1;
}

function metricValue(metrics, key) {
  if (!metrics || metrics[key] === undefined || metrics[key] === null) return "N/A";
  var value = Number(metrics[key]);
  if (!Number.isFinite(value)) return "N/A";
  if (key.indexOf("rate") !== -1 || key.indexOf("return") !== -1 || key.indexOf("drawdown") !== -1) {
    return value.toFixed(2);
  }
  return value.toFixed(3);
}

function scoreValue(value) {
  return PMCommon.formatNumber(value, 3);
}

function countValue(value) {
  if (value === null || value === undefined || value === "") return "N/A";
  var numberValue = Number(value);
  if (Number.isFinite(numberValue)) return String(Math.trunc(numberValue));
  return PMCommon.escapeHtml(String(value));
}

function renderMetrics(metrics) {
  var keys = Object.keys(metrics || {}).sort();
  if (!keys.length) return '<div class="metric-pill">N/A</div>';
  var html = "";
  for (var i = 0; i < keys.length; i += 1) {
    var key = keys[i];
    html += '<div class="metric-pill"><span>' + PMCommon.escapeHtml(key.replace(/_/g, " ")) + '</span><strong>' + PMCommon.escapeHtml(String(metrics[key])) + '</strong></div>';
  }
  return html;
}

function showDetails(row) {
  if (!row || !strategyElements.details) return;

  var details = [
    ["Symbol", row.symbol],
    ["Timeframe", row.timeframe],
    ["Regime", row.regime],
    ["Strategy", row.strategy_name],
    ["Status", row.validation_status],
    ["Validation Reason", row.validation_reason || "N/A"],
    ["Optimized At", row.optimized_at || "N/A"],
    ["Trained At", row.trained_at || "N/A"],
    ["Valid Until", row.valid_until || "N/A"]
  ];

  strategyElements.details.innerHTML = details.map(function (item) {
    return '<div class="detail-item"><span>' + PMCommon.escapeHtml(item[0]) + '</span><strong>' + PMCommon.escapeHtml(String(item[1])) + '</strong></div>';
  }).join("");

  if (strategyElements.params) strategyElements.params.textContent = JSON.stringify(row.parameters || {}, null, 2);
  if (strategyElements.train) strategyElements.train.innerHTML = renderMetrics(row.train_metrics || {});
  if (strategyElements.val) strategyElements.val.innerHTML = renderMetrics(row.val_metrics || {});
}

function updatePagination() {
  var total = strategyState.filtered.length;
  strategyState.totalPages = Math.max(1, Math.ceil(total / strategyState.pageSize));
  if (strategyState.currentPage > strategyState.totalPages) strategyState.currentPage = strategyState.totalPages;

  if (strategyElements.pageInfo) strategyElements.pageInfo.textContent = "Page " + strategyState.currentPage + " of " + strategyState.totalPages;
  if (strategyElements.prevBtn) strategyElements.prevBtn.disabled = strategyState.currentPage <= 1;
  if (strategyElements.nextBtn) strategyElements.nextBtn.disabled = strategyState.currentPage >= strategyState.totalPages;
}

function getPagedRows() {
  var start = (strategyState.currentPage - 1) * strategyState.pageSize;
  return strategyState.filtered.slice(start, start + strategyState.pageSize);
}
function findRowById(rows, rowId) {
  for (var i = 0; i < rows.length; i += 1) {
    if (rows[i].id === rowId) return rows[i];
  }
  return null;
}

function renderTable() {
  var body = strategyElements.body;
  if (!body) return;

  body.innerHTML = "";
  updatePagination();

  var pageRows = getPagedRows();
  if (!pageRows.length) {
    var emptyRow = document.createElement("tr");
    emptyRow.innerHTML = '<td colspan="12" class="table-empty">No strategies found.</td>';
    body.appendChild(emptyRow);
    return;
  }

  for (var i = 0; i < pageRows.length; i += 1) {
    var row = pageRows[i];
    var tr = document.createElement("tr");
    tr.dataset.rowId = row.id;
    if (strategyState.selectedRowId && strategyState.selectedRowId === row.id) {
      tr.classList.add("selected");
    }

    var statusClass = row.validation_status === "validated"
      ? "pill-valid"
      : (row.validation_status === "expired" ? "pill-expired" : "pill-invalid");

    tr.innerHTML =
      "<td>" + PMCommon.escapeHtml(row.symbol) + "</td>" +
      "<td>" + PMCommon.escapeHtml(row.timeframe) + "</td>" +
      "<td>" + PMCommon.escapeHtml(row.regime) + "</td>" +
      "<td>" + PMCommon.escapeHtml(row.strategy_name) + "</td>" +
      "<td>" + scoreValue(row.quality_score) + "</td>" +
      "<td>" + countValue(row.regime_train_trades) + "</td>" +
      "<td>" + countValue(row.regime_val_trades) + "</td>" +
      "<td>" + metricValue(row.val_metrics, "win_rate") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "profit_factor") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "total_return_pct") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "max_drawdown_pct") + "</td>" +
      '<td><span class="pill ' + statusClass + '">' + PMCommon.escapeHtml(row.validation_status) + "</span></td>";

    tr.addEventListener("click", createRowSelectHandler(row));
    body.appendChild(tr);
  }

  if (!strategyState.selectedRowId || !findRowById(pageRows, strategyState.selectedRowId)) {
    strategyState.selectedRowId = pageRows[0].id;
  }

  var selected = findRowById(pageRows, strategyState.selectedRowId) || pageRows[0];
  showDetails(selected);
}

function createRowSelectHandler(row) {
  return function () {
    strategyState.selectedRowId = row.id;
    showDetails(row);
    renderTable();
  };
}

function applyPreselectIfAny() {
  if (!hasPreselect(strategyState.preselect) || strategyState.preselectApplied) return;

  if (strategyState.preselect.symbol && strategyElements.filters.symbol) strategyElements.filters.symbol.value = strategyState.preselect.symbol;
  if (strategyState.preselect.timeframe && strategyElements.filters.timeframe) strategyElements.filters.timeframe.value = strategyState.preselect.timeframe;
  if (strategyState.preselect.regime && strategyElements.filters.regime) strategyElements.filters.regime.value = strategyState.preselect.regime;
  if (strategyState.preselect.strategy && strategyElements.filters.strategy) strategyElements.filters.strategy.value = strategyState.preselect.strategy;

  strategyState.preselectApplied = true;
}

function fetchStrategies() {
  var includeInvalid = strategyElements.includeInvalid && strategyElements.includeInvalid.checked;
  var url = "/api/strategies?include_invalid=" + (includeInvalid ? "1" : "0");

  return PMCommon.fetchWithRetry(url)
    .then(function (response) { return response.json(); })
    .then(function (data) {
      strategyState.rows = data.rows || [];

      if (strategyElements.count) strategyElements.count.textContent = data.summary ? data.summary.total : strategyState.rows.length;
      if (strategyElements.validCount) strategyElements.validCount.textContent = data.summary ? data.summary.validated : 0;
      if (strategyElements.invalidCount) strategyElements.invalidCount.textContent = data.summary ? data.summary.invalid : 0;

      updateFilterOptions();
      applyPreselectIfAny();
      applyFilters();
      renderTable();
    });
}

function applyFiltersAndRender(resetSelection) {
  strategyState.preselect = null;
  applyFilters();
  if (resetSelection) {
    strategyState.selectedRowId = strategyState.filtered.length ? strategyState.filtered[0].id : "";
  }
  renderTable();
}

function bindEvents() {
  for (var key in strategyElements.filters) {
    if (!Object.prototype.hasOwnProperty.call(strategyElements.filters, key)) continue;
    var select = strategyElements.filters[key];
    if (!select) continue;
    select.addEventListener("change", function () {
      applyFiltersAndRender(true);
    });
  }

  if (strategyElements.searchInput) {
    strategyElements.searchInput.addEventListener("input", PMCommon.debounce(function () {
      applyFiltersAndRender(true);
    }, 180));
  }

  if (strategyElements.includeInvalid) {
    strategyElements.includeInvalid.addEventListener("change", function () {
      strategyState.preselect = null;
      strategyState.preselectApplied = true;
      strategyState.selectedRowId = "";
      fetchStrategies().catch(function (err) {
        console.error("Failed to reload strategies:", err);
        PMCommon.showToast("Failed to load strategies");
      });
    });
  }

  if (strategyElements.prevBtn) {
    strategyElements.prevBtn.addEventListener("click", function () {
      if (strategyState.currentPage > 1) {
        strategyState.currentPage -= 1;
        renderTable();
      }
    });
  }

  if (strategyElements.nextBtn) {
    strategyElements.nextBtn.addEventListener("click", function () {
      if (strategyState.currentPage < strategyState.totalPages) {
        strategyState.currentPage += 1;
        renderTable();
      }
    });
  }
}

function init() {
  PMCommon.initTheme();
  strategyState.preselect = parsePreselect();
  bindEvents();

  fetchStrategies().catch(function (err) {
    console.error("Failed to load strategies:", err);
    PMCommon.showToast("Failed to load strategies");
  });
}

window.addEventListener("load", init);
