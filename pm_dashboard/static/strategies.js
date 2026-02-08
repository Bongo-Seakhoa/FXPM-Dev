var strategyState = {
  rows: [],
  filtered: [],
  pageSize: 20,
  currentPage: 1,
  totalPages: 1,
  preselect: null,
  preselectApplied: false,
  preselectedRow: null
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
  pageInfo: document.getElementById("strategy-page-info"),
  prevBtn: document.getElementById("strategy-prev"),
  nextBtn: document.getElementById("strategy-next"),
  filters: {
    symbol: document.getElementById("strategy-filter-symbol"),
    timeframe: document.getElementById("strategy-filter-timeframe"),
    regime: document.getElementById("strategy-filter-regime"),
    strategy: document.getElementById("strategy-filter-name"),
  },
};

function parseQueryParams() {
  var params = {};
  if (!window.location || !window.location.search) return params;
  var query = window.location.search.substring(1);
  if (!query) return params;
  var pairs = query.split("&");
  for (var i = 0; i < pairs.length; i += 1) {
    var part = pairs[i];
    if (!part) continue;
    var idx = part.indexOf("=");
    var key = idx >= 0 ? part.slice(0, idx) : part;
    var value = idx >= 0 ? part.slice(idx + 1) : "";
    key = decodeURIComponent(key || "");
    value = decodeURIComponent(value || "");
    if (key) params[key] = value;
  }
  return params;
}

function parseStrategyPreselect() {
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
  if (!preselect) return false;
  return !!(preselect.symbol || preselect.timeframe || preselect.regime || preselect.strategy);
}

function normalizeValue(value) {
  return String(value || "").toLowerCase();
}

function matchesPreselect(row, preselect) {
  if (preselect.symbol && normalizeValue(row.symbol) !== normalizeValue(preselect.symbol)) return false;
  if (preselect.timeframe && normalizeValue(row.timeframe) !== normalizeValue(preselect.timeframe)) return false;
  if (preselect.regime && normalizeValue(row.regime) !== normalizeValue(preselect.regime)) return false;
  if (preselect.strategy && normalizeValue(row.strategy_name) !== normalizeValue(preselect.strategy)) return false;
  return true;
}

function findPreselectedRow(rows, preselect) {
  for (var i = 0; i < rows.length; i += 1) {
    if (matchesPreselect(rows[i], preselect)) return rows[i];
  }
  return null;
}

function applyStrategyPreselect(preselect) {
  if (preselect.symbol && strategyElements.filters.symbol) {
    strategyElements.filters.symbol.value = preselect.symbol;
  }
  if (preselect.timeframe && strategyElements.filters.timeframe) {
    strategyElements.filters.timeframe.value = preselect.timeframe;
  }
  if (preselect.regime && strategyElements.filters.regime) {
    strategyElements.filters.regime.value = preselect.regime;
  }
  if (preselect.strategy && strategyElements.filters.strategy) {
    strategyElements.filters.strategy.value = preselect.strategy;
  }

  applyStrategyFilters();
  var match = findPreselectedRow(strategyState.filtered, preselect);
  strategyState.preselectedRow = match;
  if (match) {
    var idx = strategyState.filtered.indexOf(match);
    if (idx >= 0) {
      strategyState.currentPage = Math.floor(idx / strategyState.pageSize) + 1;
    }
  }
}

strategyState.preselect = parseStrategyPreselect();

function updateStrategyFilters() {
  var options = {
    symbol: PMCommon.uniqueValues(strategyState.rows, "symbol"),
    timeframe: PMCommon.uniqueValues(strategyState.rows, "timeframe"),
    regime: PMCommon.uniqueValues(strategyState.rows, "regime"),
    strategy: PMCommon.uniqueValues(strategyState.rows, "strategy_name"),
  };

  for (var key in strategyElements.filters) {
    if (!Object.prototype.hasOwnProperty.call(strategyElements.filters, key)) continue;
    var select = strategyElements.filters[key];
    if (!select) continue;
    var current = select.value;
    select.innerHTML = '<option value="">All</option>' + options[key].map(function (val) {
      return "<option>" + val + "</option>";
    }).join("");
    if (options[key].indexOf(current) !== -1) {
      select.value = current;
    }
  }
}

function applyStrategyFilters() {
  var filters = strategyElements.filters;
  strategyState.filtered = strategyState.rows.filter(function (row) {
    if (filters.symbol && filters.symbol.value && row.symbol !== filters.symbol.value) return false;
    if (filters.timeframe && filters.timeframe.value && row.timeframe !== filters.timeframe.value) return false;
    if (filters.regime && filters.regime.value && row.regime !== filters.regime.value) return false;
    if (filters.strategy && filters.strategy.value && row.strategy_name !== filters.strategy.value) return false;
    return true;
  });
  strategyState.currentPage = 1;
}

function renderStrategyTable() {
  var body = strategyElements.body;
  if (!body) return;
  body.innerHTML = "";
  updatePagination();
  var pageRows = getPagedRows();
  if (!pageRows.length) {
    var emptyRow = document.createElement("tr");
    emptyRow.innerHTML = '<td colspan="12" style="text-align:center; color:#5b5f6a;">No strategies found.</td>';
    body.appendChild(emptyRow);
    return;
  }
  pageRows.forEach(function (row) {
    var tr = document.createElement("tr");
    tr.dataset.rowId = row.id;
    var statusClass = row.validation_status === "validated" ? "pill-valid" : (row.validation_status === "expired" ? "pill-expired" : "pill-invalid");
    tr.innerHTML =
      "<td>" + row.symbol + "</td>" +
      "<td>" + row.timeframe + "</td>" +
      "<td>" + row.regime + "</td>" +
      "<td>" + row.strategy_name + "</td>" +
      "<td>" + (row.quality_score !== null && row.quality_score !== undefined ? row.quality_score.toFixed(3) : "N/A") + "</td>" +
      "<td>" + (row.regime_train_trades || "N/A") + "</td>" +
      "<td>" + (row.regime_val_trades || "N/A") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "win_rate") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "profit_factor") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "total_return_pct") + "</td>" +
      "<td>" + metricValue(row.val_metrics, "max_drawdown_pct") + "</td>" +
      '<td><span class="pill ' + statusClass + '">' + row.validation_status + "</span></td>";
    tr.addEventListener("click", function () { showStrategyDetails(row); });
    body.appendChild(tr);
  });
  if (pageRows.length) {
    var preselected = strategyState.preselectedRow;
    if (preselected && pageRows.indexOf(preselected) !== -1) {
      showStrategyDetails(preselected);
    } else {
      showStrategyDetails(pageRows[0]);
    }
  }
}

function metricValue(metrics, key) {
  if (!metrics || metrics[key] === undefined || metrics[key] === null) return "N/A";
  var val = Number(metrics[key]);
  if (!Number.isFinite(val)) return "N/A";
  if (key.indexOf("rate") !== -1 || key.indexOf("return") !== -1 || key.indexOf("drawdown") !== -1) {
    return val.toFixed(2);
  }
  return val.toFixed(3);
}

function showStrategyDetails(row) {
  if (!strategyElements.details) return;
  var detailItems = [
    ["Symbol", row.symbol],
    ["Timeframe", row.timeframe],
    ["Regime", row.regime],
    ["Strategy", row.strategy_name],
    ["Status", row.validation_status],
    ["Validation Reason", row.validation_reason || "N/A"],
    ["Optimized At", row.optimized_at || "N/A"],
    ["Trained At", row.trained_at || "N/A"],
    ["Valid Until", row.valid_until || "N/A"],
  ];
  strategyElements.details.innerHTML = detailItems.map(function (item) {
    return '<div class="detail-item"><span>' + item[0] + "</span><strong>" + item[1] + "</strong></div>";
  }).join("");

  if (strategyElements.params) {
    strategyElements.params.textContent = JSON.stringify(row.parameters || {}, null, 2);
  }

  if (strategyElements.train) {
    strategyElements.train.innerHTML = renderMetrics(row.train_metrics || {});
  }
  if (strategyElements.val) {
    strategyElements.val.innerHTML = renderMetrics(row.val_metrics || {});
  }
}

function renderMetrics(metrics) {
  var html = "";
  var keys = Object.keys(metrics || {}).sort();
  if (!keys.length) return "<div class=\"metric-pill\">N/A</div>";
  for (var i = 0; i < keys.length; i += 1) {
    var key = keys[i];
    var value = metrics[key];
    html += '<div class="metric-pill"><span>' + key.replace(/_/g, " ") + "</span><strong>" + value + "</strong></div>";
  }
  return html;
}

function fetchStrategies() {
  var includeInvalid = strategyElements.includeInvalid && strategyElements.includeInvalid.checked;
  var url = "/api/strategies?include_invalid=" + (includeInvalid ? "1" : "0");
  return PMCommon.fetchWithRetry(url)
    .then(function (response) { return response.json(); })
    .then(function (data) {
      strategyState.rows = data.rows || [];
      strategyState.filtered = strategyState.rows.slice();
      strategyState.currentPage = 1;
      if (strategyElements.count) strategyElements.count.textContent = data.summary ? data.summary.total : strategyState.rows.length;
      if (strategyElements.validCount) strategyElements.validCount.textContent = data.summary ? data.summary.validated : 0;
      if (strategyElements.invalidCount) strategyElements.invalidCount.textContent = data.summary ? data.summary.invalid : 0;
      updateStrategyFilters();
      if (strategyState.preselect && !strategyState.preselectApplied && hasPreselect(strategyState.preselect)) {
        applyStrategyPreselect(strategyState.preselect);
        strategyState.preselectApplied = true;
      } else {
        applyStrategyFilters();
      }
      renderStrategyTable();
    });
}

for (var key in strategyElements.filters) {
  if (!Object.prototype.hasOwnProperty.call(strategyElements.filters, key)) continue;
  var select = strategyElements.filters[key];
  if (!select) continue;
  select.addEventListener("change", function () {
    strategyState.preselect = null;
    strategyState.preselectedRow = null;
    applyStrategyFilters();
    renderStrategyTable();
  });
}

if (strategyElements.includeInvalid) {
  strategyElements.includeInvalid.addEventListener("change", function () {
    strategyState.preselect = null;
    strategyState.preselectedRow = null;
    fetchStrategies();
  });
}

if (strategyElements.prevBtn) {
  strategyElements.prevBtn.addEventListener("click", function () {
    if (strategyState.currentPage > 1) {
      strategyState.currentPage -= 1;
      renderStrategyTable();
    }
  });
}

if (strategyElements.nextBtn) {
  strategyElements.nextBtn.addEventListener("click", function () {
    if (strategyState.currentPage < strategyState.totalPages) {
      strategyState.currentPage += 1;
      renderStrategyTable();
    }
  });
}

window.addEventListener("load", function () {
  PMCommon.initTheme();
  fetchStrategies();
});

function updatePagination() {
  var total = strategyState.filtered.length;
  strategyState.totalPages = Math.max(1, Math.ceil(total / strategyState.pageSize));
  if (strategyState.currentPage > strategyState.totalPages) {
    strategyState.currentPage = strategyState.totalPages;
  }
  if (strategyElements.pageInfo) {
    strategyElements.pageInfo.textContent = "Page " + strategyState.currentPage + " of " + strategyState.totalPages;
  }
  if (strategyElements.prevBtn) {
    strategyElements.prevBtn.disabled = strategyState.currentPage <= 1;
  }
  if (strategyElements.nextBtn) {
    strategyElements.nextBtn.disabled = strategyState.currentPage >= strategyState.totalPages;
  }
}

function getPagedRows() {
  var start = (strategyState.currentPage - 1) * strategyState.pageSize;
  return strategyState.filtered.slice(start, start + strategyState.pageSize);
}
