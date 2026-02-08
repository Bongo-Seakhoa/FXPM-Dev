var state = {
  entries: [],
  filtered: [],
  displayed: [],
  selected: null,
  instrumentSpecs: {},
  config: {},
  pageSize: 20,
  currentPage: 1,
  totalPages: 1
};

var elements = {
  validCount: document.getElementById("valid-count"),
  lastUpdated: document.getElementById("last-updated"),
  sourceFiles: document.getElementById("source-files"),
  tableBody: document.getElementById("entries-body"),
  drawer: document.getElementById("drawer"),
  drawerTitle: document.getElementById("drawer-title"),
  detailsGrid: document.getElementById("details-grid"),
  ticketBox: document.getElementById("ticket-box"),
  sizingForm: document.getElementById("sizing-form"),
  useEntryBtn: document.getElementById("use-entry-btn"),
  drawerUseEntryBtn: document.getElementById("drawer-use-entry"),
  signalSl: document.getElementById("signal-sl-display"),
  signalEntry: document.getElementById("signal-entry-display"),
  signalTp: document.getElementById("signal-tp-display"),
  signalSymbol: document.getElementById("signal-symbol-display"),
  signalDirection: document.getElementById("signal-direction-display"),
  jsError: document.getElementById("js-error"),
  pageInfo: document.getElementById("page-info"),
  pageCount: document.getElementById("page-count"),
  pageSize: document.getElementById("page-size"),
  pagePrev: document.getElementById("page-prev"),
  pageNext: document.getElementById("page-next"),
  filters: {
    symbol: document.getElementById("filter-symbol"),
    timeframe: document.getElementById("filter-timeframe"),
    regime: document.getElementById("filter-regime"),
    direction: document.getElementById("filter-direction"),
    strategy: document.getElementById("filter-strategy"),
  },
};

function showJsError(message) {
  if (elements.jsError) {
    elements.jsError.style.display = "block";
    elements.jsError.textContent = "Dashboard JS error: " + message;
  }
}

window.addEventListener("error", function (event) {
  showJsError(event.message || "Unknown error");
});

window.addEventListener("unhandledrejection", function (event) {
  showJsError(event.reason ? String(event.reason) : "Unhandled promise rejection");
});

function parseDate(value) {
  if (!value) return null;
  var date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function entryTimeValue(entry) {
  var date = parseDate(entry.timestamp);
  return date ? date.getTime() : 0;
}

function sortEntries(entries) {
  return entries.sort(function (a, b) {
    var timeA = entryTimeValue(a);
    var timeB = entryTimeValue(b);
    if (timeA !== timeB) return timeB - timeA;
    var strengthA = a.signal_strength !== undefined && a.signal_strength !== null ? a.signal_strength : 0;
    var strengthB = b.signal_strength !== undefined && b.signal_strength !== null ? b.signal_strength : 0;
    return strengthB - strengthA;
  });
}

function isDailyTimeframe(timeframe) {
  var value = timeframe ? String(timeframe).toUpperCase() : "";
  return value === "D1" || value === "1D";
}

function pickLatest(entries) {
  if (!entries || !entries.length) return null;
  var sorted = sortEntries(entries.slice());
  return sorted[0];
}

function reduceEntries(entries) {
  var bySymbol = {};
  for (var i = 0; i < entries.length; i += 1) {
    var entry = entries[i];
    var symbol = entry.symbol || "N/A";
    if (!bySymbol[symbol]) {
      bySymbol[symbol] = { d1: [], other: [] };
    }
    if (isDailyTimeframe(entry.timeframe)) {
      bySymbol[symbol].d1.push(entry);
    } else {
      bySymbol[symbol].other.push(entry);
    }
  }
  var reduced = [];
  var symbols = Object.keys(bySymbol);
  for (var j = 0; j < symbols.length; j += 1) {
    var bucket = bySymbol[symbols[j]];
    var d1 = pickLatest(bucket.d1);
    var other = pickLatest(bucket.other);
    if (d1) reduced.push(d1);
    if (other) reduced.push(other);
  }
  return reduced;
}

function updateFilterOptions() {
  var options = {
    symbol: PMCommon.uniqueValues(state.entries, "symbol"),
    timeframe: PMCommon.uniqueValues(state.entries, "timeframe"),
    regime: PMCommon.uniqueValues(state.entries, "regime"),
    direction: PMCommon.uniqueValues(state.entries, "signal_direction"),
    strategy: PMCommon.uniqueValues(state.entries, "strategy_name"),
  };

  for (var key in elements.filters) {
    if (!Object.prototype.hasOwnProperty.call(elements.filters, key)) continue;
    var select = elements.filters[key];
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

function applyFilters() {
  var filters = elements.filters;
  state.filtered = state.entries.filter(function (entry) {
    if (filters.symbol && filters.symbol.value && entry.symbol !== filters.symbol.value) return false;
    if (filters.timeframe && filters.timeframe.value && entry.timeframe !== filters.timeframe.value) return false;
    if (filters.regime && filters.regime.value && entry.regime !== filters.regime.value) return false;
    if (filters.direction && filters.direction.value && entry.signal_direction !== filters.direction.value) return false;
    if (filters.strategy && filters.strategy.value && entry.strategy_name !== filters.strategy.value) return false;
    return true;
  });
  state.currentPage = 1;
  state.displayed = reduceEntries(state.filtered);
}

function updatePagination() {
  var total = state.displayed.length;
  state.totalPages = Math.max(1, Math.ceil(total / state.pageSize));
  if (state.currentPage > state.totalPages) {
    state.currentPage = state.totalPages;
  }
  if (elements.pageInfo) {
    elements.pageInfo.textContent = "Page " + state.currentPage + " of " + state.totalPages;
  }
  if (elements.pageCount) {
    elements.pageCount.textContent = total + " rows";
  }
  if (elements.pagePrev) {
    elements.pagePrev.disabled = state.currentPage <= 1;
  }
  if (elements.pageNext) {
    elements.pageNext.disabled = state.currentPage >= state.totalPages;
  }
}

function getPagedEntries() {
  var sorted = sortEntries(state.displayed.slice());
  var start = (state.currentPage - 1) * state.pageSize;
  return sorted.slice(start, start + state.pageSize);
}

function updateCounts() {
  if (!elements.validCount) return;
  var count = 0;
  for (var i = 0; i < state.displayed.length; i += 1) {
    if (state.displayed[i].valid_now) count += 1;
  }
  elements.validCount.textContent = count;
}

function buildStrategyCell(entry) {
  if (!entry.strategy_name) return "N/A";
  var params = [];
  if (entry.symbol) params.push("symbol=" + encodeURIComponent(entry.symbol));
  if (entry.timeframe) params.push("timeframe=" + encodeURIComponent(entry.timeframe));
  if (entry.regime) params.push("regime=" + encodeURIComponent(entry.regime));
  if (entry.strategy_name) params.push("strategy=" + encodeURIComponent(entry.strategy_name));
  var href = "/strategies" + (params.length ? "?" + params.join("&") : "");
  return '<a class="strategy-link" href="' + href + '">' + entry.strategy_name + "</a>";
}

function getNewestIds(entries, count) {
  var newestIds = {};
  if (!entries || !entries.length) return newestIds;
  var sorted = sortEntries(entries.slice());
  var limit = count || 3;
  for (var i = 0; i < sorted.length && i < limit; i += 1) {
    if (sorted[i].entry_id) {
      newestIds[sorted[i].entry_id] = true;
    }
  }
  return newestIds;
}

function renderTable() {
  var tbody = elements.tableBody;
  if (!tbody) return;
  tbody.innerHTML = "";
  updatePagination();
  updateCounts();
  var pageEntries = getPagedEntries();
  var newestIds = getNewestIds(state.displayed, 3);
  pageEntries.forEach(function (entry) {
    var row = document.createElement("tr");
    row.dataset.entryId = entry.entry_id;
    row.classList.add(entry.valid_now ? "row-valid" : "row-invalid");
    if (entry.valid_now && entry.entry_id && newestIds[entry.entry_id]) {
      row.classList.add("row-newest");
    }
    var dirClass = entry.signal_direction === "buy" ? "dir-buy" : "dir-sell";
    var dirText = entry.signal_direction ? entry.signal_direction.toUpperCase() : "N/A";
    row.innerHTML =
      "<td>" + (entry.symbol || "N/A") + "</td>" +
      "<td>" + (entry.timeframe || "N/A") + "</td>" +
      "<td>" + (entry.regime || "N/A") + "</td>" +
      '<td class="' + dirClass + '">' + dirText + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.stop_loss_price) + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.entry_price) + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.take_profit_price) + "</td>" +
      "<td>" + (entry.signal_strength !== undefined && entry.signal_strength !== null ? entry.signal_strength : "N/A") + "</td>" +
      "<td>" + (entry.timestamp ? new Date(entry.timestamp).toLocaleString() : "N/A") + "</td>" +
      "<td>" + buildStrategyCell(entry) + "</td>";
    row.addEventListener("click", function () { openDrawer(entry); });
    var strategyLink = row.querySelector(".strategy-link");
    if (strategyLink) {
      strategyLink.addEventListener("click", function (event) {
        event.stopPropagation();
      });
    }
    tbody.appendChild(row);
  });
}

function openDrawer(entry) {
  state.selected = entry;
  PMCommon.openDrawer(elements.drawer);
  if (elements.drawerTitle) {
    elements.drawerTitle.textContent = (entry.symbol || "Entry") + " " + (entry.signal_direction ? entry.signal_direction.toUpperCase() : "");
  }
  renderDetails(entry);
  updateTicket(entry);
  syncSizingInputs(entry);
}

function closeDrawer() {
  PMCommon.closeDrawer(elements.drawer);
}

function renderDetails(entry) {
  if (!elements.detailsGrid) return;
  var details = [
    ["Symbol", entry.symbol],
    ["Timeframe", entry.timeframe],
    ["Regime", entry.regime],
    ["Strategy", entry.strategy_name],
    ["Direction", entry.signal_direction],
    ["Stop Loss", PMCommon.formatNumber(entry.stop_loss_price)],
    ["Take Profit", PMCommon.formatNumber(entry.take_profit_price)],
    ["Entry", PMCommon.formatNumber(entry.entry_price)],
    ["Strength", entry.signal_strength !== undefined && entry.signal_strength !== null ? entry.signal_strength : "N/A"],
    ["Timestamp", entry.timestamp ? new Date(entry.timestamp).toLocaleString() : "N/A"],
    ["Reason", entry.reason || "N/A"],
    ["Source", entry.source || "N/A"],
  ];
  elements.detailsGrid.innerHTML = details.map(function (item) {
    return '<div class="detail-item"><span>' + item[0] + "</span><strong>" + (item[1] !== undefined && item[1] !== null ? item[1] : "N/A") + "</strong></div>";
  }).join("");
}

function updateTicket(entry) {
  var lot = calculateLot(entry);
  var lines = [
    "Symbol: " + (entry.symbol || "N/A"),
    "Direction: " + (entry.signal_direction ? entry.signal_direction.toUpperCase() : "N/A"),
    "SL: " + PMCommon.formatNumber(entry.stop_loss_price) + " | Entry: " + PMCommon.formatNumber(entry.entry_price) + " | TP: " + PMCommon.formatNumber(entry.take_profit_price),
    "Lot: " + (lot ? lot.lotSize : "N/A"),
    "Timeframe: " + (entry.timeframe || "N/A"),
    "Regime: " + (entry.regime || "N/A"),
    "Strategy: " + (entry.strategy_name || "N/A"),
  ];
  if (elements.ticketBox) {
    elements.ticketBox.textContent = lines.join("\n");
  }
}

function defaultPipSize(symbol) {
  if (!symbol) return 0.0001;
  if (symbol.indexOf("JPY") !== -1) return 0.01;
  if (symbol.indexOf("XAU") === 0) return 0.1;
  if (symbol.indexOf("XAG") === 0) return 0.01;
  return 0.0001;
}

function calculateLot(entry) {
  var equity = Number(document.getElementById("equity").value || 0);
  var leverage = Number(document.getElementById("leverage").value || 0);
  var riskRadio = document.querySelector("input[name='risk-mode']:checked");
  var riskMode = riskRadio ? riskRadio.value : "percent";
  var riskPct = Number(document.getElementById("risk-pct").value || 0);
  var riskFixed = Number(document.getElementById("risk-fixed").value || 0);
  var symbol = (entry && entry.symbol) || document.getElementById("sizing-symbol").value;
  var entryPrice = Number(document.getElementById("sizing-entry").value || (entry ? entry.entry_price : 0) || 0);
  var slPrice = Number(document.getElementById("sizing-sl").value || (entry ? entry.stop_loss_price : 0) || 0);

  if (!equity || !entryPrice || !slPrice) return null;
  var spec = state.instrumentSpecs[symbol] || {};
  var pipPos = spec.pip_position !== undefined ? spec.pip_position : spec.pipPosition;
  var pipSize = pipPos !== undefined ? Math.pow(10, -Number(pipPos)) : defaultPipSize(symbol);
  var pipValue = Number(spec.pip_value !== undefined ? spec.pip_value : (spec.pipValue || 0));

  var riskAmount = riskMode === "fixed" ? riskFixed : equity * (riskPct / 100);
  if (!riskAmount) return null;
  var stopDistance = Math.abs(entryPrice - slPrice);
  var pipDistance = pipSize ? stopDistance / pipSize : 0;
  if (!pipDistance || !pipValue) return null;
  var riskPerLot = pipDistance * pipValue;
  if (!riskPerLot) return null;
  var lotSize = riskAmount / riskPerLot;

  var minLot = Number(spec.min_lot !== undefined ? spec.min_lot : (spec.minLot || 0.01));
  var maxLot = Number(spec.max_lot !== undefined ? spec.max_lot : (spec.maxLot || 100));
  var step = Number(spec.lot_step !== undefined ? spec.lot_step : 0.01);
  lotSize = Math.max(minLot, Math.min(maxLot, lotSize));
  lotSize = Math.round(lotSize / step) * step;

  var marginUsed = null;
  if (leverage) {
    var contractSize = Number(spec.contract_size !== undefined ? spec.contract_size : 100000);
    marginUsed = (entryPrice * contractSize * lotSize) / leverage;
  }

  return {
    lotSize: lotSize.toFixed(2),
    pipDistance: pipDistance.toFixed(1),
    riskAmount: riskAmount.toFixed(2),
    marginUsed: marginUsed ? marginUsed.toFixed(2) : null,
  };
}

function syncSizingInputs(entry) {
  document.getElementById("sizing-symbol").value = entry.symbol || "";
  document.getElementById("sizing-entry").value = entry.entry_price !== undefined && entry.entry_price !== null ? entry.entry_price : "";
  document.getElementById("sizing-sl").value = entry.stop_loss_price !== undefined && entry.stop_loss_price !== null ? entry.stop_loss_price : "";
  document.getElementById("sizing-tp").value = entry.take_profit_price !== undefined && entry.take_profit_price !== null ? entry.take_profit_price : "";
  if (elements.signalSl) {
    elements.signalSl.textContent = PMCommon.formatNumber(entry.stop_loss_price);
  }
  if (elements.signalEntry) {
    elements.signalEntry.textContent = PMCommon.formatNumber(entry.entry_price);
  }
  if (elements.signalTp) {
    elements.signalTp.textContent = PMCommon.formatNumber(entry.take_profit_price);
  }
  if (elements.signalSymbol) {
    elements.signalSymbol.textContent = entry.symbol || "N/A";
  }
  if (elements.signalDirection) {
    elements.signalDirection.textContent = entry.signal_direction ? entry.signal_direction.toUpperCase() : "N/A";
  }
  updateSizingOutputs(entry);
}

function updateSizingOutputs(entry) {
  var output = calculateLot(entry);
  document.getElementById("lot-output").textContent = output ? output.lotSize : "N/A";
  document.getElementById("pip-output").textContent = output ? output.pipDistance : "N/A";
  document.getElementById("risk-output").textContent = output ? output.riskAmount : "N/A";
  document.getElementById("margin-output").textContent = output && output.marginUsed ? output.marginUsed : "N/A";
}

function fetchEntries() {
  return PMCommon.fetchWithRetry("/api/entries")
    .then(function (response) { return response.json(); })
    .then(function (data) {
      state.entries = data.entries || [];
      state.instrumentSpecs = data.instrument_specs || {};
      if (elements.lastUpdated) {
        elements.lastUpdated.textContent = data.last_updated ? new Date(data.last_updated).toLocaleTimeString() : "N/A";
      }
      if (elements.sourceFiles) {
        elements.sourceFiles.textContent = data.source_files ? data.source_files.length : 0;
      }

      updateFilterOptions();
      applyFilters();
      renderTable();
    })
    .catch(function (err) { showJsError(err ? String(err) : "Failed to fetch entries"); });
}

function fetchConfig() {
  return PMCommon.fetchWithRetry("/api/config")
    .then(function (response) { return response.json(); })
    .then(function (data) {
      state.config = data;
      document.getElementById("cfg-pm-root").value = data.pm_root || "";
      document.getElementById("cfg-refresh").value = data.refresh_interval_sec !== undefined ? data.refresh_interval_sec : 5;
      document.getElementById("cfg-min-strength").value = data.min_strength !== undefined ? data.min_strength : 0;
      document.getElementById("cfg-max-age").value = data.max_signal_age_minutes !== undefined ? data.max_signal_age_minutes : 1440;
      document.getElementById("cfg-alert-enabled").checked = data.alert && data.alert.enabled !== undefined ? data.alert.enabled : true;
      document.getElementById("cfg-sound").checked = data.alert && data.alert.sound !== undefined ? data.alert.sound : true;
      document.getElementById("cfg-alert-min").value = data.alert && data.alert.min_strength !== undefined ? data.alert.min_strength : 0;
      document.getElementById("cfg-patterns").value = (data.file_patterns || []).join("\n");
    })
    .catch(function (err) { showJsError(err ? String(err) : "Failed to fetch config"); });
}

var refreshTimer = null;

function startAutoRefresh(intervalSec) {
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
  var safeInterval = Math.max(2, Number(intervalSec || 5));
  refreshTimer = setInterval(fetchEntries, safeInterval * 1000);
}

function saveConfig(event) {
  event.preventDefault();
  var payload = {
    pm_root: document.getElementById("cfg-pm-root").value.trim(),
    refresh_interval_sec: Number(document.getElementById("cfg-refresh").value || 5),
    min_strength: Number(document.getElementById("cfg-min-strength").value || 0),
    max_signal_age_minutes: Number(document.getElementById("cfg-max-age").value || 1440),
    file_patterns: document.getElementById("cfg-patterns").value.split("\n").map(function (line) {
      return line.trim();
    }).filter(function (line) { return line; }),
    alert: {
      enabled: document.getElementById("cfg-alert-enabled").checked,
      sound: document.getElementById("cfg-sound").checked,
      min_strength: Number(document.getElementById("cfg-alert-min").value || 0),
    },
  };
  return fetch("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  })
    .then(function () { PMCommon.showToast("Config saved"); return fetchConfig(); })
    .then(function () { return fetchEntries(); })
    .then(function () { startAutoRefresh(state.config.refresh_interval_sec || 5); })
    .catch(function (err) { showJsError(err ? String(err) : "Failed to save config"); });
}

function init() {
  PMCommon.initTheme();
  PMCommon.initEscapeClose(elements.drawer);
  if (!elements.tableBody || !elements.validCount) {
    showJsError("Required DOM elements missing. Reload the page.");
    return;
  }
  if (elements.pageSize && elements.pageSize.value) {
    state.pageSize = Number(elements.pageSize.value || 20);
  }
  fetchConfig()
    .then(fetchEntries)
    .then(function () { startAutoRefresh(state.config.refresh_interval_sec || 5); })
    .catch(function (err) { showJsError(err ? String(err) : "Initialization failed"); });
}

var closeBtn = document.getElementById("close-drawer");
if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
var refreshBtn = document.getElementById("refresh-btn");
if (refreshBtn) refreshBtn.addEventListener("click", fetchEntries);
var settingsForm = document.getElementById("settings-form");
if (settingsForm) settingsForm.addEventListener("submit", saveConfig);

for (var key in elements.filters) {
  if (!Object.prototype.hasOwnProperty.call(elements.filters, key)) continue;
  var select = elements.filters[key];
  if (!select) continue;
  select.addEventListener("change", function () {
    applyFilters();
    renderTable();
  });
}

if (elements.pagePrev) {
  elements.pagePrev.addEventListener("click", function () {
    if (state.currentPage > 1) {
      state.currentPage -= 1;
      renderTable();
    }
  });
}

if (elements.pageNext) {
  elements.pageNext.addEventListener("click", function () {
    if (state.currentPage < state.totalPages) {
      state.currentPage += 1;
      renderTable();
    }
  });
}

if (elements.pageSize) {
  elements.pageSize.addEventListener("change", function () {
    state.pageSize = Number(elements.pageSize.value || 20);
    state.currentPage = 1;
    renderTable();
  });
}

if (elements.sizingForm) {
  elements.sizingForm.addEventListener("input", function () {
    updateSizingOutputs(state.selected);
    if (state.selected) {
      updateTicket(state.selected);
    }
  });
}

var copySl = document.getElementById("copy-sl");
if (copySl) {
  copySl.addEventListener("click", function () {
    var value = state.selected && state.selected.stop_loss_price !== undefined ? state.selected.stop_loss_price : "";
    PMCommon.copyToClipboard(String(value), "Stop Loss");
  });
}
var copyTp = document.getElementById("copy-tp");
if (copyTp) {
  copyTp.addEventListener("click", function () {
    var value = state.selected && state.selected.take_profit_price !== undefined ? state.selected.take_profit_price : "";
    PMCommon.copyToClipboard(String(value), "Take Profit");
  });
}
var copyEntry = document.getElementById("copy-entry");
if (copyEntry) {
  copyEntry.addEventListener("click", function () {
    var symbol = state.selected && state.selected.symbol ? state.selected.symbol : "";
    var entry = state.selected && state.selected.entry_price !== undefined ? state.selected.entry_price : "";
    PMCommon.copyToClipboard(symbol + " " + entry, "Entry");
  });
}
var copyTicket = document.getElementById("copy-ticket");
if (copyTicket) {
  copyTicket.addEventListener("click", function () {
    PMCommon.copyToClipboard(elements.ticketBox ? elements.ticketBox.textContent : "", "Ticket");
  });
}

window.addEventListener("load", init);

if (elements.useEntryBtn) {
  elements.useEntryBtn.addEventListener("click", function () {
    if (!state.selected) return;
    syncSizingInputs(state.selected);
    var sizingSection = document.getElementById("sizing-section");
    if (sizingSection && sizingSection.scrollIntoView) {
      sizingSection.scrollIntoView({ behavior: "smooth" });
    }
  });
}

if (elements.drawerUseEntryBtn) {
  elements.drawerUseEntryBtn.addEventListener("click", function () {
    if (!state.selected) return;
    syncSizingInputs(state.selected);
    var sizingSection = document.getElementById("sizing-section");
    if (sizingSection && sizingSection.scrollIntoView) {
      sizingSection.scrollIntoView({ behavior: "smooth" });
    }
  });
}
