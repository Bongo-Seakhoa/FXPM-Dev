var state = {
  entries: [],
  filtered: [],
  displayed: [],
  selected: null,
  selectedEntryId: "",
  instrumentSpecs: {},
  config: {},
  pageSize: 20,
  currentPage: 1,
  totalPages: 1,
  latestOnly: true,
  sortBy: "recent"
};

var VIEW_PREFS_KEY = "pm-signaldesk-view";
var SIZING_PREFS_KEY = "pm-signaldesk-sizing";

var elements = {
  validCount: document.getElementById("valid-count"),
  totalCount: document.getElementById("total-count"),
  filteredCount: document.getElementById("filtered-count"),
  lastUpdated: document.getElementById("last-updated"),
  staleness: document.getElementById("staleness"),
  sourceFiles: document.getElementById("source-files"),
  statusBanner: document.getElementById("status-banner"),
  tableBody: document.getElementById("entries-body"),
  drawer: document.getElementById("drawer"),
  drawerTitle: document.getElementById("drawer-title"),
  detailsGrid: document.getElementById("details-grid"),
  ticketBox: document.getElementById("ticket-box"),
  selectedSummary: document.getElementById("selected-summary"),
  sizingForm: document.getElementById("sizing-form"),
  useEntryBtn: document.getElementById("use-entry-btn"),
  clearSelectionBtn: document.getElementById("clear-selection-btn"),
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
  searchInput: document.getElementById("filter-search"),
  sortBy: document.getElementById("sort-by"),
  latestOnly: document.getElementById("latest-only"),
  filters: {
    symbol: document.getElementById("filter-symbol"),
    timeframe: document.getElementById("filter-timeframe"),
    regime: document.getElementById("filter-regime"),
    direction: document.getElementById("filter-direction"),
    strategy: document.getElementById("filter-strategy")
  }
};

function showStatus(message, tone) {
  if (!elements.statusBanner) return;
  if (!message) {
    elements.statusBanner.className = "status-banner hidden";
    elements.statusBanner.textContent = "";
    return;
  }
  elements.statusBanner.className = "status-banner tone-" + (tone || "info");
  elements.statusBanner.textContent = message;
}

function showJsError(message) {
  if (elements.jsError) {
    elements.jsError.style.display = "block";
    elements.jsError.textContent = "Dashboard JS error: " + message;
  }
  showStatus(message, "error");
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

function entryStrength(entry) {
  var value = Number(entry.signal_strength);
  return Number.isFinite(value) ? value : 0;
}

function entryRR(entry) {
  var entryPrice = Number(entry.entry_price);
  var sl = Number(entry.stop_loss_price);
  var tp = Number(entry.take_profit_price);
  if (!Number.isFinite(entryPrice) || !Number.isFinite(sl) || !Number.isFinite(tp)) return null;
  var risk = Math.abs(entryPrice - sl);
  if (risk <= 0) return null;
  return Math.abs(tp - entryPrice) / risk;
}

function sortEntries(items) {
  return items.sort(function (a, b) {
    if (state.sortBy === "symbol") {
      var compare = String(a.symbol || "").localeCompare(String(b.symbol || ""));
      if (compare !== 0) return compare;
      return entryTimeValue(b) - entryTimeValue(a);
    }
    if (state.sortBy === "strength") {
      var diff = entryStrength(b) - entryStrength(a);
      if (diff !== 0) return diff;
      return entryTimeValue(b) - entryTimeValue(a);
    }
    var timeDiff = entryTimeValue(b) - entryTimeValue(a);
    if (timeDiff !== 0) return timeDiff;
    return entryStrength(b) - entryStrength(a);
  });
}

function findById(entries, entryId) {
  for (var i = 0; i < entries.length; i += 1) {
    if (entries[i].entry_id === entryId) return entries[i];
  }
  return null;
}

function updateFilterOptions() {
  var options = {
    symbol: PMCommon.uniqueValues(state.entries, "symbol"),
    timeframe: PMCommon.uniqueValues(state.entries, "timeframe"),
    regime: PMCommon.uniqueValues(state.entries, "regime"),
    direction: PMCommon.uniqueValues(state.entries, "signal_direction"),
    strategy: PMCommon.uniqueValues(state.entries, "strategy_name")
  };

  for (var key in elements.filters) {
    if (!Object.prototype.hasOwnProperty.call(elements.filters, key)) continue;
    var select = elements.filters[key];
    if (!select) continue;
    var current = select.value;

    var html = '<option value="">All</option>';
    for (var index = 0; index < options[key].length; index += 1) {
      var value = options[key][index];
      html += '<option value="' + PMCommon.escapeHtml(value) + '">' + PMCommon.escapeHtml(value) + "</option>";
    }
    select.innerHTML = html;
    if (options[key].indexOf(current) !== -1) select.value = current;
  }
}

function reduceEntries(entries) {
  if (!state.latestOnly) return entries.slice();

  var grouped = {};
  for (var i = 0; i < entries.length; i += 1) {
    var entry = entries[i];
    var key = [entry.symbol || "N/A", entry.timeframe || "N/A"].join("|");
    var current = grouped[key];
    if (!current || entryTimeValue(entry) > entryTimeValue(current)) {
      grouped[key] = entry;
    }
  }

  return Object.keys(grouped).map(function (key) {
    return grouped[key];
  });
}

function applyFilters() {
  var searchTerm = (elements.searchInput && elements.searchInput.value ? elements.searchInput.value : "").trim().toLowerCase();

  state.filtered = state.entries.filter(function (entry) {
    if (elements.filters.symbol && elements.filters.symbol.value && entry.symbol !== elements.filters.symbol.value) return false;
    if (elements.filters.timeframe && elements.filters.timeframe.value && entry.timeframe !== elements.filters.timeframe.value) return false;
    if (elements.filters.regime && elements.filters.regime.value && entry.regime !== elements.filters.regime.value) return false;
    if (elements.filters.direction && elements.filters.direction.value && entry.signal_direction !== elements.filters.direction.value) return false;
    if (elements.filters.strategy && elements.filters.strategy.value && entry.strategy_name !== elements.filters.strategy.value) return false;

    if (searchTerm) {
      var text = [
        entry.symbol,
        entry.timeframe,
        entry.regime,
        entry.strategy_name,
        entry.reason,
        entry.signal_direction
      ].join(" ").toLowerCase();
      if (text.indexOf(searchTerm) === -1) return false;
    }

    return true;
  });

  state.displayed = reduceEntries(state.filtered);
  state.currentPage = 1;
}

function updateCounts() {
  var valid = 0;
  for (var i = 0; i < state.displayed.length; i += 1) {
    if (state.displayed[i].valid_now) valid += 1;
  }

  if (elements.validCount) elements.validCount.textContent = String(valid);
  if (elements.totalCount) elements.totalCount.textContent = String(state.entries.length);
  if (elements.filteredCount) elements.filteredCount.textContent = String(state.displayed.length);
}

function updatePagination() {
  var total = state.displayed.length;
  state.totalPages = Math.max(1, Math.ceil(total / state.pageSize));
  if (state.currentPage > state.totalPages) state.currentPage = state.totalPages;

  if (elements.pageInfo) elements.pageInfo.textContent = "Page " + state.currentPage + " of " + state.totalPages;
  if (elements.pageCount) elements.pageCount.textContent = total + " rows";
  if (elements.pagePrev) elements.pagePrev.disabled = state.currentPage <= 1;
  if (elements.pageNext) elements.pageNext.disabled = state.currentPage >= state.totalPages;
}

function pageEntries() {
  var sorted = sortEntries(state.displayed.slice());
  var start = (state.currentPage - 1) * state.pageSize;
  return sorted.slice(start, start + state.pageSize);
}

function strategyCell(entry) {
  if (!entry.strategy_name) return "N/A";
  var params = [];
  if (entry.symbol) params.push("symbol=" + encodeURIComponent(entry.symbol));
  if (entry.timeframe) params.push("timeframe=" + encodeURIComponent(entry.timeframe));
  if (entry.regime) params.push("regime=" + encodeURIComponent(entry.regime));
  if (entry.strategy_name) params.push("strategy=" + encodeURIComponent(entry.strategy_name));
  var href = "/strategies" + (params.length ? "?" + params.join("&") : "");
  return '<a class="strategy-link" href="' + href + '">' + PMCommon.escapeHtml(entry.strategy_name) + "</a>";
}
function renderTable() {
  if (!elements.tableBody) return;

  updateCounts();
  updatePagination();
  elements.tableBody.innerHTML = "";

  var rows = pageEntries();
  if (!rows.length) {
    var emptyRow = document.createElement("tr");
    emptyRow.innerHTML = '<td colspan="11" class="table-empty">No entries match the current filters.</td>';
    elements.tableBody.appendChild(emptyRow);
    return;
  }

  for (var i = 0; i < rows.length; i += 1) {
    var entry = rows[i];
    var rr = entryRR(entry);
    var direction = String(entry.signal_direction || "").toLowerCase();
    var dirClass = direction === "buy" ? "dir-buy" : (direction === "sell" ? "dir-sell" : "");

    var row = document.createElement("tr");
    row.dataset.entryId = entry.entry_id || "";
    row.tabIndex = 0;
    row.classList.add(entry.valid_now ? "row-valid" : "row-invalid");
    if (state.selectedEntryId && entry.entry_id === state.selectedEntryId) row.classList.add("selected");

    row.innerHTML =
      "<td>" + PMCommon.escapeHtml(entry.symbol || "N/A") + "</td>" +
      "<td>" + PMCommon.escapeHtml(entry.timeframe || "N/A") + "</td>" +
      "<td>" + PMCommon.escapeHtml(entry.regime || "N/A") + "</td>" +
      '<td class="' + dirClass + '">' + PMCommon.escapeHtml((entry.signal_direction || "N/A").toUpperCase()) + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.entry_price) + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.stop_loss_price) + "</td>" +
      "<td>" + PMCommon.formatNumber(entry.take_profit_price) + "</td>" +
      "<td>" + (rr !== null ? rr.toFixed(2) : "N/A") + "</td>" +
      "<td>" + (entry.signal_strength !== undefined && entry.signal_strength !== null ? PMCommon.formatNumber(entry.signal_strength, 2) : "N/A") + "</td>" +
      "<td>" + PMCommon.formatRelativeTime(entry.timestamp) + "</td>" +
      "<td>" + strategyCell(entry) + "</td>";

    row.addEventListener("click", createOpenHandler(entry));
    row.addEventListener("keydown", createKeyHandler(entry));

    var strategyLink = row.querySelector(".strategy-link");
    if (strategyLink) {
      strategyLink.addEventListener("click", function (event) {
        event.stopPropagation();
      });
    }

    elements.tableBody.appendChild(row);
  }
}

function createOpenHandler(entry) {
  return function () {
    openDrawer(entry);
  };
}

function createKeyHandler(entry) {
  return function (event) {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openDrawer(entry);
    }
  };
}

function updateSummary(entry) {
  if (!elements.selectedSummary) return;
  if (!entry) {
    elements.selectedSummary.textContent = "No entry selected.";
    return;
  }

  var rr = entryRR(entry);
  var text = [
    entry.symbol || "N/A",
    (entry.signal_direction || "N/A").toUpperCase(),
    entry.timeframe || "N/A",
    entry.regime || "N/A"
  ].join(" | ");

  if (rr !== null) text += " | R:R " + rr.toFixed(2);
  elements.selectedSummary.textContent = text;
}

function defaultPipSize(symbol) {
  var upper = String(symbol || "").toUpperCase();
  if (upper.indexOf("JPY") !== -1) return 0.01;
  if (upper.indexOf("XAU") === 0) return 0.1;
  if (upper.indexOf("XAG") === 0) return 0.01;
  if (upper.indexOf("US30") === 0 || upper.indexOf("US100") === 0 || upper.indexOf("DE30") === 0) return 1;
  return 0.0001;
}

function defaultPipValue(symbol) {
  var upper = String(symbol || "").toUpperCase();
  if (upper.indexOf("JPY") !== -1) return 9;
  if (upper.indexOf("XAU") === 0 || upper.indexOf("XAG") === 0) return 1;
  if (upper.indexOf("US30") === 0 || upper.indexOf("US100") === 0 || upper.indexOf("DE30") === 0) return 1;
  if (upper.indexOf("BTC") === 0 || upper.indexOf("ETH") === 0 || upper.indexOf("XRP") === 0 || upper.indexOf("TON") === 0) return 1;
  return 10;
}

function sizingEntry(entry) {
  if (entry) return entry;
  return {
    symbol: document.getElementById("sizing-symbol").value || "",
    entry_price: Number(document.getElementById("sizing-entry").value || 0),
    stop_loss_price: Number(document.getElementById("sizing-sl").value || 0),
    take_profit_price: Number(document.getElementById("sizing-tp").value || 0)
  };
}

function calculateLot(entry) {
  var activeEntry = sizingEntry(entry);
  var symbol = activeEntry.symbol || "";

  var equity = Number(document.getElementById("equity").value || 0);
  var leverage = Number(document.getElementById("leverage").value || 0);
  var riskPct = Number(document.getElementById("risk-pct").value || 0);
  var riskFixed = Number(document.getElementById("risk-fixed").value || 0);

  var modeRadio = document.querySelector("input[name='risk-mode']:checked");
  var riskMode = modeRadio ? modeRadio.value : "percent";

  var entryPrice = Number(activeEntry.entry_price || 0);
  var stopLoss = Number(activeEntry.stop_loss_price || 0);
  var takeProfit = Number(activeEntry.take_profit_price || 0);

  if (!equity || !entryPrice || !stopLoss) return null;

  var spec = state.instrumentSpecs[symbol] || {};
  var pipPos = spec.pip_position !== undefined ? Number(spec.pip_position) : null;
  var pipSize = Number.isFinite(pipPos) ? Math.pow(10, -pipPos) : defaultPipSize(symbol);

  var pipValue = spec.pip_value !== undefined ? Number(spec.pip_value) : defaultPipValue(symbol);
  if (!Number.isFinite(pipValue) || pipValue <= 0) pipValue = defaultPipValue(symbol);

  var riskAmount = riskMode === "fixed" ? riskFixed : (equity * (riskPct / 100));
  if (!riskAmount) return null;

  var stopDistance = Math.abs(entryPrice - stopLoss);
  var pipDistance = pipSize ? stopDistance / pipSize : 0;
  if (!pipDistance || !pipValue) return null;

  var riskPerLot = pipDistance * pipValue;
  var rawLot = riskAmount / riskPerLot;

  var minLot = spec.min_lot !== undefined ? Number(spec.min_lot) : 0.01;
  var maxLot = spec.max_lot !== undefined ? Number(spec.max_lot) : 100;
  var step = spec.lot_step !== undefined ? Number(spec.lot_step) : 0.01;

  if (!step || step <= 0) step = 0.01;
  if (!minLot || minLot <= 0) minLot = 0.01;
  if (!maxLot || maxLot < minLot) maxLot = 100;

  var lotSize = Math.max(minLot, Math.min(maxLot, rawLot));
  lotSize = Math.floor(lotSize / step) * step;
  if (lotSize < minLot) lotSize = minLot;

  var marginUsed = null;
  if (leverage > 0) {
    var contractSize = spec.contract_size !== undefined ? Number(spec.contract_size) : 100000;
    marginUsed = (entryPrice * contractSize * lotSize) / leverage;
  }

  var rr = null;
  if (takeProfit) {
    var risk = Math.abs(entryPrice - stopLoss);
    var reward = Math.abs(takeProfit - entryPrice);
    if (risk > 0) rr = reward / risk;
  }

  return {
    lotSize: lotSize,
    pipDistance: pipDistance,
    riskAmount: riskAmount,
    marginUsed: marginUsed,
    rr: rr
  };
}

function syncSizing(entry) {
  var symbol = entry && entry.symbol ? entry.symbol : "";
  var entryPrice = entry && entry.entry_price !== undefined && entry.entry_price !== null ? entry.entry_price : "";
  var stopLoss = entry && entry.stop_loss_price !== undefined && entry.stop_loss_price !== null ? entry.stop_loss_price : "";
  var takeProfit = entry && entry.take_profit_price !== undefined && entry.take_profit_price !== null ? entry.take_profit_price : "";

  document.getElementById("sizing-symbol").value = symbol;
  document.getElementById("sizing-entry").value = entryPrice;
  document.getElementById("sizing-sl").value = stopLoss;
  document.getElementById("sizing-tp").value = takeProfit;

  if (elements.signalSl) elements.signalSl.textContent = PMCommon.formatNumber(stopLoss);
  if (elements.signalEntry) elements.signalEntry.textContent = PMCommon.formatNumber(entryPrice);
  if (elements.signalTp) elements.signalTp.textContent = PMCommon.formatNumber(takeProfit);
  if (elements.signalSymbol) elements.signalSymbol.textContent = symbol || "N/A";
  if (elements.signalDirection) elements.signalDirection.textContent = entry && entry.signal_direction ? String(entry.signal_direction).toUpperCase() : "N/A";
}

function updateSizingOutputs(entry) {
  var output = calculateLot(entry);
  document.getElementById("lot-output").textContent = output ? output.lotSize.toFixed(2) : "N/A";
  document.getElementById("pip-output").textContent = output ? output.pipDistance.toFixed(1) : "N/A";
  document.getElementById("risk-output").textContent = output ? PMCommon.formatCurrency(output.riskAmount) : "N/A";
  document.getElementById("margin-output").textContent = output && output.marginUsed !== null ? PMCommon.formatCurrency(output.marginUsed) : "N/A";
  document.getElementById("rr-output").textContent = output && output.rr !== null ? output.rr.toFixed(2) : "N/A";
}

function updateTicket(entry) {
  if (!elements.ticketBox) return;
  if (!entry) {
    elements.ticketBox.textContent = "Select an entry to generate a ticket.";
    return;
  }

  var lot = calculateLot(entry);
  var rr = entryRR(entry);
  var lines = [
    "Symbol: " + (entry.symbol || "N/A"),
    "Direction: " + (entry.signal_direction ? String(entry.signal_direction).toUpperCase() : "N/A"),
    "Entry: " + PMCommon.formatNumber(entry.entry_price),
    "SL: " + PMCommon.formatNumber(entry.stop_loss_price),
    "TP: " + PMCommon.formatNumber(entry.take_profit_price),
    "R:R: " + (rr !== null ? rr.toFixed(2) : "N/A"),
    "Size: " + (lot ? lot.lotSize.toFixed(2) : "N/A") + " lots",
    "Timeframe: " + (entry.timeframe || "N/A"),
    "Regime: " + (entry.regime || "N/A"),
    "Strategy: " + (entry.strategy_name || "N/A")
  ];

  elements.ticketBox.textContent = lines.join("\n");
}

function renderDetails(entry) {
  if (!elements.detailsGrid || !entry) return;

  var rr = entryRR(entry);
  var details = [
    ["Symbol", entry.symbol],
    ["Timeframe", entry.timeframe],
    ["Regime", entry.regime],
    ["Strategy", entry.strategy_name],
    ["Direction", entry.signal_direction ? entry.signal_direction.toUpperCase() : "N/A"],
    ["Entry", PMCommon.formatNumber(entry.entry_price)],
    ["Stop Loss", PMCommon.formatNumber(entry.stop_loss_price)],
    ["Take Profit", PMCommon.formatNumber(entry.take_profit_price)],
    ["R:R", rr !== null ? rr.toFixed(2) : "N/A"],
    ["Strength", entry.signal_strength !== undefined && entry.signal_strength !== null ? PMCommon.formatNumber(entry.signal_strength, 2) : "N/A"],
    ["Timestamp", PMCommon.formatDateTime(entry.timestamp)],
    ["Reason", entry.reason || "N/A"],
    ["Source", entry.source || "N/A"]
  ];

  elements.detailsGrid.innerHTML = details.map(function (item) {
    return '<div class="detail-item"><span>' + PMCommon.escapeHtml(item[0]) + "</span><strong>" + PMCommon.escapeHtml(item[1] !== undefined && item[1] !== null ? String(item[1]) : "N/A") + "</strong></div>";
  }).join("");
}

function setSelection(entry) {
  state.selected = entry || null;
  state.selectedEntryId = entry && entry.entry_id ? entry.entry_id : "";

  updateSummary(state.selected);
  syncSizing(state.selected);
  updateSizingOutputs(state.selected);
  updateTicket(state.selected);
  renderTable();

  var hasSelection = Boolean(state.selected);
  if (elements.useEntryBtn) elements.useEntryBtn.disabled = !hasSelection;
  if (elements.drawerUseEntryBtn) elements.drawerUseEntryBtn.disabled = !hasSelection;
  if (elements.clearSelectionBtn) elements.clearSelectionBtn.disabled = !hasSelection;
}

function clearSelection() {
  setSelection(null);
  PMCommon.closeDrawer(elements.drawer);
}

function openDrawer(entry) {
  setSelection(entry);
  if (elements.drawerTitle) {
    elements.drawerTitle.textContent = (entry.symbol || "Entry") + " " + ((entry.signal_direction || "").toUpperCase());
  }
  renderDetails(entry);
  PMCommon.openDrawer(elements.drawer);
}

function useSelectedForSizing() {
  if (!state.selected) {
    PMCommon.showToast("Select an entry first");
    return;
  }
  syncSizing(state.selected);
  updateSizingOutputs(state.selected);
  updateTicket(state.selected);
  var section = document.getElementById("sizing-section");
  if (section && section.scrollIntoView) section.scrollIntoView({ behavior: "smooth", block: "start" });
}

function persistSizing() {
  var payload = {
    equity: document.getElementById("equity").value,
    leverage: document.getElementById("leverage").value,
    riskPct: document.getElementById("risk-pct").value,
    riskFixed: document.getElementById("risk-fixed").value,
    riskMode: (document.querySelector("input[name='risk-mode']:checked") || {}).value || "percent"
  };
  PMCommon.saveJSON(SIZING_PREFS_KEY, payload);
}

function loadSizing() {
  var payload = PMCommon.loadJSON(SIZING_PREFS_KEY, {});
  if (!payload || typeof payload !== "object") return;

  if (payload.equity !== undefined) document.getElementById("equity").value = payload.equity;
  if (payload.leverage !== undefined) document.getElementById("leverage").value = payload.leverage;
  if (payload.riskPct !== undefined) document.getElementById("risk-pct").value = payload.riskPct;
  if (payload.riskFixed !== undefined) document.getElementById("risk-fixed").value = payload.riskFixed;
  var mode = payload.riskMode === "fixed" ? "fixed" : "percent";
  var radio = document.querySelector("input[name='risk-mode'][value='" + mode + "']");
  if (radio) radio.checked = true;
}
function persistView() {
  PMCommon.saveJSON(VIEW_PREFS_KEY, {
    pageSize: state.pageSize,
    sortBy: state.sortBy,
    latestOnly: state.latestOnly
  });
}

function loadView() {
  var payload = PMCommon.loadJSON(VIEW_PREFS_KEY, {});
  if (!payload || typeof payload !== "object") return;
  if (payload.pageSize && Number(payload.pageSize) > 0) state.pageSize = Number(payload.pageSize);
  if (payload.sortBy && ["recent", "strength", "symbol"].indexOf(payload.sortBy) !== -1) state.sortBy = payload.sortBy;
  if (payload.latestOnly !== undefined) state.latestOnly = Boolean(payload.latestOnly);
}

function applyViewToDom() {
  if (elements.pageSize) elements.pageSize.value = String(state.pageSize);
  if (elements.sortBy) elements.sortBy.value = state.sortBy;
  if (elements.latestOnly) elements.latestOnly.checked = state.latestOnly;
}

function fetchEntries() {
  return PMCommon.fetchWithRetry("/api/entries")
    .then(function (response) { return response.json(); })
    .then(function (data) {
      state.entries = data.entries || [];
      state.instrumentSpecs = data.instrument_specs || {};

      if (elements.lastUpdated) elements.lastUpdated.textContent = data.last_updated ? PMCommon.formatDateTime(data.last_updated) : "N/A";
      if (elements.staleness) elements.staleness.textContent = data.last_updated ? PMCommon.formatRelativeTime(data.last_updated) : "N/A";
      if (elements.sourceFiles) elements.sourceFiles.textContent = data.source_files ? String(data.source_files.length) : "0";

      updateFilterOptions();
      applyFilters();

      if (state.selectedEntryId) {
        var selected = findById(state.entries, state.selectedEntryId);
        if (selected) {
          state.selected = selected;
          updateSummary(selected);
          syncSizing(selected);
          updateSizingOutputs(selected);
          updateTicket(selected);
        } else {
          setSelection(null);
        }
      }

      renderTable();

      if (data.last_error) {
        showStatus("Watcher warning: " + data.last_error, "warning");
      } else if (!state.entries.length) {
        showStatus("No entries found in current PM outputs.", "info");
      } else {
        showStatus("");
      }
    })
    .catch(function (err) {
      showJsError(err ? String(err) : "Failed to fetch entries");
    });
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
    .catch(function (err) {
      showJsError(err ? String(err) : "Failed to fetch config");
    });
}

var refreshTimer = null;

function startAutoRefresh(intervalSec) {
  if (refreshTimer) clearInterval(refreshTimer);
  var seconds = Math.max(2, Number(intervalSec || 5));
  refreshTimer = setInterval(fetchEntries, seconds * 1000);
}

function saveConfig(event) {
  event.preventDefault();

  var payload = {
    pm_root: document.getElementById("cfg-pm-root").value.trim(),
    refresh_interval_sec: Number(document.getElementById("cfg-refresh").value || 5),
    min_strength: Number(document.getElementById("cfg-min-strength").value || 0),
    max_signal_age_minutes: Number(document.getElementById("cfg-max-age").value || 1440),
    file_patterns: document.getElementById("cfg-patterns").value
      .split("\n")
      .map(function (line) { return line.trim(); })
      .filter(function (line) { return Boolean(line); }),
    alert: {
      enabled: document.getElementById("cfg-alert-enabled").checked,
      sound: document.getElementById("cfg-sound").checked,
      min_strength: Number(document.getElementById("cfg-alert-min").value || 0)
    }
  };

  return fetch("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  })
    .then(function (response) {
      if (!response.ok) throw new Error("HTTP " + response.status);
      return response.json();
    })
    .then(function (config) {
      state.config = config || {};
      PMCommon.showToast("Settings saved");
      startAutoRefresh(state.config.refresh_interval_sec || 5);
      return fetchEntries();
    })
    .catch(function (err) {
      showJsError(err ? String(err) : "Failed to save config");
    });
}

function resetFilters() {
  for (var key in elements.filters) {
    if (!Object.prototype.hasOwnProperty.call(elements.filters, key)) continue;
    if (elements.filters[key]) elements.filters[key].value = "";
  }
  if (elements.searchInput) elements.searchInput.value = "";
  applyFilters();
  renderTable();
}

function copyActions() {
  var copySl = document.getElementById("copy-sl");
  if (copySl) {
    copySl.addEventListener("click", function () {
      var value = state.selected && state.selected.stop_loss_price !== undefined ? state.selected.stop_loss_price : "";
      PMCommon.copyToClipboard(String(value), "Stop loss");
    });
  }

  var copyTp = document.getElementById("copy-tp");
  if (copyTp) {
    copyTp.addEventListener("click", function () {
      var value = state.selected && state.selected.take_profit_price !== undefined ? state.selected.take_profit_price : "";
      PMCommon.copyToClipboard(String(value), "Take profit");
    });
  }

  var copyEntry = document.getElementById("copy-entry");
  if (copyEntry) {
    copyEntry.addEventListener("click", function () {
      if (!state.selected) return;
      PMCommon.copyToClipboard((state.selected.symbol || "") + " " + (state.selected.entry_price || ""), "Entry");
    });
  }

  var copyTicket = document.getElementById("copy-ticket");
  if (copyTicket) {
    copyTicket.addEventListener("click", function () {
      PMCommon.copyToClipboard(elements.ticketBox ? elements.ticketBox.textContent : "", "Ticket");
    });
  }
}

function bindEvents() {
  var closeBtn = document.getElementById("close-drawer");
  if (closeBtn) closeBtn.addEventListener("click", function () { PMCommon.closeDrawer(elements.drawer); });

  var refreshBtn = document.getElementById("refresh-btn");
  if (refreshBtn) refreshBtn.addEventListener("click", fetchEntries);

  var clearFiltersBtn = document.getElementById("clear-filters-btn");
  if (clearFiltersBtn) clearFiltersBtn.addEventListener("click", resetFilters);

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

  if (elements.searchInput) {
    elements.searchInput.addEventListener("input", PMCommon.debounce(function () {
      applyFilters();
      renderTable();
    }, 180));
  }

  if (elements.sortBy) {
    elements.sortBy.addEventListener("change", function () {
      state.sortBy = elements.sortBy.value || "recent";
      persistView();
      renderTable();
    });
  }

  if (elements.latestOnly) {
    elements.latestOnly.addEventListener("change", function () {
      state.latestOnly = Boolean(elements.latestOnly.checked);
      persistView();
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
      persistView();
      renderTable();
    });
  }

  if (elements.sizingForm) {
    elements.sizingForm.addEventListener("input", function () {
      updateSizingOutputs(state.selected);
      updateTicket(state.selected);
      persistSizing();
    });
  }

  if (elements.useEntryBtn) elements.useEntryBtn.addEventListener("click", useSelectedForSizing);
  if (elements.drawerUseEntryBtn) elements.drawerUseEntryBtn.addEventListener("click", useSelectedForSizing);
  if (elements.clearSelectionBtn) elements.clearSelectionBtn.addEventListener("click", clearSelection);

  copyActions();
}

function init() {
  PMCommon.initTheme();
  PMCommon.initEscapeClose(elements.drawer, function () {
    PMCommon.closeDrawer(elements.drawer);
  });

  loadView();
  loadSizing();
  applyViewToDom();
  bindEvents();
  setSelection(null);

  if (!elements.tableBody) {
    showJsError("Required table elements were not found.");
    return;
  }

  fetchConfig()
    .then(fetchEntries)
    .then(function () { startAutoRefresh(state.config.refresh_interval_sec || 5); })
    .catch(function (err) { showJsError(err ? String(err) : "Initialization failed"); });
}

window.addEventListener("load", init);
