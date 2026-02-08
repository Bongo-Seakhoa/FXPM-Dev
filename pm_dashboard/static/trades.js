/* Trade History with Advanced Filtering, Sorting, and Export */

var tradesState = {
  allTrades: [],
  filteredTrades: [],
  displayedTrades: [],
  currentPage: 1,
  pageSize: 50,
  totalPages: 1,
  selectedTrade: null,
  sortColumn: 'timestamp',
  sortDirection: 'desc'
};

var elements = {
  tradesBody: document.getElementById('trades-body'),
  totalTrades: document.getElementById('total-trades'),
  showingCount: document.getElementById('showing-count'),
  pageInfo: document.getElementById('page-info'),
  rowCount: document.getElementById('row-count'),
  pageSize: document.getElementById('page-size'),
  pagePrev: document.getElementById('page-prev'),
  pageNext: document.getElementById('page-next'),
  searchInput: document.getElementById('search-input'),
  filters: {
    symbol: document.getElementById('filter-symbol'),
    direction: document.getElementById('filter-direction'),
    status: document.getElementById('filter-status'),
    timeframe: document.getElementById('filter-timeframe'),
    regime: document.getElementById('filter-regime')
  },
  drawer: document.getElementById('drawer'),
  drawerTitle: document.getElementById('drawer-title'),
  detailsGrid: document.getElementById('details-grid'),
  summaryTrades: document.getElementById('summary-trades'),
  summaryPnl: document.getElementById('summary-pnl'),
  summaryWinrate: document.getElementById('summary-winrate'),
  summaryAvg: document.getElementById('summary-avg')
};

function formatTimestamp(ts) {
  if (!ts) return 'N/A';
  try {
    var date = new Date(ts);
    return date.toLocaleString();
  } catch (e) {
    return ts;
  }
}

function fetchTrades() {
  return PMCommon.fetchWithRetry('/api/trades?limit=500')
    .then(function(response) { return response.json(); })
    .then(function(data) {
      tradesState.allTrades = data.trades || [];
      if (elements.totalTrades) {
        elements.totalTrades.textContent = data.total || 0;
      }
      updateFilterOptions();
      applyFilters();
      renderTable();
    })
    .catch(function(err) {
      console.error('Failed to fetch trades:', err);
      PMCommon.showToast('Failed to load trades');
    });
}

function updateFilterOptions() {
  var options = {
    symbol: PMCommon.uniqueValues(tradesState.allTrades, 'symbol'),
    direction: PMCommon.uniqueValues(tradesState.allTrades, 'direction'),
    status: PMCommon.uniqueValues(tradesState.allTrades, 'status'),
    timeframe: PMCommon.uniqueValues(tradesState.allTrades, 'timeframe'),
    regime: PMCommon.uniqueValues(tradesState.allTrades, 'regime')
  };

  for (var key in elements.filters) {
    if (!Object.prototype.hasOwnProperty.call(elements.filters, key)) continue;
    var select = elements.filters[key];
    if (!select) continue;
    var current = select.value;
    select.innerHTML = '<option value="">All</option>' +
      options[key].map(function(val) {
        return '<option>' + val + '</option>';
      }).join('');
    if (options[key].indexOf(current) !== -1) {
      select.value = current;
    }
  }
}

function applyFilters() {
  var searchTerm = elements.searchInput ? elements.searchInput.value.toLowerCase() : '';
  var filters = elements.filters;

  tradesState.filteredTrades = tradesState.allTrades.filter(function(trade) {
    if (searchTerm) {
      var searchable = [
        trade.symbol,
        trade.direction,
        trade.strategy,
        trade.timeframe,
        trade.regime
      ].join(' ').toLowerCase();
      if (searchable.indexOf(searchTerm) === -1) return false;
    }

    if (filters.symbol && filters.symbol.value && trade.symbol !== filters.symbol.value) return false;
    if (filters.direction && filters.direction.value && trade.direction !== filters.direction.value) return false;
    if (filters.status && filters.status.value && trade.status !== filters.status.value) return false;
    if (filters.timeframe && filters.timeframe.value && trade.timeframe !== filters.timeframe.value) return false;
    if (filters.regime && filters.regime.value && trade.regime !== filters.regime.value) return false;

    return true;
  });

  if (elements.showingCount) {
    elements.showingCount.textContent = tradesState.filteredTrades.length;
  }

  sortTrades();
  tradesState.currentPage = 1;
  updateSummary();
}

function sortTrades() {
  var col = tradesState.sortColumn;
  var dir = tradesState.sortDirection;

  tradesState.filteredTrades.sort(function(a, b) {
    var valA = a[col];
    var valB = b[col];

    if (col === 'timestamp') {
      var dateA = valA ? new Date(valA).getTime() : 0;
      var dateB = valB ? new Date(valB).getTime() : 0;
      return dir === 'asc' ? dateA - dateB : dateB - dateA;
    }

    if (typeof valA === 'number' && typeof valB === 'number') {
      return dir === 'asc' ? valA - valB : valB - valA;
    }

    var strA = String(valA || '').toLowerCase();
    var strB = String(valB || '').toLowerCase();
    if (strA < strB) return dir === 'asc' ? -1 : 1;
    if (strA > strB) return dir === 'asc' ? 1 : -1;
    return 0;
  });
}

function updatePagination() {
  var total = tradesState.filteredTrades.length;
  tradesState.totalPages = Math.max(1, Math.ceil(total / tradesState.pageSize));

  if (tradesState.currentPage > tradesState.totalPages) {
    tradesState.currentPage = tradesState.totalPages;
  }

  if (elements.pageInfo) {
    elements.pageInfo.textContent = 'Page ' + tradesState.currentPage + ' of ' + tradesState.totalPages;
  }

  if (elements.rowCount) {
    elements.rowCount.textContent = total + ' rows';
  }

  if (elements.pagePrev) {
    elements.pagePrev.disabled = tradesState.currentPage <= 1;
  }

  if (elements.pageNext) {
    elements.pageNext.disabled = tradesState.currentPage >= tradesState.totalPages;
  }
}

function getPagedTrades() {
  var start = (tradesState.currentPage - 1) * tradesState.pageSize;
  return tradesState.filteredTrades.slice(start, start + tradesState.pageSize);
}

function renderTable() {
  var tbody = elements.tradesBody;
  if (!tbody) return;

  tbody.innerHTML = '';
  updatePagination();

  var pageTrades = getPagedTrades();
  tradesState.displayedTrades = pageTrades;

  if (!pageTrades.length) {
    var emptyRow = document.createElement('tr');
    emptyRow.innerHTML = '<td colspan="12" style="text-align: center; padding: 40px; color: var(--muted);">No trades found matching your filters.</td>';
    tbody.appendChild(emptyRow);
    return;
  }

  pageTrades.forEach(function(trade) {
    var row = document.createElement('tr');
    var pnl = trade.pnl || 0;
    var pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
    var dirClass = trade.direction === 'LONG' ? 'dir-buy' : 'dir-sell';

    row.innerHTML =
      '<td style="font-size: 12px;">' + formatTimestamp(trade.timestamp) + '</td>' +
      '<td><strong>' + (trade.symbol || 'N/A') + '</strong></td>' +
      '<td class="' + dirClass + '">' + (trade.direction || 'N/A') + '</td>' +
      '<td>' + PMCommon.formatNumber(trade.volume, 2) + '</td>' +
      '<td>' + PMCommon.formatNumber(trade.price, 5) + '</td>' +
      '<td>' + PMCommon.formatNumber(trade.sl, 5) + '</td>' +
      '<td>' + PMCommon.formatNumber(trade.tp, 5) + '</td>' +
      '<td class="' + pnlClass + '"><strong>' + PMCommon.formatCurrency(pnl) + '</strong></td>' +
      '<td>' + (trade.timeframe || 'N/A') + '</td>' +
      '<td>' + (trade.regime || 'N/A') + '</td>' +
      '<td style="font-size: 12px;">' + (trade.strategy || 'N/A') + '</td>' +
      '<td>' + (trade.status || 'N/A') + '</td>';

    row.style.cursor = 'pointer';
    row.addEventListener('click', function() { openDrawer(trade); });

    tbody.appendChild(row);
  });

  updateSortHeaders();
}

function updateSortHeaders() {
  var headers = document.querySelectorAll('th.sortable');
  headers.forEach(function(th) {
    var col = th.getAttribute('data-sort');
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (col === tradesState.sortColumn) {
      th.classList.add(tradesState.sortDirection === 'asc' ? 'sorted-asc' : 'sorted-desc');
    }
  });
}

function handleSort(event) {
  var th = event.target.closest('th.sortable');
  if (!th) return;

  var col = th.getAttribute('data-sort');
  if (tradesState.sortColumn === col) {
    tradesState.sortDirection = tradesState.sortDirection === 'asc' ? 'desc' : 'asc';
  } else {
    tradesState.sortColumn = col;
    tradesState.sortDirection = 'desc';
  }

  applyFilters();
  renderTable();
}

function updateSummary() {
  var trades = tradesState.filteredTrades;
  var pnls = trades.map(function(t) { return t.pnl || 0; });
  var totalPnl = pnls.reduce(function(sum, p) { return sum + p; }, 0);
  var wins = pnls.filter(function(p) { return p > 0; }).length;
  var winRate = trades.length > 0 ? (wins / trades.length * 100) : 0;
  var avgPnl = trades.length > 0 ? totalPnl / trades.length : 0;

  if (elements.summaryTrades) elements.summaryTrades.textContent = trades.length;
  if (elements.summaryPnl) elements.summaryPnl.textContent = PMCommon.formatCurrency(totalPnl);
  if (elements.summaryWinrate) elements.summaryWinrate.textContent = winRate.toFixed(2) + '%';
  if (elements.summaryAvg) elements.summaryAvg.textContent = PMCommon.formatCurrency(avgPnl);
}

function openDrawer(trade) {
  tradesState.selectedTrade = trade;
  PMCommon.openDrawer(elements.drawer);

  if (elements.drawerTitle) {
    elements.drawerTitle.textContent = (trade.symbol || 'Trade') + ' ' + (trade.direction || '');
  }

  if (elements.detailsGrid) {
    var details = [
      ['Timestamp', formatTimestamp(trade.timestamp)],
      ['Symbol', trade.symbol || 'N/A'],
      ['Direction', trade.direction || 'N/A'],
      ['Volume', PMCommon.formatNumber(trade.volume, 2)],
      ['Entry Price', PMCommon.formatNumber(trade.price, 5)],
      ['Stop Loss', PMCommon.formatNumber(trade.sl, 5)],
      ['Take Profit', PMCommon.formatNumber(trade.tp, 5)],
      ['P&L', PMCommon.formatCurrency(trade.pnl || 0)],
      ['Status', trade.status || 'N/A'],
      ['Timeframe', trade.timeframe || 'N/A'],
      ['Regime', trade.regime || 'N/A'],
      ['Strategy', trade.strategy || 'N/A'],
      ['Magic Number', trade.magic || 'N/A']
    ];

    elements.detailsGrid.innerHTML = details.map(function(item) {
      return '<div class="detail-item"><span>' + item[0] + '</span><strong>' + item[1] + '</strong></div>';
    }).join('');
  }
}

function closeDrawer() {
  PMCommon.closeDrawer(elements.drawer);
}

function exportToCSV() {
  if (!tradesState.filteredTrades.length) {
    PMCommon.showToast('No trades to export');
    return;
  }

  var headers = ['Timestamp', 'Symbol', 'Direction', 'Volume', 'Entry Price', 'Stop Loss', 'Take Profit', 'P&L', 'Status', 'Timeframe', 'Regime', 'Strategy', 'Magic'];
  var rows = [headers.join(',')];

  tradesState.filteredTrades.forEach(function(trade) {
    var row = [
      trade.timestamp || '',
      trade.symbol || '',
      trade.direction || '',
      trade.volume || 0,
      trade.price || 0,
      trade.sl || 0,
      trade.tp || 0,
      trade.pnl || 0,
      trade.status || '',
      trade.timeframe || '',
      trade.regime || '',
      trade.strategy || '',
      trade.magic || ''
    ];
    rows.push(row.join(','));
  });

  var csv = rows.join('\n');
  var blob = new Blob([csv], { type: 'text/csv' });
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = 'pm_trade_history_' + new Date().toISOString().split('T')[0] + '.csv';
  a.click();
  URL.revokeObjectURL(url);
  PMCommon.showToast('CSV exported');
}

function resetFilters() {
  if (elements.searchInput) elements.searchInput.value = '';
  for (var key in elements.filters) {
    if (elements.filters[key]) elements.filters[key].value = '';
  }
  applyFilters();
  renderTable();
}

function copyTradeData() {
  if (!tradesState.selectedTrade) return;
  var trade = tradesState.selectedTrade;
  var text = 'Symbol: ' + (trade.symbol || '') + '\n' +
    'Direction: ' + (trade.direction || '') + '\n' +
    'Entry: ' + (trade.price || '') + '\n' +
    'SL: ' + (trade.sl || '') + '\n' +
    'TP: ' + (trade.tp || '') + '\n' +
    'P&L: ' + (trade.pnl || 0) + '\n' +
    'Status: ' + (trade.status || '');

  PMCommon.copyToClipboard(text, 'Trade data');
}

var _debouncedApplyFilters = PMCommon.debounce(function() {
  applyFilters();
  renderTable();
}, 300);

function init() {
  PMCommon.initTheme();
  PMCommon.initScrollToTop();
  PMCommon.initEscapeClose(elements.drawer);
  fetchTrades();

  var closeBtn = document.getElementById('close-drawer');
  if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

  var exportBtn = document.getElementById('export-csv-btn');
  if (exportBtn) exportBtn.addEventListener('click', exportToCSV);

  var resetBtn = document.getElementById('reset-filters-btn');
  if (resetBtn) resetBtn.addEventListener('click', resetFilters);

  var copyBtn = document.getElementById('copy-trade-data');
  if (copyBtn) copyBtn.addEventListener('click', copyTradeData);

  if (elements.searchInput) {
    elements.searchInput.addEventListener('input', _debouncedApplyFilters);
  }

  for (var key in elements.filters) {
    if (elements.filters[key]) {
      elements.filters[key].addEventListener('change', function() {
        applyFilters();
        renderTable();
      });
    }
  }

  if (elements.pagePrev) {
    elements.pagePrev.addEventListener('click', function() {
      if (tradesState.currentPage > 1) {
        tradesState.currentPage -= 1;
        renderTable();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });
  }

  if (elements.pageNext) {
    elements.pageNext.addEventListener('click', function() {
      if (tradesState.currentPage < tradesState.totalPages) {
        tradesState.currentPage += 1;
        renderTable();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });
  }

  if (elements.pageSize) {
    elements.pageSize.addEventListener('change', function() {
      tradesState.pageSize = Number(elements.pageSize.value || 50);
      tradesState.currentPage = 1;
      renderTable();
    });
  }

  var thead = document.querySelector('thead');
  if (thead) {
    thead.addEventListener('click', handleSort);
  }
}

window.addEventListener('load', init);
