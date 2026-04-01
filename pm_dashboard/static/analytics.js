/* Analytics Dashboard with Chart.js */

var analyticsState = {
  data: null,
  charts: {},
  simulatedData: null,
  currentReturnBasis: 'dollar'
};

function canUseCharts() {
  return typeof Chart !== 'undefined';
}

var chartColors = {
  primary: '#0b6e6e',
  success: '#0f8a5f',
  danger: '#b83333',
  warning: '#d97706',
  info: '#1f3c88',
  accent: '#e06d3c',
  grid: 'rgba(0, 0, 0, 0.05)'
};

var chartColorsDark = {
  primary: '#58a6a6',
  success: '#3fb950',
  danger: '#f85149',
  warning: '#f0884d',
  info: '#79a9ff',
  accent: '#f0884d',
  grid: 'rgba(255, 255, 255, 0.05)'
};

function getChartColors() {
  return PMCommon.isDark() ? chartColorsDark : chartColors;
}

function getChartTextColor() {
  return PMCommon.isDark() ? '#e8eef6' : '#1c222b';
}

function getChartMutedColor() {
  return PMCommon.isDark() ? '#a4afbc' : '#5f6774';
}

function applyChartThemeDefaults() {
  if (!canUseCharts()) return;
  var colors = getChartColors();
  Chart.defaults.color = getChartMutedColor();
  Chart.defaults.borderColor = colors.grid;
  Chart.defaults.plugins.legend.labels.color = getChartTextColor();
  Chart.defaults.plugins.tooltip.backgroundColor = PMCommon.isDark() ? 'rgba(17, 23, 31, 0.94)' : 'rgba(255, 255, 255, 0.97)';
  Chart.defaults.plugins.tooltip.titleColor = getChartTextColor();
  Chart.defaults.plugins.tooltip.bodyColor = getChartTextColor();
  Chart.defaults.plugins.tooltip.borderColor = colors.grid;
  Chart.defaults.plugins.tooltip.borderWidth = 1;
}

function setAnalyticsLoading(isLoading, message) {
  PMCommon.setLoadingState(document.getElementById('analytics-loading'), isLoading, message || 'Loading analytics...');
}

function showChartUnavailableState() {
  var containers = document.querySelectorAll('.chart-container');
  for (var i = 0; i < containers.length; i += 1) {
    var container = containers[i];
    if (!container) continue;
    var canvas = container.querySelector('canvas');
    if (canvas) canvas.style.display = 'none';
    if (!container.querySelector('.chart-fallback-card')) {
      var fallback = document.createElement('div');
      fallback.className = 'chart-fallback-card';
      fallback.textContent = 'Charts are temporarily unavailable. KPI cards and tables remain live.';
      container.appendChild(fallback);
    }
  }
}

function fetchAnalytics() {
  setAnalyticsLoading(true, 'Loading analytics...');
  return PMCommon.fetchWithRetry('/api/analytics')
    .then(function(response) { return response.json(); })
    .then(function(data) {
      analyticsState.data = data;
      if (!data.has_data) {
        showNoDataMessage();
        return;
      }
      renderKPIs(data.metrics);
      renderEquityChart(data.equity_curve);
      renderDrawdownChart(data.drawdown_curve);
      renderSymbolChart(data.by_symbol);
      renderTimeframeChart(data.by_timeframe);
      renderMonthlyChart(data.monthly);
      renderDailyPnLChart(data.daily_pnl);
      renderTradeStats(data.metrics);
      renderRecentTrades(data.recent_trades);
      renderRegimeBreakdown(data.by_regime);
      if (data.heatmap) renderHeatmap(data.heatmap);
      if (data.strategy_ranking) renderStrategyRanking(data.strategy_ranking);
      updateTradesCount(data.total_trades_loaded);
    })
    .catch(function(err) {
      console.error('Failed to fetch analytics:', err);
      PMCommon.showToast('Failed to load analytics');
    })
    .finally(function() {
      setAnalyticsLoading(false);
    });
}

function showNoDataMessage() {
  var main = document.querySelector('main');
  if (main) {
    main.innerHTML = '<div class="card text-center" style="padding: 60px;"><h3>No Trade Data Available</h3><p class="text-muted">No trades found in pm_outputs/trades_*.json files.</p></div>';
  }
}

function renderKPIs(metrics) {
  document.getElementById('total-return').textContent = PMCommon.formatPercentage(metrics.total_return_pct);
  document.getElementById('max-dd-stat').textContent = PMCommon.formatPercentage(metrics.max_drawdown_pct);
  document.getElementById('sharpe-stat').textContent = PMCommon.formatNumber(metrics.sharpe_ratio, 2);

  document.getElementById('kpi-total-trades').textContent = metrics.total_trades || 0;
  document.getElementById('kpi-win-rate').textContent = PMCommon.formatPercentage(metrics.win_rate);
  document.getElementById('kpi-profit-factor').textContent = PMCommon.formatNumber(metrics.profit_factor, 2);
  document.getElementById('kpi-total-pnl').textContent = PMCommon.formatCurrency(metrics.total_pnl);

  // Extended KPIs
  var el;
  el = document.getElementById('kpi-sortino');
  if (el) el.textContent = PMCommon.formatNumber(metrics.sortino_ratio, 2);
  el = document.getElementById('kpi-calmar');
  if (el) el.textContent = PMCommon.formatNumber(metrics.calmar_ratio, 2);
  el = document.getElementById('kpi-recovery');
  if (el) el.textContent = PMCommon.formatNumber(metrics.recovery_factor, 2);
  el = document.getElementById('kpi-expectancy');
  if (el) el.textContent = PMCommon.formatCurrency(metrics.expectancy);
  el = document.getElementById('kpi-max-consec-wins');
  if (el) el.textContent = metrics.max_consecutive_wins || 0;
  el = document.getElementById('kpi-max-consec-losses');
  if (el) el.textContent = metrics.max_consecutive_losses || 0;

  // Extended risk metrics
  el = document.getElementById('kpi-dd-duration');
  if (el) el.textContent = (metrics.drawdown_duration || 0) + ' trades';
  el = document.getElementById('kpi-recovery-time');
  if (el) el.textContent = (metrics.recovery_time || 0) + ' trades';
  el = document.getElementById('kpi-ulcer-index');
  if (el) el.textContent = PMCommon.formatNumber(metrics.ulcer_index, 4);
  el = document.getElementById('kpi-long-pf');
  if (el) el.textContent = PMCommon.formatNumber(metrics.long_profit_factor, 2);
  el = document.getElementById('kpi-short-pf');
  if (el) el.textContent = PMCommon.formatNumber(metrics.short_profit_factor, 2);
  el = document.getElementById('kpi-avg-trade');
  if (el) el.textContent = PMCommon.formatCurrency(metrics.avg_trade_pnl);
}

function renderEquityChart(equityCurve) {
  if (!canUseCharts()) return;
  if (!equityCurve || !equityCurve.length) return;

  var colors = getChartColors();
  var ctx = document.getElementById('equity-chart');
  if (!ctx) return;

  if (analyticsState.charts.equity) {
    analyticsState.charts.equity.destroy();
  }

  var labels = equityCurve.map(function(p) { return p.timestamp; });
  var data = equityCurve.map(function(p) { return p.equity; });

  analyticsState.charts.equity = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Equity',
        data: data,
        borderColor: colors.primary,
        backgroundColor: PMCommon.isDark() ? 'rgba(88, 166, 166, 0.1)' : 'rgba(11, 110, 110, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'Equity: ' + PMCommon.formatCurrency(context.parsed.y);
            }
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            maxTicksLimit: 8,
            callback: function(value) {
              var label = this.getLabelForValue(value);
              if (label) {
                try {
                  var date = new Date(label);
                  return date.toLocaleDateString();
                } catch (e) {
                  return label;
                }
              }
              return '';
            }
          }
        },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return '$' + value.toFixed(0);
            }
          }
        }
      }
    }
  });
}

function renderDrawdownChart(drawdownCurve) {
  if (!canUseCharts()) return;
  if (!drawdownCurve || !drawdownCurve.length) return;

  var colors = getChartColors();
  var ctx = document.getElementById('drawdown-chart');
  if (!ctx) return;

  if (analyticsState.charts.drawdown) {
    analyticsState.charts.drawdown.destroy();
  }

  var labels = drawdownCurve.map(function(p) { return p.timestamp; });
  var data = drawdownCurve.map(function(p) { return -p.drawdown_pct; });

  analyticsState.charts.drawdown = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Drawdown %',
        data: data,
        borderColor: colors.danger,
        backgroundColor: PMCommon.isDark() ? 'rgba(248, 81, 73, 0.1)' : 'rgba(184, 51, 51, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'Drawdown: ' + (-context.parsed.y).toFixed(2) + '%';
            }
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            maxTicksLimit: 8,
            callback: function(value) {
              var label = this.getLabelForValue(value);
              if (label) {
                try {
                  var date = new Date(label);
                  return date.toLocaleDateString();
                } catch (e) {
                  return label;
                }
              }
              return '';
            }
          }
        },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return (-value).toFixed(1) + '%';
            }
          }
        }
      }
    }
  });
}

function renderSymbolChart(bySymbol) {
  if (!canUseCharts()) return;
  if (!bySymbol) return;

  var colors = getChartColors();
  var ctx = document.getElementById('symbol-chart');
  if (!ctx) return;

  if (analyticsState.charts.symbol) {
    analyticsState.charts.symbol.destroy();
  }

  var sortedSymbols = Object.entries(bySymbol).sort(function(a, b) {
    return b[1].total_pnl - a[1].total_pnl;
  }).slice(0, 10);

  var labels = sortedSymbols.map(function(entry) { return entry[0]; });
  var data = sortedSymbols.map(function(entry) { return entry[1].total_pnl; });
  var backgroundColors = data.map(function(val) {
    return val >= 0 ? colors.success : colors.danger;
  });

  analyticsState.charts.symbol = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Total P&L',
        data: data,
        backgroundColor: backgroundColors,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'P&L: ' + PMCommon.formatCurrency(context.parsed.y);
            }
          }
        }
      },
      scales: {
        x: { display: true, grid: { display: false } },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return '$' + value.toFixed(0);
            }
          }
        }
      }
    }
  });
}

function renderTimeframeChart(byTimeframe) {
  if (!canUseCharts()) return;
  if (!byTimeframe) return;

  var colors = getChartColors();
  var ctx = document.getElementById('timeframe-chart');
  if (!ctx) return;

  if (analyticsState.charts.timeframe) {
    analyticsState.charts.timeframe.destroy();
  }

  var labels = Object.keys(byTimeframe);
  var data = labels.map(function(tf) { return byTimeframe[tf].total_pnl; });
  var backgroundColors = data.map(function(val) {
    return val >= 0 ? colors.success : colors.danger;
  });

  analyticsState.charts.timeframe = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Total P&L',
        data: data,
        backgroundColor: backgroundColors,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'P&L: ' + PMCommon.formatCurrency(context.parsed.y);
            }
          }
        }
      },
      scales: {
        x: { display: true, grid: { display: false } },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return '$' + value.toFixed(0);
            }
          }
        }
      }
    }
  });
}

function renderMonthlyChart(monthly) {
  if (!canUseCharts()) return;
  if (!monthly) return;

  var colors = getChartColors();
  var ctx = document.getElementById('monthly-chart');
  if (!ctx) return;

  if (analyticsState.charts.monthly) {
    analyticsState.charts.monthly.destroy();
  }

  var labels = Object.keys(monthly).reverse();
  var data = labels.map(function(month) { return monthly[month].total_pnl; });
  var backgroundColors = data.map(function(val) {
    return val >= 0 ? colors.success : colors.danger;
  });

  analyticsState.charts.monthly = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Monthly P&L',
        data: data,
        backgroundColor: backgroundColors,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'P&L: ' + PMCommon.formatCurrency(context.parsed.y);
            }
          }
        }
      },
      scales: {
        x: { display: true, grid: { display: false } },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return '$' + value.toFixed(0);
            }
          }
        }
      }
    }
  });
}

function renderDailyPnLChart(dailyPnL) {
  if (!canUseCharts()) return;
  if (!dailyPnL) return;

  var colors = getChartColors();
  var ctx = document.getElementById('daily-pnl-chart');
  if (!ctx) return;

  if (analyticsState.charts.dailyPnL) {
    analyticsState.charts.dailyPnL.destroy();
  }

  var labels = dailyPnL.map(function(d) { return d.date; }).reverse();
  var data = dailyPnL.map(function(d) { return d.pnl; }).reverse();
  var backgroundColors = data.map(function(val) {
    return val >= 0 ? colors.success : colors.danger;
  });

  analyticsState.charts.dailyPnL = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Daily P&L',
        data: data,
        backgroundColor: backgroundColors,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'P&L: ' + PMCommon.formatCurrency(context.parsed.y);
            }
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { display: false },
          ticks: {
            maxTicksLimit: 10,
            callback: function(value) {
              var label = this.getLabelForValue(value);
              if (label) {
                var parts = label.split('-');
                if (parts.length === 3) {
                  return parts[1] + '/' + parts[2];
                }
              }
              return label;
            }
          }
        },
        y: {
          display: true,
          grid: { color: colors.grid },
          ticks: {
            callback: function(value) {
              return '$' + value.toFixed(0);
            }
          }
        }
      }
    }
  });
}

function renderTradeStats(metrics) {
  var container = document.getElementById('trade-stats');
  if (!container) return;

  var stats = [
    { label: 'Total Trades', value: metrics.total_trades },
    { label: 'Winning Trades', value: metrics.winning_trades },
    { label: 'Losing Trades', value: metrics.losing_trades },
    { label: 'Avg Win', value: PMCommon.formatCurrency(metrics.avg_win) },
    { label: 'Avg Loss', value: PMCommon.formatCurrency(metrics.avg_loss) },
    { label: 'Best Trade', value: PMCommon.formatCurrency(metrics.best_trade) },
    { label: 'Worst Trade', value: PMCommon.formatCurrency(metrics.worst_trade) },
    { label: 'Gross Profit', value: PMCommon.formatCurrency(metrics.gross_profit) },
    { label: 'Gross Loss', value: PMCommon.formatCurrency(metrics.gross_loss) },
    { label: 'Avg Trade P&L', value: PMCommon.formatCurrency(metrics.avg_trade_pnl) },
    { label: 'Max DD $', value: PMCommon.formatCurrency(metrics.max_drawdown_abs) },
    { label: 'DD Duration', value: metrics.drawdown_duration || 0 },
    { label: 'Recovery Time', value: metrics.recovery_time || 0 },
    { label: 'Ulcer Index', value: PMCommon.formatNumber(metrics.ulcer_index, 4) }
  ];

  container.innerHTML = stats.map(function(stat) {
    return '<div class="metric-pill"><span>' + stat.label + '</span><strong>' + stat.value + '</strong></div>';
  }).join('');
}

function renderRecentTrades(trades) {
  var tbody = document.getElementById('recent-trades');
  if (!tbody || !trades) return;

  tbody.innerHTML = '';
  var recentTrades = trades.slice(0, 10);

  recentTrades.forEach(function(trade) {
    var row = document.createElement('tr');
    var pnl = trade.pnl || 0;
    var pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
    var dirClass = trade.direction === 'LONG' ? 'dir-buy' : 'dir-sell';

    var timeStr = 'N/A';
    if (trade.timestamp) {
      try {
        var date = new Date(trade.timestamp);
        timeStr = date.toLocaleString();
      } catch (e) {}
    }

    row.innerHTML =
      '<td style="font-size: 12px;">' + timeStr + '</td>' +
      '<td>' + (trade.symbol || 'N/A') + '</td>' +
      '<td class="' + dirClass + '">' + (trade.direction || 'N/A') + '</td>' +
      '<td class="' + pnlClass + '">' + PMCommon.formatCurrency(pnl) + '</td>';

    tbody.appendChild(row);
  });
}

function renderRegimeBreakdown(byRegime) {
  var container = document.getElementById('regime-breakdown');
  if (!container || !byRegime) return;

  var regimes = Object.keys(byRegime);
  if (!regimes.length) {
    container.innerHTML = '<div class="text-muted">No regime data available.</div>';
    return;
  }

  container.innerHTML = regimes.map(function(regime) {
    var metrics = byRegime[regime];
    return '<div class="metric-pill">' +
      '<span>' + regime + '</span>' +
      '<strong>P&L: ' + PMCommon.formatCurrency(metrics.total_pnl) + '</strong>' +
      '<div style="font-size: 11px; margin-top: 4px; color: var(--muted);">' +
      'WR: ' + PMCommon.formatPercentage(metrics.win_rate) + ' | ' +
      'PF: ' + PMCommon.formatNumber(metrics.profit_factor, 2) +
      '</div>' +
      '</div>';
  }).join('');
}

function renderStrategyRanking(ranking) {
  if (!canUseCharts()) return;
  var ctx = document.getElementById('strategy-ranking-chart');
  if (!ctx || !ranking || !ranking.length) return;

  if (analyticsState.charts.strategyRanking) {
    analyticsState.charts.strategyRanking.destroy();
  }

  var colors = getChartColors();
  var labels = ranking.map(function(r) { return r.strategy; });
  var data = ranking.map(function(r) { return r.total_pnl; });
  var bgColors = data.map(function(v) { return v >= 0 ? colors.success : colors.danger; });

  analyticsState.charts.strategyRanking = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Strategy P&L',
        data: data,
        backgroundColor: bgColors,
        borderWidth: 0
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(context) {
              return 'P&L: ' + PMCommon.formatCurrency(context.parsed.x);
            }
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { color: colors.grid },
          ticks: { callback: function(v) { return '$' + v.toFixed(0); } }
        },
        y: { display: true, grid: { display: false } }
      }
    }
  });
}

function renderHeatmap(heatmap) {
  var container = document.getElementById('heatmap-grid');
  if (!container || !heatmap) return;

  var days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  var hours = [];
  for (var h = 0; h < 24; h++) hours.push(h);

  // Find min/max for color scale
  var allVals = [];
  for (var dk in heatmap) {
    for (var hk in heatmap[dk]) {
      allVals.push(heatmap[dk][hk]);
    }
  }
  var maxAbs = Math.max.apply(null, allVals.map(function(v) { return Math.abs(v); }));
  if (!maxAbs) maxAbs = 1;

  var html = '<div class="heatmap-row heatmap-header"><div class="heatmap-label"></div>';
  for (var hi = 0; hi < hours.length; hi++) {
    html += '<div class="heatmap-cell heatmap-hour-label">' + hours[hi] + '</div>';
  }
  html += '</div>';

  for (var di = 0; di < days.length; di++) {
    html += '<div class="heatmap-row"><div class="heatmap-label">' + days[di] + '</div>';
    for (var hj = 0; hj < hours.length; hj++) {
      var val = (heatmap[days[di]] && heatmap[days[di]][String(hj)]) || 0;
      var intensity = val / maxAbs;
      var bg;
      if (val > 0) {
        bg = 'rgba(15, 138, 95, ' + (0.15 + Math.abs(intensity) * 0.7) + ')';
      } else if (val < 0) {
        bg = 'rgba(184, 51, 51, ' + (0.15 + Math.abs(intensity) * 0.7) + ')';
      } else {
        bg = 'transparent';
      }
      html += '<div class="heatmap-cell" style="background:' + bg + ';" title="' + days[di] + ' ' + hj + ':00 - ' + PMCommon.formatCurrency(val) + '"></div>';
    }
    html += '</div>';
  }

  container.innerHTML = html;
}

function updateTradesCount(count) {
  var el = document.getElementById('trades-loaded-count');
  if (el) {
    el.textContent = count || 0;
  }
}

function updateAllCharts() {
  if (!canUseCharts()) return;
  if (!analyticsState.data) return;
  var data = analyticsState.data;
  renderEquityChart(data.equity_curve);
  renderDrawdownChart(data.drawdown_curve);
  renderSymbolChart(data.by_symbol);
  renderTimeframeChart(data.by_timeframe);
  renderMonthlyChart(data.monthly);
  renderDailyPnLChart(data.daily_pnl);
  if (data.strategy_ranking) renderStrategyRanking(data.strategy_ranking);
}

function exportToCSV() {
  if (!analyticsState.data || !analyticsState.data.recent_trades) {
    PMCommon.showToast('No data to export');
    return;
  }

  var trades = analyticsState.data.recent_trades;
  var headers = ['Timestamp', 'Symbol', 'Direction', 'Volume', 'Price', 'SL', 'TP', 'P&L', 'Status'];
  var rows = [headers.join(',')];

  trades.forEach(function(trade) {
    var row = [
      trade.timestamp || '',
      trade.symbol || '',
      trade.direction || '',
      trade.volume || 0,
      trade.price || 0,
      trade.sl || 0,
      trade.tp || 0,
      trade.pnl || 0,
      trade.status || ''
    ];
    rows.push(row.map(function(value) { return PMCommon.csvEscape(value); }).join(','));
  });

  var csv = rows.join('\n');
  var blob = new Blob([csv], { type: 'text/csv' });
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = 'pm_trades_export_' + new Date().toISOString().split('T')[0] + '.csv';
  a.click();
  URL.revokeObjectURL(url);
  PMCommon.showToast('CSV exported');
}

function runSimulation() {
  var capital = parseFloat(document.getElementById('sim-capital').value) || 10000;
  var startDate = document.getElementById('sim-start-date').value;
  var endDate = document.getElementById('sim-end-date').value;
  var returnBasis = document.getElementById('sim-return-basis').value || 'dollar';
  var statusEl = document.getElementById('sim-status');
  var simBtn = document.getElementById('simulate-btn');

  if (statusEl) statusEl.textContent = 'Running simulation...';
  if (simBtn) simBtn.disabled = true;

  var payload = {
    initial_capital: capital,
    start_date: startDate || null,
    end_date: endDate || null,
    return_basis: returnBasis,
    max_trades: 1000
  };

  fetch('/api/simulate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
    .then(function(response) { return response.json(); })
    .then(function(data) {
      if (simBtn) simBtn.disabled = false;

      if (!data.success) {
        if (statusEl) statusEl.textContent = 'Error: ' + (data.error || 'Unknown error');
        PMCommon.showToast('Simulation failed: ' + (data.error || 'Unknown error'));
        return;
      }

      if (statusEl) {
        statusEl.textContent = data.message + ' (' + data.total_trades + ' trades)';
      }

      // Store simulated data
      analyticsState.simulatedData = data;
      analyticsState.currentReturnBasis = returnBasis;

      // Update dashboard with simulated results
      renderKPIs(data.metrics);
      renderEquityChart(data.equity_curve);
      renderDrawdownChart(data.drawdown_curve);
      renderTradeStats(data.metrics);

      if (data.trades && data.trades.length > 0) {
        renderRecentTrades(data.trades);
      }

      PMCommon.showToast('Simulation complete: ' + data.total_trades + ' trades');
    })
    .catch(function(err) {
      if (simBtn) simBtn.disabled = false;
      if (statusEl) statusEl.textContent = 'Error: ' + err.message;
      console.error('Simulation error:', err);
      PMCommon.showToast('Simulation failed');
    });
}

function downloadHistoricalData() {
  var statusEl = document.getElementById('sim-status');
  var downloadBtn = document.getElementById('download-data-btn');

  if (statusEl) statusEl.textContent = 'Starting root M5 data refresh...';
  if (downloadBtn) downloadBtn.disabled = true;

  fetch('/api/download_historical_data', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }
  })
    .then(function(response) { return response.json(); })
    .then(function(data) {
      if (downloadBtn) downloadBtn.disabled = false;

      if (data.success) {
        if (statusEl) statusEl.textContent = data.message || 'Root data maintenance started';
        PMCommon.showToast('Root M5 data maintenance started');
      } else {
        if (statusEl) statusEl.textContent = 'Error: ' + (data.error || 'Unknown error');
        PMCommon.showToast('Data maintenance failed: ' + (data.error || 'Unknown error'));
      }
    })
    .catch(function(err) {
      if (downloadBtn) downloadBtn.disabled = false;
      if (statusEl) statusEl.textContent = 'Error: ' + err.message;
      console.error('Download error:', err);
      PMCommon.showToast('Data maintenance failed');
    });
}

function setDefaultDates() {
  var today = new Date();
  var oneMonthAgo = new Date();
  oneMonthAgo.setMonth(today.getMonth() - 1);

  var endDateEl = document.getElementById('sim-end-date');
  var startDateEl = document.getElementById('sim-start-date');

  if (endDateEl && !endDateEl.value) {
    endDateEl.value = today.toISOString().split('T')[0];
  }

  if (startDateEl && !startDateEl.value) {
    startDateEl.value = oneMonthAgo.toISOString().split('T')[0];
  }
}

function init() {
  PMCommon.initTheme();
  applyChartThemeDefaults();
  PMCommon.onThemeChange(function() {
    applyChartThemeDefaults();
    updateAllCharts();
  });
  PMCommon.initScrollToTop();
  if (!canUseCharts()) {
    showChartUnavailableState();
    PMCommon.showToast('Chart library unavailable - showing metrics/tables only');
  }
  fetchAnalytics();

  // Set default dates
  setDefaultDates();

  var refreshBtn = document.getElementById('refresh-btn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', fetchAnalytics);
  }

  var exportBtn = document.getElementById('export-btn');
  if (exportBtn) {
    exportBtn.addEventListener('click', exportToCSV);
  }

  var simulateBtn = document.getElementById('simulate-btn');
  if (simulateBtn) {
    simulateBtn.addEventListener('click', runSimulation);
  }

  var downloadDataBtn = document.getElementById('download-data-btn');
  if (downloadDataBtn) {
    downloadDataBtn.addEventListener('click', downloadHistoricalData);
  }

  // Return basis selector change
  var returnBasisSelect = document.getElementById('sim-return-basis');
  if (returnBasisSelect) {
    returnBasisSelect.addEventListener('change', function() {
      if (analyticsState.simulatedData) {
        // Re-run simulation with new basis if we have simulated data
        PMCommon.showToast('Changing return basis - please re-run simulation');
      }
    });
  }
}

window.addEventListener('load', init);
