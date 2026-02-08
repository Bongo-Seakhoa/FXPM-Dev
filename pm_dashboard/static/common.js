/* PMCommon — shared dashboard utilities */
var PMCommon = (function () {
  'use strict';

  // ── Theme ──────────────────────────────────────────────────────────
  var _isDark = false;
  var _themeChangeCallbacks = [];

  function initTheme() {
    var saved = localStorage.getItem('pm-theme') || 'light';
    _isDark = saved === 'dark';
    _applyTheme();
    var toggle = document.getElementById('theme-toggle');
    if (toggle) toggle.addEventListener('click', toggleTheme);
  }

  function toggleTheme() {
    _isDark = !_isDark;
    localStorage.setItem('pm-theme', _isDark ? 'dark' : 'light');
    _applyTheme();
    for (var i = 0; i < _themeChangeCallbacks.length; i++) {
      _themeChangeCallbacks[i](_isDark);
    }
  }

  function _applyTheme() {
    document.documentElement.setAttribute('data-theme', _isDark ? 'dark' : 'light');
    var icon = document.querySelector('.theme-toggle-icon');
    if (icon) icon.textContent = _isDark ? '\u2600\uFE0F' : '\uD83C\uDF19';
  }

  function isDark() { return _isDark; }

  function onThemeChange(cb) { _themeChangeCallbacks.push(cb); }

  // ── Formatting ─────────────────────────────────────────────────────
  function formatNumber(value, decimals) {
    if (decimals === undefined) decimals = 5;
    if (value === null || value === undefined || Number.isNaN(value)) return 'N/A';
    var num = Number(value);
    if (!Number.isFinite(num)) return 'N/A';
    if (Math.abs(num) >= 1000 && decimals > 2) return num.toFixed(2);
    return num.toFixed(decimals);
  }

  function formatCurrency(value) {
    if (value === null || value === undefined || Number.isNaN(value)) return '$0.00';
    var num = Number(value);
    if (!Number.isFinite(num)) return '$0.00';
    return '$' + num.toFixed(2);
  }

  function formatPercentage(value) {
    if (value === null || value === undefined || Number.isNaN(value)) return '0.00%';
    var num = Number(value);
    if (!Number.isFinite(num)) return '0.00%';
    return num.toFixed(2) + '%';
  }

  // ── Data helpers ───────────────────────────────────────────────────
  function uniqueValues(items, key) {
    var values = {};
    for (var i = 0; i < items.length; i++) {
      var val = items[i][key];
      if (val) values[val] = true;
    }
    return Object.keys(values).sort();
  }

  // ── Pagination ─────────────────────────────────────────────────────
  function updatePagination(state, els) {
    var total = (state.items || []).length;
    state.totalPages = Math.max(1, Math.ceil(total / state.pageSize));
    if (state.currentPage > state.totalPages) state.currentPage = state.totalPages;
    if (els.pageInfo) {
      els.pageInfo.textContent = 'Page ' + state.currentPage + ' of ' + state.totalPages;
    }
    if (els.rowCount) {
      els.rowCount.textContent = total + ' rows';
    }
    if (els.prevBtn) {
      els.prevBtn.disabled = state.currentPage <= 1;
    }
    if (els.nextBtn) {
      els.nextBtn.disabled = state.currentPage >= state.totalPages;
    }
  }

  // ── Clipboard + Toast ──────────────────────────────────────────────
  function copyToClipboard(text, label) {
    if (!text) return;
    var doCopy = function () { _fallbackCopy(text); };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text)
        .then(function () { showToast(label ? label + ' copied' : 'Copied'); })
        .catch(function () { doCopy(); showToast(label ? label + ' copied' : 'Copied'); });
    } else {
      doCopy();
      showToast(label ? label + ' copied' : 'Copied');
    }
  }

  function _fallbackCopy(text) {
    var temp = document.createElement('textarea');
    temp.value = text;
    temp.style.position = 'fixed';
    temp.style.opacity = '0';
    document.body.appendChild(temp);
    temp.select();
    document.execCommand('copy');
    document.body.removeChild(temp);
  }

  var _toastContainer = null;

  function showToast(message, durationMs) {
    if (!durationMs) durationMs = 2200;
    if (!_toastContainer) {
      _toastContainer = document.createElement('div');
      _toastContainer.className = 'pm-toast-container';
      document.body.appendChild(_toastContainer);
    }
    var el = document.createElement('div');
    el.className = 'pm-toast';
    el.textContent = message;
    _toastContainer.appendChild(el);
    // trigger reflow then add visible class for animation
    el.offsetHeight; // eslint-disable-line no-unused-expressions
    el.classList.add('pm-toast-show');
    setTimeout(function () {
      el.classList.remove('pm-toast-show');
      el.classList.add('pm-toast-hide');
      setTimeout(function () {
        if (el.parentNode) el.parentNode.removeChild(el);
      }, 350);
    }, durationMs);
  }

  // ── Drawer ─────────────────────────────────────────────────────────
  function closeDrawer(drawerEl) {
    if (drawerEl) drawerEl.classList.remove('open');
  }

  function openDrawer(drawerEl) {
    if (drawerEl) drawerEl.classList.add('open');
  }

  function initEscapeClose(drawerEl) {
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && drawerEl && drawerEl.classList.contains('open')) {
        closeDrawer(drawerEl);
      }
    });
  }

  // ── Debounce ───────────────────────────────────────────────────────
  function debounce(fn, delay) {
    var timer = null;
    return function () {
      var ctx = this;
      var args = arguments;
      if (timer) clearTimeout(timer);
      timer = setTimeout(function () { fn.apply(ctx, args); }, delay);
    };
  }

  // ── Fetch with retry ───────────────────────────────────────────────
  function fetchWithRetry(url, options, retries) {
    if (retries === undefined) retries = 3;
    var attempt = 0;
    var timeoutMs = 10000;

    function doFetch() {
      attempt++;
      return new Promise(function (resolve, reject) {
        var controller = null;
        var timer = null;
        if (typeof AbortController !== 'undefined') {
          controller = new AbortController();
          var opts = Object.assign({}, options || {}, { signal: controller.signal });
        }
        timer = setTimeout(function () {
          if (controller) controller.abort();
        }, timeoutMs);

        fetch(url, controller ? opts : (options || {}))
          .then(function (response) {
            clearTimeout(timer);
            if (!response.ok) throw new Error('HTTP ' + response.status);
            resolve(response);
          })
          .catch(function (err) {
            clearTimeout(timer);
            if (attempt < retries) {
              var backoff = Math.pow(2, attempt) * 300;
              setTimeout(function () { doFetch().then(resolve).catch(reject); }, backoff);
            } else {
              reject(err);
            }
          });
      });
    }
    return doFetch();
  }

  // ── Scroll to top ─────────────────────────────────────────────────
  function initScrollToTop() {
    var scrollBtn = document.getElementById('scroll-top');
    if (!scrollBtn) return;
    window.addEventListener('scroll', function () {
      if (window.scrollY > 300) {
        scrollBtn.classList.add('visible');
      } else {
        scrollBtn.classList.remove('visible');
      }
    });
    scrollBtn.addEventListener('click', function () {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }

  // ── Public API ─────────────────────────────────────────────────────
  return {
    initTheme: initTheme,
    toggleTheme: toggleTheme,
    isDark: isDark,
    onThemeChange: onThemeChange,
    formatNumber: formatNumber,
    formatCurrency: formatCurrency,
    formatPercentage: formatPercentage,
    uniqueValues: uniqueValues,
    updatePagination: updatePagination,
    copyToClipboard: copyToClipboard,
    showToast: showToast,
    closeDrawer: closeDrawer,
    openDrawer: openDrawer,
    initEscapeClose: initEscapeClose,
    debounce: debounce,
    fetchWithRetry: fetchWithRetry,
    initScrollToTop: initScrollToTop
  };
})();
