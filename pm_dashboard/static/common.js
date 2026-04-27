/* PMCommon - shared dashboard utilities */
var PMCommon = (function () {
  "use strict";

  var _isDark = false;
  var _themeChangeCallbacks = [];
  var _toastContainer = null;
  var WRITE_TOKEN_STORAGE_KEY = "pm-dashboard-write-token";

  function initTheme() {
    var savedTheme = null;
    try {
      savedTheme = localStorage.getItem("pm-theme");
    } catch (err) {
      savedTheme = null;
    }

    if (savedTheme === "dark" || savedTheme === "light") {
      _isDark = savedTheme === "dark";
    } else {
      _isDark = true;
    }

    _applyTheme();

    var toggle = document.getElementById("theme-toggle");
    if (!toggle) return;

    toggle.addEventListener("click", toggleTheme);
    toggle.setAttribute("role", "button");
    toggle.setAttribute("tabindex", "0");
    toggle.addEventListener("keydown", function (event) {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        toggleTheme();
      }
    });
  }

  function toggleTheme() {
    _isDark = !_isDark;
    try {
      localStorage.setItem("pm-theme", _isDark ? "dark" : "light");
    } catch (err) {
      // Ignore storage failures (private browsing, strict policies).
    }
    _applyTheme();
    for (var i = 0; i < _themeChangeCallbacks.length; i += 1) {
      _themeChangeCallbacks[i](_isDark);
    }
  }

  function _applyTheme() {
    document.documentElement.setAttribute("data-theme", _isDark ? "dark" : "light");

    var toggle = document.getElementById("theme-toggle");
    if (toggle) {
      toggle.setAttribute("aria-pressed", _isDark ? "true" : "false");
      toggle.setAttribute("title", _isDark ? "Switch to light mode" : "Switch to dark mode");
    }

    var icon = document.querySelector(".theme-toggle-icon");
    if (icon) {
      icon.textContent = _isDark ? "LIGHT" : "DARK";
    }

    var label = document.querySelector(".theme-toggle-label");
    if (label) {
      label.textContent = _isDark ? "Light mode" : "Dark mode";
    }
  }

  function isDark() {
    return _isDark;
  }

  function onThemeChange(callback) {
    if (typeof callback === "function") {
      _themeChangeCallbacks.push(callback);
    }
  }

  function formatNumber(value, decimals) {
    if (decimals === undefined) decimals = 5;
    if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
    var num = Number(value);
    if (!Number.isFinite(num)) return "N/A";
    return num.toFixed(decimals);
  }

  function formatCurrency(value, decimals) {
    if (decimals === undefined) decimals = 2;
    if (value === null || value === undefined || Number.isNaN(value)) return "$0.00";
    var num = Number(value);
    if (!Number.isFinite(num)) return "$0.00";
    return "$" + num.toFixed(decimals);
  }

  function formatPercentage(value, decimals) {
    if (decimals === undefined) decimals = 2;
    if (value === null || value === undefined || Number.isNaN(value)) return "0.00%";
    var num = Number(value);
    if (!Number.isFinite(num)) return "0.00%";
    return num.toFixed(decimals) + "%";
  }

  function formatDateTime(value) {
    if (!value) return "N/A";
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  }

  function formatRelativeTime(value) {
    if (!value) return "N/A";
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return "N/A";

    var diffMs = Date.now() - date.getTime();
    var diffSec = Math.floor(Math.abs(diffMs) / 1000);

    var unit = "second";
    var amount = diffSec;
    if (diffSec >= 60) {
      unit = "minute";
      amount = Math.floor(diffSec / 60);
    }
    if (diffSec >= 3600) {
      unit = "hour";
      amount = Math.floor(diffSec / 3600);
    }
    if (diffSec >= 86400) {
      unit = "day";
      amount = Math.floor(diffSec / 86400);
    }

    var suffix = diffMs >= 0 ? "ago" : "from now";
    return amount + " " + unit + (amount === 1 ? "" : "s") + " " + suffix;
  }

  function escapeHtml(value) {
    var text = String(value === null || value === undefined ? "" : value);
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function uniqueValues(items, key) {
    var values = {};
    for (var i = 0; i < items.length; i += 1) {
      var value = items[i] && items[i][key];
      if (value !== null && value !== undefined && String(value).trim() !== "") {
        values[String(value)] = true;
      }
    }
    return Object.keys(values).sort(function (a, b) {
      return a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });
    });
  }

  function updatePagination(state, elements) {
    var total = (state.items || []).length;
    state.totalPages = Math.max(1, Math.ceil(total / state.pageSize));
    if (state.currentPage > state.totalPages) state.currentPage = state.totalPages;

    if (elements.pageInfo) {
      elements.pageInfo.textContent = "Page " + state.currentPage + " of " + state.totalPages;
    }
    if (elements.rowCount) {
      elements.rowCount.textContent = total + " rows";
    }
    if (elements.prevBtn) {
      elements.prevBtn.disabled = state.currentPage <= 1;
    }
    if (elements.nextBtn) {
      elements.nextBtn.disabled = state.currentPage >= state.totalPages;
    }
  }

  function copyToClipboard(text, label) {
    var value = text === null || text === undefined ? "" : String(text);
    if (!value) return;

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value)
        .then(function () {
          showToast((label || "Value") + " copied");
        })
        .catch(function () {
          _fallbackCopy(value);
          showToast((label || "Value") + " copied");
        });
      return;
    }

    _fallbackCopy(value);
    showToast((label || "Value") + " copied");
  }

  function _fallbackCopy(text) {
    var input = document.createElement("textarea");
    input.value = text;
    input.setAttribute("readonly", "readonly");
    input.style.position = "fixed";
    input.style.left = "-9999px";
    document.body.appendChild(input);
    input.select();
    document.execCommand("copy");
    document.body.removeChild(input);
  }

  function showToast(message, durationMs) {
    if (!durationMs) durationMs = 2200;

    if (!_toastContainer) {
      _toastContainer = document.createElement("div");
      _toastContainer.className = "pm-toast-container";
      document.body.appendChild(_toastContainer);
    }

    var toast = document.createElement("div");
    toast.className = "pm-toast";
    toast.textContent = String(message || "Done");
    _toastContainer.appendChild(toast);

    // Trigger transition.
    toast.offsetHeight;
    toast.classList.add("pm-toast-show");

    setTimeout(function () {
      toast.classList.remove("pm-toast-show");
      toast.classList.add("pm-toast-hide");
      setTimeout(function () {
        if (toast.parentNode) {
          toast.parentNode.removeChild(toast);
        }
      }, 320);
    }, durationMs);
  }

  function openDrawer(drawerEl) {
    if (!drawerEl) return;
    drawerEl.classList.add("open");
    document.body.classList.add("drawer-open");
  }

  function closeDrawer(drawerEl) {
    if (!drawerEl) return;
    drawerEl.classList.remove("open");
    document.body.classList.remove("drawer-open");
  }

  function initEscapeClose(drawerEl, closeHandler) {
    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") return;
      if (!drawerEl || !drawerEl.classList.contains("open")) return;
      if (typeof closeHandler === "function") {
        closeHandler();
      } else {
        closeDrawer(drawerEl);
      }
    });
  }

  function debounce(fn, delay) {
    var timer = null;
    return function () {
      var context = this;
      var args = arguments;
      if (timer) clearTimeout(timer);
      timer = setTimeout(function () {
        fn.apply(context, args);
      }, delay);
    };
  }

  function fetchWithRetry(url, options, retries, timeoutMs) {
    if (retries === undefined) retries = 3;
    if (timeoutMs === undefined) timeoutMs = 10000;

    function attempt(remainingRetries) {
      return new Promise(function (resolve, reject) {
        var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
        var requestOptions = Object.assign({}, options || {});
        if (controller) {
          requestOptions.signal = controller.signal;
        }

        var timeout = setTimeout(function () {
          if (controller) controller.abort();
        }, timeoutMs);

        fetch(url, requestOptions)
          .then(function (response) {
            clearTimeout(timeout);
            if (!response.ok) {
              throw new Error("HTTP " + response.status);
            }
            resolve(response);
          })
          .catch(function (error) {
            clearTimeout(timeout);
            if (remainingRetries > 1) {
              var attemptNumber = retries - remainingRetries + 1;
              var delay = Math.pow(2, attemptNumber) * 250;
              setTimeout(function () {
                attempt(remainingRetries - 1).then(resolve).catch(reject);
              }, delay);
            } else {
              reject(error);
            }
          });
      });
    }

    return attempt(retries);
  }

  function _getWriteToken() {
    try {
      return sessionStorage.getItem(WRITE_TOKEN_STORAGE_KEY) || "";
    } catch (err) {
      return "";
    }
  }

  function _setWriteToken(token) {
    try {
      if (token) {
        sessionStorage.setItem(WRITE_TOKEN_STORAGE_KEY, token);
      } else {
        sessionStorage.removeItem(WRITE_TOKEN_STORAGE_KEY);
      }
    } catch (err) {
      // Ignore storage failures.
    }
  }

  function _withWriteToken(options, token) {
    var requestOptions = Object.assign({}, options || {});
    var headers = Object.assign({}, requestOptions.headers || {});
    if (token) headers["X-PM-Dashboard-Token"] = token;
    requestOptions.headers = headers;
    return requestOptions;
  }

  function fetchWrite(url, options) {
    return fetch(url, _withWriteToken(options, _getWriteToken()))
      .then(function (response) {
        if (response.status !== 401 || typeof window.prompt !== "function") {
          return response;
        }
        var token = window.prompt("Dashboard write token");
        if (!token) return response;
        _setWriteToken(token);
        return fetch(url, _withWriteToken(options, token));
      });
  }

  function setLoadingState(element, isLoading, message) {
    if (!element) return;
    if (message !== undefined && message !== null) {
      element.textContent = String(message);
    }
    if (isLoading) {
      element.classList.remove("hidden");
      element.setAttribute("aria-busy", "true");
      return;
    }
    element.classList.add("hidden");
    element.setAttribute("aria-busy", "false");
  }

  function initScrollToTop() {
    var button = document.getElementById("scroll-top");
    if (!button) return;

    window.addEventListener("scroll", function () {
      if (window.scrollY > 280) {
        button.classList.add("visible");
      } else {
        button.classList.remove("visible");
      }
    });

    button.addEventListener("click", function () {
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  }

  function saveJSON(key, value) {
    if (!key) return;
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (err) {
      // Ignore storage failures.
    }
  }

  function loadJSON(key, fallback) {
    if (!key) return fallback;
    try {
      var raw = localStorage.getItem(key);
      if (!raw) return fallback;
      return JSON.parse(raw);
    } catch (err) {
      return fallback;
    }
  }

  function csvEscape(value) {
    var text = value === null || value === undefined ? "" : String(value);
    if (text.indexOf(",") !== -1 || text.indexOf("\n") !== -1 || text.indexOf("\"") !== -1) {
      return '"' + text.replace(/\"/g, '""') + '"';
    }
    return text;
  }

  return {
    initTheme: initTheme,
    toggleTheme: toggleTheme,
    isDark: isDark,
    onThemeChange: onThemeChange,
    formatNumber: formatNumber,
    formatCurrency: formatCurrency,
    formatPercentage: formatPercentage,
    formatDateTime: formatDateTime,
    formatRelativeTime: formatRelativeTime,
    escapeHtml: escapeHtml,
    uniqueValues: uniqueValues,
    updatePagination: updatePagination,
    copyToClipboard: copyToClipboard,
    showToast: showToast,
    openDrawer: openDrawer,
    closeDrawer: closeDrawer,
    initEscapeClose: initEscapeClose,
    debounce: debounce,
    fetchWithRetry: fetchWithRetry,
    fetchWrite: fetchWrite,
    setLoadingState: setLoadingState,
    initScrollToTop: initScrollToTop,
    saveJSON: saveJSON,
    loadJSON: loadJSON,
    csvEscape: csvEscape
  };
})();
