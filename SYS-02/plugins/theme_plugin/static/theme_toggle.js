/**
 * Universal Airflow Theme Engine (Production Dual-Mode)
 * Controls Dark / Light mode across 100% of Airflow interface using native theme button.
 * Controls Airflow 3.0 Left Sidebar navigation state.
 * Handles Shadow DOM penetration for Cluster Activity (Home) cleanly in both Light and Dark modes.
 * Ensures perfect high-contrast typography, balanced card heights, and fixes Pools Slots clipping.
 */

(function () {
  "use strict";

  const STORAGE_KEY = "airflow_theme_preference";
  const DARK_CLASS = "airflow-dark-theme";

  function getSavedTheme() {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved === "light" || saved === "dark") return saved;
      const nativeDark = localStorage.getItem("darkTheme");
      if (nativeDark === "false") return "light";
      if (nativeDark === "true") return "dark";
    } catch (e) {}
    return "dark"; // Default to dark mode
  }

  // Inject structural layout fixes for Cluster Activity (applies in both Light and Dark modes)
  function syncClusterActivityLayout(shadow) {
    if (!shadow) return;
    let layoutStyle = shadow.getElementById("airflow-shadow-layout-fix");
    if (!layoutStyle) {
      layoutStyle = document.createElement("style");
      layoutStyle.id = "airflow-shadow-layout-fix";
      shadow.appendChild(layoutStyle);
      layoutStyle.textContent = `
        /* Align card heights across the top 3 columns */
        div[style*="justify-content: space-between"][style*="flex-wrap: wrap"],
        div[style*="justifyContent: space-between"][style*="flexWrap: wrap"] {
          align-items: stretch !important;
        }

        /* Left Column: Stack Unpaused DAGs and Longest Runs evenly to match Pools Slots height */
        div[style*="width: 25%"][style*="flex-direction: column"],
        div[style*="width: 25%"][style*="flexDirection: column"] {
          justify-content: space-between !important;
          gap: 12px !important;
        }

        div[style*="width: 25%"][style*="flex-direction: column"] > div,
        div[style*="width: 25%"][style*="flexDirection: column"] > div {
          flex: 1 1 0% !important;
          display: flex !important;
          flex-direction: column !important;
          justify-content: center !important;
          margin-bottom: 0 !important;
        }

        /* Pools Slots ECharts canvas padding fix - ensure default_pool is fully visible */
        div[data-testid="echart-container"] {
          padding-left: 14px !important;
        }
      `;
    }
  }

  // Inject or remove Dark Theme inside Cluster Activity Shadow DOM
  function syncClusterActivityShadowTheme(isDark) {
    const root = document.querySelector("#root");
    if (!root || !root.shadowRoot) return;
    const shadow = root.shadowRoot;

    // Apply structural layout fixes in all modes
    syncClusterActivityLayout(shadow);

    let shadowStyle = shadow.getElementById("airflow-shadow-dark-theme");

    if (isDark) {
      if (!shadowStyle) {
        shadowStyle = document.createElement("style");
        shadowStyle.id = "airflow-shadow-dark-theme";
        shadow.appendChild(shadowStyle);
      }
      shadowStyle.textContent = `
        /* Top 3 Metric Cards (Unpaused DAGs, Pools Slots, Health) */
        div[style*="background: white"],
        div[style*="background-color: white"],
        div[style*="background: rgb(255, 255, 255)"],
        div[style*="background-color: rgb(255, 255, 255)"],
        div[class*="css-"][style*="white"] {
          background-color: #0f172a !important;
          background: #0f172a !important;
          border: 1px solid #1e293b !important;
          border-radius: 8px !important;
          box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.4) !important;
        }

        /* Historical Metrics main wrapper & sub-bar */
        div[style*="blackAlpha"],
        div[style*="rgba(0, 0, 0, 0.08)"] {
          background-color: #0b0f19 !important;
          background: #0b0f19 !important;
          border: 1px solid #1e293b !important;
          border-radius: 8px !important;
        }

        /* Unified Segmented Control: Date + Time Input Boxes */
        input[type="date"],
        input[type="date"].chakra-input {
          background-color: #131c31 !important;
          background: #131c31 !important;
          color: #f1f5f9 !important;
          border: 1px solid #334155 !important;
          border-right: 1px solid #1e293b !important;
          border-top-left-radius: 8px !important;
          border-bottom-left-radius: 8px !important;
          border-top-right-radius: 0px !important;
          border-bottom-right-radius: 0px !important;
          height: 40px !important;
          line-height: 40px !important;
          padding: 0 12px !important;
          font-size: 13.5px !important;
          font-weight: 500 !important;
          outline: none !important;
          box-shadow: none !important;
          transition: border-color 0.2s, box-shadow 0.2s !important;
        }

        input[type="time"],
        input[type="time"].chakra-input {
          background-color: #131c31 !important;
          background: #131c31 !important;
          color: #f1f5f9 !important;
          border: 1px solid #334155 !important;
          border-left: none !important;
          border-top-left-radius: 0px !important;
          border-bottom-left-radius: 0px !important;
          border-top-right-radius: 8px !important;
          border-bottom-right-radius: 8px !important;
          height: 40px !important;
          line-height: 40px !important;
          padding: 0 12px !important;
          font-size: 13.5px !important;
          font-weight: 500 !important;
          outline: none !important;
          box-shadow: none !important;
          transition: border-color 0.2s, box-shadow 0.2s !important;
        }

        input[type="date"]:focus,
        input[type="date"].chakra-input:focus,
        input[type="time"]:focus,
        input[type="time"].chakra-input:focus {
          border-color: #38bdf8 !important;
          box-shadow: 0 0 0 1px #38bdf8 !important;
          z-index: 2 !important;
        }

        /* Generic fallback inputs if any */
        input:not([type="date"]):not([type="time"]) {
          background-color: #131c31 !important;
          background: #131c31 !important;
          color: #f1f5f9 !important;
          border: 1px solid #334155 !important;
          border-radius: 8px !important;
          height: 40px !important;
          padding: 0 12px !important;
        }

        /* Clear Filters Button */
        button[aria-label="Reset filters"],
        button.chakra-button {
          background-color: #1e293b !important;
          background: #1e293b !important;
          color: #38bdf8 !important;
          border: 1px solid #334155 !important;
          border-radius: 8px !important;
          height: 40px !important;
          padding: 0 16px !important;
          font-size: 13px !important;
          font-weight: 600 !important;
          cursor: pointer !important;
          transition: all 0.2s ease !important;
          display: inline-flex !important;
          align-items: center !important;
          justify-content: center !important;
        }

        button[aria-label="Reset filters"]:hover,
        button.chakra-button:hover {
          background-color: #334155 !important;
          background: #334155 !important;
          color: #7dd3fc !important;
          border-color: #38bdf8 !important;
          box-shadow: 0 0 12px rgba(56, 189, 248, 0.2) !important;
        }

        /* Donut chart axes and legends */
        svg text {
          fill: #94a3b8 !important;
        }
      `;
      shadowStyle.disabled = false;

      // 1. Fix Start Date & End Date floating labels
      shadow.querySelectorAll('b').forEach(el => {
        const t = el.textContent.trim();
        if (t === 'Start Date' || t === 'End Date') {
          el.style.color = '#38bdf8';
          el.style.backgroundColor = '#0b0f19';
          el.style.padding = '1px 6px';
          el.style.borderRadius = '4px';
          el.style.border = '1px solid #334155';
          el.style.fontSize = '11.5px';
          el.style.fontWeight = '700';
          el.style.letterSpacing = '0.03em';
          el.style.zIndex = '10';
        } else if (['MetaDatabase', 'Scheduler', 'Triggerer', 'Dag Processor'].includes(t)) {
          el.style.color = '#38bdf8';
          el.style.fontSize = '14.5px';
          el.style.fontWeight = '600';
        }
      });

      // 2. Fix Health card labels & duration text (NON-DESTRUCTIVE - style only, never mutate innerHTML)
      shadow.querySelectorAll('p, span').forEach(el => {
        const t = el.textContent.trim();
        if (t === 'status:' || t === 'last heartbeat:') {
          el.style.color = '#94a3b8';
          el.style.fontWeight = '500';
        } else if (t.startsWith('for a period of')) {
          el.style.color = '#38bdf8';
          el.style.fontWeight = '600';
          el.style.fontSize = '13px';
        }
      });

      // 3. Fix Health status badges (vivid high contrast, non-glare, dark slate pill)
      shadow.querySelectorAll('.chakra-badge').forEach(b => {
        const txt = b.textContent.toLowerCase();
        b.style.borderRadius = '4px';
        b.style.fontWeight = '600';
        b.style.padding = '2px 8px';
        if (txt.includes('healthy') || txt.includes('2026') || txt.includes('+07') || txt.includes(':')) {
          b.style.backgroundColor = 'rgba(34, 197, 94, 0.18)';
          b.style.color = '#4ade80';
          b.style.border = '1px solid rgba(34, 197, 94, 0.4)';
        } else if (txt.includes('unknown') || txt.includes('failed') || txt.includes('error')) {
          b.style.backgroundColor = 'rgba(239, 68, 68, 0.18)';
          b.style.color = '#f87171';
          b.style.border = '1px solid rgba(239, 68, 68, 0.4)';
        }
      });
    } else {
      // In Light Mode: Clean up all custom overrides completely!
      if (shadowStyle) {
        shadowStyle.remove();
      }
      shadow.querySelectorAll('b, p, span, .chakra-badge').forEach(el => {
        el.style.color = '';
        el.style.backgroundColor = '';
        el.style.border = '';
        el.style.padding = '';
        el.style.borderRadius = '';
        el.style.fontSize = '';
        el.style.fontWeight = '';
        el.style.letterSpacing = '';
        el.style.zIndex = '';
      });
    }
  }

  // Restore or clean SVG circles when in Light mode
  function syncSvgCirclesDualMode(isDark) {
    const svgCircles = document.querySelectorAll('svg[id*="dag-run-"] circle, svg[id*="task-instance-"] circle, svg circle.has-svg-tooltip');
    if (!isDark) {
      // In Light Mode: restore white background
      svgCircles.forEach(c => {
        if (c.getAttribute("fill") === "#0b0f19") {
          c.setAttribute("fill", "#fff");
        }
        if (c.getAttribute("stroke") === "#1e293b") {
          c.setAttribute("stroke", "gainsboro");
        }
      });
    }
  }

  function applyGlobalTheme(theme) {
    const isDark = (theme === "dark");

    // 1. Sync HTML and Body classes and dataset attributes
    if (isDark) {
      document.documentElement.classList.add(DARK_CLASS);
      document.documentElement.setAttribute("data-color-scheme", "dark");
      document.documentElement.setAttribute("data-theme", "dark");
      if (document.body) {
        document.body.classList.add(DARK_CLASS);
      }
    } else {
      document.documentElement.classList.remove(DARK_CLASS);
      document.documentElement.setAttribute("data-color-scheme", "light");
      document.documentElement.setAttribute("data-theme", "light");
      if (document.body) {
        document.body.classList.remove(DARK_CLASS);
      }
    }

    // 2. Sync LocalStorage across all Airflow subsystems
    try {
      localStorage.setItem(STORAGE_KEY, theme);
      localStorage.setItem("darkTheme", isDark ? "true" : "false");
      localStorage.setItem("chakra-ui-color-mode", theme);
    } catch (e) {}

    // 3. Update Airflow native Sun / Moon icons
    const darkIcon = document.getElementById("dark_icon");
    const lightIcon = document.getElementById("light_icon");
    if (darkIcon && lightIcon) {
      darkIcon.style.display = isDark ? "block" : "none";
      lightIcon.style.display = isDark ? "none" : "block";
    }

    // 4. Ensure body has with-v3-sidebar class
    if (document.body && !document.body.classList.contains("with-v3-sidebar")) {
      document.body.classList.add("with-v3-sidebar");
    }

    // 5. Dual-mode sync for Shadow DOM and SVG elements
    syncClusterActivityShadowTheme(isDark);
    syncSvgCirclesDualMode(isDark);
  }

  function toggleMode() {
    const cur = getSavedTheme();
    const next = (cur === "dark") ? "light" : "dark";
    applyGlobalTheme(next);
  }

  // Pre-apply immediately
  applyGlobalTheme(getSavedTheme());

  // Wire up Airflow's native Theme Toggle button (#themeToggleButton)
  function wireNativeToggleButton() {
    const nativeBtn = document.getElementById("themeToggleButton");
    if (nativeBtn && !nativeBtn.hasAttribute("data-wired")) {
      nativeBtn.setAttribute("data-wired", "true");
      nativeBtn.style.display = "block";
      nativeBtn.style.cursor = "pointer";
      nativeBtn.addEventListener("click", function (e) {
        e.preventDefault();
        toggleMode();
      });
    }

    const oldCustomBtn = document.getElementById("theme-toggle-universal-container");
    if (oldCustomBtn) {
      oldCustomBtn.remove();
    }
  }

  // Update Left Sidebar active item based on current URL path
  function syncSidebarActiveState() {
    const sidebar = document.getElementById("airflow-v3-sidebar");
    if (!sidebar) return;

    if (document.body && !document.body.classList.contains("with-v3-sidebar")) {
      document.body.classList.add("with-v3-sidebar");
    }

    const path = window.location.pathname;
    const isHome = path.includes("/cluster_activity");
    const isDags = !isHome && (path.includes("/dags") || path === "/" || path.includes("/home") || path.includes("/grid") || path.includes("/graph"));
    const isAssets = path.includes("/datasets");
    const isBrowse = path.includes("/taskinstance") || path.includes("/dagrun") || path.includes("/audit_log") || path.includes("/job");
    const isAdmin = path.includes("/connection") || path.includes("/variable") || path.includes("/pool") || path.includes("/plugin") || path.includes("/configuration");

    const items = sidebar.querySelectorAll(".sidebar-item");
    items.forEach(el => el.classList.remove("active"));

    if (isHome) {
      const el = document.getElementById("v3-nav-home");
      if (el) el.classList.add("active");
    } else if (isAssets) {
      const el = document.getElementById("v3-nav-assets");
      if (el) el.classList.add("active");
    } else if (isBrowse) {
      const el = document.getElementById("v3-nav-browse");
      if (el) el.classList.add("active");
    } else if (isAdmin) {
      const el = document.getElementById("v3-nav-admin");
      if (el) el.classList.add("active");
    } else if (isDags) {
      const el = document.getElementById("v3-nav-dags");
      if (el) el.classList.add("active");
    }
  }

  function init() {
    const theme = getSavedTheme();
    applyGlobalTheme(theme);
    wireNativeToggleButton();
    syncSidebarActiveState();

    // Observe SPA routing changes & dynamic content (including Shadow DOM mount)
    const observer = new MutationObserver(() => {
      wireNativeToggleButton();
      syncSidebarActiveState();
      const currentTheme = getSavedTheme();
      const isDark = (currentTheme === "dark") || document.documentElement.classList.contains(DARK_CLASS);
      syncClusterActivityShadowTheme(isDark);
      syncSvgCirclesDualMode(isDark);
    });

    if (document.body) {
      observer.observe(document.body, { childList: true, subtree: true });
    }

    // Shadow DOM poll for first 3 seconds to catch late React mounts
    let checks = 0;
    const interval = setInterval(() => {
      checks++;
      const currentTheme = getSavedTheme();
      const isDark = (currentTheme === "dark") || document.documentElement.classList.contains(DARK_CLASS);
      syncClusterActivityShadowTheme(isDark);
      syncSvgCirclesDualMode(isDark);
      if (checks > 15) clearInterval(interval);
    }, 200);

    window.addEventListener("popstate", () => {
      const t = getSavedTheme();
      applyGlobalTheme(t);
      syncSidebarActiveState();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
