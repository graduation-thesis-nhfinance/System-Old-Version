"""
Airflow UI Theme Plugin with Live Light / Dark Mode Toggle.
Injects customizable dark mode styling matching modern Airflow 2.10 / 3.0 theme
and mounts the permanent Airflow 3.0 Left Vertical Navigation Sidebar.
"""

import os
import logging
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request

logger = logging.getLogger(__name__)

STATIC_FOLDER = os.path.join(os.path.dirname(__file__), "theme_plugin", "static")

theme_bp = Blueprint(
    "theme_plugin",
    __name__,
    static_folder=STATIC_FOLDER,
    static_url_path="/theme_plugin/static"
)

SIDEBAR_HTML = """
<!-- AIRFLOW 3.0 LEFT VERTICAL NAVIGATION SIDEBAR -->
<nav id="airflow-v3-sidebar" aria-label="Airflow Navigation">
  <a class="sidebar-logo" href="/home" title="Apache Airflow Control Center">
    <svg width="30" height="30" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M0.887268 35.3904L18.1511 18.0062C18.2597 17.8967 18.28 17.7288 18.1917 17.6055C17.1629 16.1694 15.2656 15.9206 14.5625 14.9558C12.479 12.098 11.9503 10.4805 11.0552 10.5806C10.9927 10.5876 10.9369 10.6214 10.8929 10.6664L4.78358 16.929C1.26881 20.5318 0.764791 28.4641 0.672439 35.1063C0.668261 35.4064 1.03241 35.556 1.24203 35.3411L0.887268 35.3904Z" fill="#017cee"/>
      <path d="M35.4734 34.9588L18.1375 18.047C18.0289 17.941 17.861 17.9207 17.7377 18.0091C16.3017 19.0378 16.0528 20.9351 15.088 21.6384C12.2302 23.7217 10.6126 24.2504 10.7129 25.1456C10.72 25.2082 10.7536 25.2639 10.7987 25.3077L17.0613 31.4172C20.664 34.9319 28.5964 35.4359 35.2385 35.5282C35.5386 35.5326 35.6882 35.1684 35.4734 34.9588Z" fill="#00ad46"/>
      <path fill-rule="evenodd" clip-rule="evenodd" d="M17.0612 31.4173C15.0932 29.4975 14.1801 25.6994 17.953 17.8671C11.8213 20.6074 9.67257 24.2094 10.7296 25.2407L17.0612 31.4173Z" fill="#04d659"/>
      <path d="M35.0445 0.346896L18.1327 17.6827C18.0268 17.7913 18.0065 17.9592 18.0948 18.0825C19.1236 19.5186 21.0209 19.7674 21.724 20.7322C23.8075 23.59 24.3362 25.2075 25.2313 25.1074C25.2938 25.1004 25.3496 25.0666 25.3936 25.0216L31.5029 18.759C35.0177 15.1562 35.5217 7.22392 35.6141 0.58175C35.6182 0.281597 35.2541 0.132024 35.0445 0.346896Z" fill="#00c7d4"/>
      <path fill-rule="evenodd" clip-rule="evenodd" d="M31.5031 18.759C29.5832 20.7269 25.7851 21.6401 17.9528 17.8671C20.693 23.9988 24.2951 26.1477 25.3263 25.0905L31.5031 18.759Z" fill="#11e1ee"/>
      <path d="M0.432658 0.775339L17.7685 17.6871C17.8771 17.793 18.045 17.8134 18.1683 17.725C19.6043 16.6963 19.8532 14.799 20.8179 14.0957C23.6759 12.0123 25.2934 11.4837 25.193 10.5885C25.186 10.526 25.1523 10.4702 25.1074 10.4263L18.8447 4.31685C15.242 0.802203 7.30967 0.298184 0.667512 0.205751C0.367359 0.201573 0.217786 0.565722 0.432658 0.775339Z" fill="#e43921"/>
      <path fill-rule="evenodd" clip-rule="evenodd" d="M18.8446 4.31675C20.8125 6.23662 21.7257 10.0346 17.9528 17.8669C24.0844 15.1267 26.2333 11.5246 25.1761 10.4934L18.8446 4.31675Z" fill="#ff7557"/>
      <path fill-rule="evenodd" clip-rule="evenodd" d="M4.4028 16.9752C6.32267 15.0072 10.1207 14.0942 17.953 17.867C15.2128 11.7354 11.6107 9.58661 10.5795 10.6437L4.4028 16.9752Z" fill="#0cb6ff"/>
      <path d="M17.9649 18.6209C18.3825 18.6157 18.7169 18.273 18.7117 17.8553C18.7065 17.4377 18.3638 17.1034 17.9462 17.1085C17.5285 17.1137 17.1942 17.4564 17.1994 17.8741C17.2045 18.2917 17.5473 18.626 17.9649 18.6209Z" fill="#4a4848"/>
    </svg>
  </a>
  <div class="sidebar-menu">
    <a class="sidebar-item" href="/cluster_activity" id="v3-nav-home" title="Cluster Overview Dashboard">
      <svg viewBox="0 0 24 24"><path d="M3 13h1v7c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2v-7h1a1 1 0 0 0 .7-1.7L12.7 2.3a1 1 0 0 0-1.4 0L2.3 11.3A1 1 0 0 0 3 13z"/></svg>
      <span class="label">Home</span>
    </a>
    <a class="sidebar-item" href="/home" id="v3-nav-dags" title="DAGs (Pipelines Catalog)">
      <svg viewBox="0 0 24 24"><path d="M4 6H2v14c0 1.1.9 2 2 2h14v-2H4V6zm16-4H8c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-1 9h-4v4h-2v-4H9V9h4V5h2v4h4v2z"/></svg>
      <span class="label">DAGs</span>
    </a>
    <a class="sidebar-item" href="/datasets" id="v3-nav-assets" title="Assets (Data-Aware Scheduling)">
      <svg viewBox="0 0 24 24"><path d="M21 16.5l-9 5.2-9-5.2V7.5l9-5.2 9 5.2v9zM12 4.3L5 8.4l7 4 7-4-7-4.1zm-7 5.8v6.2l6 3.5v-6.2l-6-3.5zm8 9.7l6-3.5v-6.2l-6 3.5v6.2z"/></svg>
      <span class="label">Assets</span>
    </a>
    <a class="sidebar-item" href="/taskinstance/list/" id="v3-nav-browse" title="Browse (Runs, Tasks, Logs)">
      <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/><path d="M14.5 9.5L9.5 14.5l1.5-4 3.5-1z" stroke="currentColor" stroke-width="1.5"/></svg>
      <span class="label">Browse</span>
    </a>
    <a class="sidebar-item" href="/connection/list/" id="v3-nav-admin" title="Admin (Connections, Variables, Pools)">
      <svg viewBox="0 0 24 24"><path d="M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58a.49.49 0 0 0 .12-.61l-1.92-3.32a.488.488 0 0 0-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54a.484.484 0 0 0-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58a.49.49 0 0 0-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z"/></svg>
      <span class="label">Admin</span>
    </a>
  </div>
  <div class="sidebar-footer">
    <a class="sidebar-item" href="https://airflow.apache.org/docs/" target="_blank" rel="noopener noreferrer" title="Documentation & APIs">
      <svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>
      <span class="label">Docs</span>
    </a>
  </div>
</nav>
"""

@theme_bp.record_once
def on_blueprint_registered(state):
    app = state.app

    @app.after_request
    def inject_theme_assets(response):
        if response.content_type and "text/html" in response.content_type:
            try:
                content = response.get_data(as_text=True)

                # Early Head Injection with cache-busting version ?v=4.0
                head_snippet = """
                <!-- AIRFLOW ULTRA-DEEP THEME EARLY BOOTSTRAP -->
                <script>
                (function() {
                  try {
                    var pref = localStorage.getItem('airflow_theme_preference') || 'dark';
                    if (pref === 'dark') {
                      document.documentElement.classList.add('airflow-dark-theme');
                      document.documentElement.setAttribute('data-color-scheme', 'dark');
                      document.documentElement.setAttribute('data-theme', 'dark');
                      localStorage.setItem('darkTheme', 'true');
                      localStorage.setItem('chakra-ui-color-mode', 'dark');
                    }
                  } catch (e) {}
                })();
                </script>
                <link rel="stylesheet" href="/theme_plugin/static/dark_theme.css?v=4.0">
                """

                # Body Injection (Sidebar HTML + Script)
                body_snippet = f"""
                {SIDEBAR_HTML}
                <script src="/theme_plugin/static/theme_toggle.js?v=4.0"></script>
                """

                if "</head>" in content:
                    content = content.replace("</head>", f"{head_snippet}\n</head>")

                # Ensure <body> has with-v3-sidebar class
                if "<body" in content:
                    content = content.replace("<body", '<body class="with-v3-sidebar"', 1)

                if "</body>" in content:
                    content = content.replace("</body>", f"{body_snippet}\n</body>")

                response.set_data(content)
            except Exception as e:
                logger.debug(f"Theme asset injection error: {e}")
        return response

class AirflowThemePlugin(AirflowPlugin):
    name = "theme_toggle_plugin"
    flask_blueprints = [theme_bp]
