// NHFinance Theme Injector for MinIO Console
(function () {
  function applyNHFinanceBranding() {
    // 1. Ghi đè Title trang
    if (!document.title.includes("NHFinance")) {
      document.title = "NHFinance Object Store | " + document.title.replace("MinIO Console", "").trim();
    }

    // 2. Thay thế logo MinIO bằng Logo & Tên NHFinance
    // MinIO đặt logo ở góc trên bên trái của Sidebar
    const sidebars = document.querySelectorAll('nav, aside, div[class*="sidebar"], div[class*="Sidebar"], div[class*="makeStyles-sidebar"]');
    sidebars.forEach((sidebar) => {
      // Tìm container chứa logo MinIO (chứa chữ OBJECT STORE hoặc ảnh SVG minio)
      const svgs = sidebar.querySelectorAll('svg, img');
      svgs.forEach((el) => {
        const parent = el.closest('div');
        if (parent && (parent.innerText.includes("OBJECT STORE") || parent.innerText.includes("MINIO") || parent.innerText.includes("Community"))) {
          if (!parent.getAttribute("data-nhfinance-branded")) {
            parent.setAttribute("data-nhfinance-branded", "true");
            parent.innerHTML = `
              <div style="display: flex; align-items: center; gap: 10px; padding: 12px 14px; background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); border-radius: 8px; border: 1px solid #334155; margin-bottom: 12px;">
                <div style="width: 32px; height: 32px; border-radius: 6px; background: #0ea5e9; display: flex; align-items: center; justify-content: center; font-weight: 900; color: #ffffff; font-family: sans-serif; font-size: 16px; box-shadow: 0 2px 6px rgba(14,165,233,0.4);">
                  NH
                </div>
                <div style="display: flex; flex-direction: column;">
                  <span style="font-size: 14px; font-weight: 800; color: #f8fafc; letter-spacing: 0.5px; font-family: sans-serif;">NHFINANCE</span>
                  <span style="font-size: 9px; font-weight: 700; color: #38bdf8; letter-spacing: 1px; font-family: monospace;">LAKEHOUSE S3</span>
                </div>
              </div>
            `;
          }
        }
      });
    });

    // 3. Chèn banner header "NHFinance Enterprise" nếu có topbar
    const header = document.querySelector('header');
    if (header && !header.querySelector('#nhfinance-banner-chip')) {
      const chip = document.createElement('div');
      chip.id = 'nhfinance-banner-chip';
      chip.style.cssText = 'margin-left: 20px; font-family: monospace; font-size: 11px; font-weight: 700; color: #38bdf8; background: rgba(14,165,233,0.15); border: 1px solid rgba(14,165,233,0.35); padding: 3px 12px; border-radius: 9999px; display: inline-flex; align-items: center; gap: 6px;';
      chip.innerHTML = '<span>🔒</span> <span>NHFINANCE ENTERPRISE DATA LAKE</span>';
      header.appendChild(chip);
    }
  }

  // Chạy ngay khi load và lắng nghe DOM thay đổi (vì React re-render liên tục)
  document.addEventListener("DOMContentLoaded", applyNHFinanceBranding);
  window.addEventListener("load", applyNHFinanceBranding);

  const observer = new MutationObserver(function () {
    applyNHFinanceBranding();
  });

  observer.observe(document.documentElement, {
    childList: true,
    subtree: true,
  });
})();
