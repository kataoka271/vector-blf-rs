// Drag-to-reposition and minimize/expand for the floating video panel
// (#video-section). Pure client-side DOM manipulation -- video-section,
// video-float-header, and video-float-minimize-btn are static, always-mounted
// elements, so document-level delegated listeners registered once at script
// load are sufficient (no re-binding needed across Dash re-renders).
(function () {
    var dragState = null;

    function clamp(v, min, max) {
        return Math.max(min, Math.min(max, v));
    }

    document.addEventListener("pointerdown", function (e) {
        if (e.target.closest("#video-float-minimize-btn")) return;
        var header = e.target.closest("#video-float-header");
        if (!header) return;
        var panel = document.getElementById("video-section");
        if (!panel) return;
        var rect = panel.getBoundingClientRect();
        // Freeze the current on-screen position as explicit left/top,
        // canceling the CSS class's default top/right anchor so left/top
        // drive it from here on.
        panel.style.left = rect.left + "px";
        panel.style.top = rect.top + "px";
        panel.style.right = "auto";
        dragState = { panel: panel, startX: e.clientX, startY: e.clientY, origLeft: rect.left, origTop: rect.top };
        header.setPointerCapture(e.pointerId);
    });

    document.addEventListener("pointermove", function (e) {
        if (!dragState) return;
        var panel = dragState.panel;
        var rect = panel.getBoundingClientRect();
        var newLeft = clamp(dragState.origLeft + (e.clientX - dragState.startX), 0, window.innerWidth - rect.width);
        var newTop = clamp(dragState.origTop + (e.clientY - dragState.startY), 0, window.innerHeight - rect.height);
        panel.style.left = newLeft + "px";
        panel.style.top = newTop + "px";
    });

    document.addEventListener("pointerup", function () {
        dragState = null;
    });
    document.addEventListener("pointercancel", function () {
        dragState = null;
    });

    document.addEventListener("click", function (e) {
        var btn = e.target.closest("#video-float-minimize-btn");
        if (!btn) return;
        var panel = document.getElementById("video-section");
        if (!panel) return;
        var minimized = panel.classList.toggle("video-float-minimized");
        btn.textContent = minimized ? "+" : "-";
    });
})();
