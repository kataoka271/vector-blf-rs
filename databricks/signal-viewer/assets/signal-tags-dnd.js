// Drag-to-reorder for the selected-signal tags in #signal-tags (plot order
// follows this list -- see _extract_traces in figures.py). Tags are
// re-rendered by the render_signal_tags Python callback on every
// signal-select value change, so listeners are delegated at the document
// level rather than bound per-tag.
(function () {
    var dragKey = null;

    function tagEl(target) {
        return target.closest(".signal-tag");
    }

    function clearDropMarkers() {
        document.querySelectorAll(".signal-tag-drop-before, .signal-tag-drop-after").forEach(function (el) {
            el.classList.remove("signal-tag-drop-before", "signal-tag-drop-after");
        });
    }

    document.addEventListener("dragstart", function (e) {
        var tag = tagEl(e.target);
        if (!tag) return;
        dragKey = tag.dataset.key;
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain", dragKey);
        tag.classList.add("signal-tag-dragging");
    });

    document.addEventListener("dragend", function (e) {
        var tag = tagEl(e.target);
        if (tag) tag.classList.remove("signal-tag-dragging");
        clearDropMarkers();
        dragKey = null;
    });

    document.addEventListener("dragover", function (e) {
        var tag = tagEl(e.target);
        if (!tag || !dragKey || tag.dataset.key === dragKey) return;
        e.preventDefault();
        var rect = tag.getBoundingClientRect();
        var before = e.clientX - rect.left < rect.width / 2;
        tag.classList.toggle("signal-tag-drop-before", before);
        tag.classList.toggle("signal-tag-drop-after", !before);
    });

    document.addEventListener("dragleave", function (e) {
        var tag = tagEl(e.target);
        if (tag) tag.classList.remove("signal-tag-drop-before", "signal-tag-drop-after");
    });

    document.addEventListener("drop", function (e) {
        var container = document.getElementById("signal-tags");
        var tag = tagEl(e.target);
        if (!container || !tag || !dragKey || tag.dataset.key === dragKey) return;
        e.preventDefault();
        var before = tag.classList.contains("signal-tag-drop-before");
        clearDropMarkers();

        var visible = Array.prototype.slice.call(container.querySelectorAll(".signal-tag"));
        var order = visible.map(function (el) { return el.dataset.key; }).filter(function (k) { return k !== dragKey; });
        var targetIdx = order.indexOf(tag.dataset.key);
        if (targetIdx === -1) return;
        order.splice(before ? targetIdx : targetIdx + 1, 0, dragKey);

        // Tags beyond _MAX_VISIBLE_TAGS aren't rendered (see render_signal_tags
        // in callbacks.py), so splice the reordered visible prefix back onto
        // the untouched tail of the full selection.
        var full = window._signalSelectValue || order;
        var tail = full.slice(visible.length);
        window.dash_clientside.set_props("signal-select", { value: order.concat(tail) });
    });
})();
