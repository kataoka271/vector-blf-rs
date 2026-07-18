window._videoSync = {
    // Draws/updates a single named vertical cursor line on a Plotly graph,
    // merging with (not replacing) any other shapes already on the figure --
    // e.g. Genie anomaly markers added server-side via fig.add_vline.
    setCursor: function(gdId, xValue) {
        var gd = document.getElementById(gdId);
        if (!gd || !gd.layout) return;
        var shapes = (gd.layout.shapes || []).filter(function(s) { return s.name !== '_video_cursor'; });
        if (xValue !== null && xValue !== undefined) {
            shapes.push({
                name: '_video_cursor',
                type: 'line',
                xref: 'x', yref: 'paper',
                x0: xValue, x1: xValue, y0: 0, y1: 1,
                line: { color: '#3498db', width: 2 },
            });
        }
        Plotly.relayout(gd, { shapes: shapes });
    }
};
