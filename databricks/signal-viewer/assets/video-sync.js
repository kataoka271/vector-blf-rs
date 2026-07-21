window._videoSync = {
    // Draws/updates a single named vertical cursor line on a Plotly graph,
    // merging with (not replacing) any other shapes already on the figure --
    // e.g. Genie anomaly markers added server-side via fig.add_vline.
    setCursor: function(gdId, xValue) {
        // dcc.Graph(id=gdId) is an outer wrapper div -- the element Plotly.js
        // actually instruments (.data/.layout, target of Plotly.relayout) is
        // the nested ".js-plotly-plot" div.
        var gd = document.querySelector("#" + gdId + " .js-plotly-plot");
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
