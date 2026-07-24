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
    },

    // Moves the map's current-position marker (trace index 1, see _map_fig in
    // figures.py) to the GPS sample nearest xValue. track is the {t, lat, lon}
    // payload from gps-track-store; t entries are either ISO datetime strings
    // or plain numbers, matching whatever domain xValue itself is in (both are
    // derived the same way -- from event_time when present, else timestamp_s).
    updateMapMarker: function(gdId, track, xValue) {
        if (!track || !track.t || !track.t.length || xValue === null || xValue === undefined) return;
        var gd = document.querySelector("#" + gdId + " .js-plotly-plot");
        if (!gd || !gd.data || gd.data.length < 2) return;

        var toNum = (typeof track.t[0] === "string")
            ? function(v) { return new Date(v).getTime(); }
            : function(v) { return v; };
        var target = (typeof xValue === "string") ? new Date(xValue).getTime() : xValue;

        var tArr = track.t;
        var lo = 0, hi = tArr.length - 1;
        while (lo < hi) {
            var mid = (lo + hi) >> 1;
            if (toNum(tArr[mid]) < target) { lo = mid + 1; } else { hi = mid; }
        }
        if (lo > 0 && Math.abs(toNum(tArr[lo - 1]) - target) <= Math.abs(toNum(tArr[lo]) - target)) {
            lo = lo - 1;
        }

        Plotly.restyle(gd, { lat: [[track.lat[lo]]], lon: [[track.lon[lo]]] }, [1]);
    }
};
