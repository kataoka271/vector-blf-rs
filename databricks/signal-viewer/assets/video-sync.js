window._videoSync = {
    // Playback state shared by every clientside callback in callbacks.py.
    // playlist entries are {file, src, t_min, t_max, t0}, ordered by start time
    // (built server-side by update_video_panel).
    playlist: [],
    // Index of the segment loaded in the active element; null when nothing is loaded.
    currentIndex: null,
    // Which of the two stacked <video> elements is visible and playing.
    activeId: 'video-player-a',
    // Index the *idle* element is preloading, if any.
    bufferedIndex: null,

    active: function() { return document.getElementById(this.activeId); },

    idle: function() {
        var otherId = (this.activeId === 'video-player-a') ? 'video-player-b' : 'video-player-a';
        return document.getElementById(otherId);
    },

    seek: function(el, seconds) {
        if (el === null || seconds === null || seconds === undefined) return;
        // currentTime before metadata arrives is silently dropped, so defer until
        // the duration is known (readyState >= HAVE_METADATA).
        if (el.readyState >= 1) {
            el.currentTime = seconds;
            return;
        }
        el.addEventListener('loadedmetadata', function once() {
            el.currentTime = seconds;
            el.removeEventListener('loadedmetadata', once);
        });
    },

    // Points the hidden element at segment `idx` so a later switch to it is a
    // visibility swap rather than a cold fetch. preload="auto" is only a hint to
    // the browser, but even a partial buffer takes connection setup and
    // first-byte latency off the switch.
    preload: function(idx) {
        var idle = this.idle();
        if (!idle || this.bufferedIndex === idx) return;
        var seg = this.playlist[idx];
        if (!seg) {
            idle.removeAttribute('src');
            idle.load();
            this.bufferedIndex = null;
            return;
        }
        idle.src = seg.src;
        idle.load();
        this.bufferedIndex = idx;
    },

    // Makes segment `idx` the visible element, optionally seeking to `seconds`
    // and starting playback. Swaps to the idle element when it is already
    // buffering `idx` (the seamless path, taken on end-of-file auto-advance);
    // otherwise repoints the active element in place.
    show: function(idx, seconds, play) {
        var el = this.active();
        if (!el || !this.playlist[idx]) return;
        if (idx !== this.currentIndex) {
            var next = this.idle();
            if (this.bufferedIndex === idx && next) {
                // Reassign activeId before pausing the outgoing element: the
                // 'pause' listener ignores non-active elements, which is what
                // keeps the swap from disabling video-cursor-interval mid-play.
                this.activeId = next.id;
                next.classList.remove('video-idle');
                el.classList.add('video-idle');
                el.pause();
                this.bufferedIndex = null;
                el = next;
            } else {
                el.src = this.playlist[idx].src;
            }
            this.currentIndex = idx;
        }
        this.seek(el, seconds);
        if (play) el.play().catch(function() {});
        this.preload(idx + 1);
    },

    // Clears both elements. Clearing src (not just hiding the panel) is what
    // actually stops playback and any in-flight range requests.
    reset: function() {
        ['video-player-a', 'video-player-b'].forEach(function(id) {
            var el = document.getElementById(id);
            if (!el) return;
            el.pause();
            el.removeAttribute('src');
            el.load();
        });
        this.currentIndex = null;
        this.bufferedIndex = null;
    },

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
