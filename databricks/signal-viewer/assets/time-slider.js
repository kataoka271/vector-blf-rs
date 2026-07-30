window.dccFunctions = window.dccFunctions || {};
window.dccFunctions._fmtSliderTime = function(value) {
    var store = window._timeRangeStore;
    if (!store) return value.toFixed(3);
    var tMin = store.tMin;
    // store.t0 is a tz-naive string representing a UTC instant (see db.py) -- native
    // `new Date()` would otherwise parse it as browser-local time, not UTC.
    var t0Ms = store.t0 ? new Date(/Z$|[+-]\d\d:?\d\d$/.test(store.t0) ? store.t0 : store.t0 + 'Z').getTime() : null;
    if (t0Ms !== null) {
        var dt = new Date(t0Ms + (value - tMin) * 1000);
        var hms = [dt.getUTCHours(), dt.getUTCMinutes(), dt.getUTCSeconds()]
            .map(function(v) { return v.toString().padStart(2, '0'); }).join(':');
        return hms + '.' + dt.getUTCMilliseconds().toString().padStart(3, '0');
    }
    var total = Math.abs(value - tMin);
    var s = Math.floor(total);
    var ms = Math.round((total - s) * 1000);
    var hms = [Math.floor(s / 3600), Math.floor((s % 3600) / 60), s % 60]
        .map(function(v) { return v.toString().padStart(2, '0'); }).join(':');
    return hms + '.' + ms.toString().padStart(3, '0');
};
