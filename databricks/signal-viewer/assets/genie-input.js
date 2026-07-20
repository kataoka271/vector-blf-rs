document.addEventListener('keydown', function(e) {
    if (e.target && e.target.id === 'genie-input' && e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        var btn = document.getElementById('genie-ask-btn');
        if (btn && !btn.disabled) {
            btn.click();
        }
    }
});
