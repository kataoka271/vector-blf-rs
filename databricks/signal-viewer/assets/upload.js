// Drag-and-drop / click-to-browse uploader for the /upload screen (see
// layout.py's _upload_dropzone). Each file is fetch()'d straight to
// /upload-proxy/<kind> (app/upload.py) as its own request body rather than through
// a Dash callback, so a large video file streams from the browser instead of being
// base64-encoded into a single callback payload.
(function () {
    function dropzoneFor(el) {
        return el.closest(".upload-dropzone");
    }

    // dash.html has no Input component (see layout.py's _upload_dropzone), so the
    // native file input isn't server-rendered -- created here on first click instead;
    // drag-and-drop pulls files straight from the DataTransfer and never needs one.
    function ensureFileInput(zone) {
        var input = zone.querySelector('input[type="file"]');
        if (input) return input;
        input = document.createElement("input");
        input.type = "file";
        input.multiple = true;
        input.accept = zone.dataset.accept || "";
        input.style.display = "none";
        zone.appendChild(input);
        return input;
    }

    function addRow(list, filename) {
        var row = document.createElement("div");
        row.className = "upload-row uploading";
        var name = document.createElement("span");
        name.className = "upload-row-name";
        name.textContent = filename;
        var status = document.createElement("span");
        status.className = "upload-row-status";
        status.textContent = "Uploading...";
        row.appendChild(name);
        row.appendChild(status);
        list.prepend(row);
        return row;
    }

    function uploadOne(kind, file, list) {
        var row = addRow(list, file.name);
        fetch("/upload-proxy/" + kind, {
            method: "POST",
            headers: { "Content-Type": "application/octet-stream", "X-Filename": file.name },
            body: file,
        })
            .then(function (resp) {
                if (!resp.ok) {
                    return resp.text().then(function (text) {
                        throw new Error(text || resp.statusText);
                    });
                }
                row.className = "upload-row done";
                row.querySelector(".upload-row-status").textContent = "Done";
            })
            .catch(function (err) {
                row.className = "upload-row error";
                row.querySelector(".upload-row-status").textContent = "Failed: " + err.message;
            });
    }

    function handleFiles(zone, files) {
        var list = document.getElementById(zone.dataset.kind + "-upload-list");
        if (!list) return;
        Array.prototype.forEach.call(files, function (file) {
            uploadOne(zone.dataset.kind, file, list);
        });
    }

    document.addEventListener("click", function (e) {
        var zone = dropzoneFor(e.target);
        if (!zone || e.target.tagName === "INPUT") return;
        ensureFileInput(zone).click();
    });

    document.addEventListener("change", function (e) {
        if (e.target.tagName !== "INPUT" || e.target.type !== "file") return;
        var zone = dropzoneFor(e.target);
        if (!zone) return;
        handleFiles(zone, e.target.files);
        e.target.value = "";
    });

    document.addEventListener("dragover", function (e) {
        var zone = dropzoneFor(e.target);
        if (!zone) return;
        e.preventDefault();
        zone.classList.add("dragover");
    });

    document.addEventListener("dragleave", function (e) {
        var zone = dropzoneFor(e.target);
        if (zone) zone.classList.remove("dragover");
    });

    document.addEventListener("drop", function (e) {
        var zone = dropzoneFor(e.target);
        if (!zone) return;
        e.preventDefault();
        zone.classList.remove("dragover");
        handleFiles(zone, e.dataTransfer.files);
    });
})();
