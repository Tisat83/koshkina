document.addEventListener("DOMContentLoaded", function () {
    const overlay = document.getElementById("lightboxOverlay");
    const bigImg = document.getElementById("lightboxImage");
    const closeBtn = document.getElementById("lightboxClose");

    if (!overlay || !bigImg) {
        return;
    }

    function openLightbox(src, alt) {
        bigImg.src = src;
        bigImg.alt = alt || "";
        overlay.classList.add("is-open");
    }

    function closeLightbox() {
        overlay.classList.remove("is-open");
        bigImg.src = "";
    }

    document.querySelectorAll(".js-lightbox").forEach(function (img) {
        img.addEventListener("click", function () {
            openLightbox(img.src, img.alt);
        });
    });

    overlay.addEventListener("click", function (e) {
        if (e.target === overlay || e.target === closeBtn) {
            closeLightbox();
        }
    });

    document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") {
            closeLightbox();
        }
    });
});
