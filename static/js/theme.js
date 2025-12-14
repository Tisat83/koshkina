document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;
    const toggle = document.getElementById("themeToggle");
    const icon = document.getElementById("themeToggleIcon");

    function applyTheme(theme) {
        if (theme === "dark") {
            body.classList.add("dark-theme");
        } else {
            body.classList.remove("dark-theme");
        }

        if (icon) {
            icon.textContent = theme === "dark" ? "🌙" : "☀️";
        }

        try {
            window.localStorage.setItem("koshkina-theme", theme);
        } catch (e) {
            // если localStorage недоступен — просто игнорируем
        }
    }

    let savedTheme = null;
    try {
        savedTheme = window.localStorage.getItem("koshkina-theme");
    } catch (e) {
        savedTheme = null;
    }

    if (savedTheme === "dark" || savedTheme === "light") {
        applyTheme(savedTheme);
    } else {
        applyTheme("light");
    }

    if (toggle) {
        toggle.addEventListener("click", function () {
            const isDark = body.classList.contains("dark-theme");
            applyTheme(isDark ? "light" : "dark");
        });
    }
});
