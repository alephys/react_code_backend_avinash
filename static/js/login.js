// static/js/login.js
document.addEventListener("DOMContentLoaded", function() {
    document.getElementById("loginForm").addEventListener("submit", async function(event) {
        event.preventDefault();
        let username = document.getElementById("username").value;
        let password = document.getElementById("password").value;
        let messageEl = document.getElementById("message");
        let csrfToken = document.querySelector("[name=csrfmiddlewaretoken]");
        if (!csrfToken) {
            messageEl.style.color = "red";
            messageEl.innerText = "CSRF token not found. Please refresh the page.";
            console.error("CSRF token input not found in the form.");
            return;
        }
        messageEl.innerText = "Logging in...";
        messageEl.style.color = "blue";
        try {
            const response = await fetch("/login/", {
                method: "POST",
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-CSRFToken": csrfToken.value
                },
                body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`
            });
            const data = await response.json();
            if (data.success) {
                messageEl.style.color = "green";
                messageEl.innerText = data.message;
                window.location.href = data.redirect || "/home/";
            } else {
                messageEl.style.color = "red";
                messageEl.innerText = data.message;
            }
        } catch (error) {
            messageEl.style.color = "red";
            messageEl.innerText = "Unexpected error. Please try again.";
            console.error("Fetch error:", error);
        }
    });
});