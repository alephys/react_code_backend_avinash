// static/js/submit.js
document.getElementById("submitForm").addEventListener("submit", async function(event) {
    event.preventDefault();
    let command = document.getElementById("command").value;
    let messageEl = document.getElementById("message");
    messageEl.innerText = "Submitting...";
    messageEl.style.color = "blue";
    try {
        const response = await fetch("/submit/", {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-CSRFToken": document.querySelector("[name=csrfmiddlewaretoken]").value
            },
            body: `command=${encodeURIComponent(command)}`
        });
        const data = await response.json();
        if (data.approved) {
            messageEl.style.color = "green";
            messageEl.innerText = data.message;
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