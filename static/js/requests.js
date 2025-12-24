// static/js/requests.js
async function handleRequest(requestId, action) {
    const url = action === 'approve' ? `/approve_request/${requestId}/` : `/decline_request/${requestId}/`;
    const messageEl = document.createElement('div');
    messageEl.className = action === 'approve' ? 'success' : 'error';
    document.querySelector('.main-content').prepend(messageEl);
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value
            }
        });
        const data = await response.json();
        if (data.success) {
            messageEl.innerText = data.message;
            setTimeout(() => location.reload(), 1000); // Refresh to update request list
        } else {
            messageEl.innerText = data.message;
        }
    } catch (error) {
        messageEl.innerText = 'Unexpected error. Please try again.';
        console.error('Fetch error:', error);
    }
}