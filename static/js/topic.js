// static/js/topic.js
async function managePartition(topicName, action) {
    const url = action === 'create' ? `/create_partition/${topicName}/` : `/delete_partition/${topicName}/`;
    const messageEl = document.createElement('div');
    messageEl.className = action === 'create' ? 'success' : 'error';
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
            messageEl.innerText = `Partition ${action}d successfully`;
            setTimeout(() => location.reload(), 15000); // Refresh to update partitions
        } else {
            messageEl.innerText = data.message;
        }
    } catch (error) {
        messageEl.innerText = 'Unexpected error. Please try again.';
        console.error('Fetch error:', error);
    }
}