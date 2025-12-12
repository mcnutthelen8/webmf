document.addEventListener('DOMContentLoaded', () => {
    fetch('/data')
        .then(response => response.json())
        .then(data => {
            const dataList = document.getElementById('data-list');
            data.forEach(item => {
                const listItem = document.createElement('li');
                listItem.textContent = JSON.stringify(item);
                dataList.appendChild(listItem);
            });
        })
        .catch(error => console.error('Error fetching data:', error));
});
