document.getElementById('upload-form').addEventListener('submit', function(e) {
    e.preventDefault();
    var formData = new FormData(this);

    fetch('/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        document.getElementById('results').innerText = data.message;
    })
    .catch(error => console.error('Error:', error));
});

// 处理查询请求
document.getElementById('search-form').addEventListener('submit', function(e) {
    e.preventDefault();
    var search = document.querySelector('[name="search"]').value;

    fetch('/search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ search: search })
    })
    .then(response => response.json())
    .then(data => {
        document.getElementById('search-results').innerText = JSON.stringify(data, null, 2);
    })
    .catch(error => console.error('Error:', error));
});
