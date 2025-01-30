function previewImage(event) {
    var reader = new FileReader();
    reader.onload = function() {
        var output = document.getElementById('imagePreview');
        output.src = reader.result;
    };
    reader.readAsDataURL(event.target.files[0]);
}

function displaySearchResults(data) {
    var resultsDiv = document.getElementById('searchResults');
    resultsDiv.innerHTML = '';

    data.image_urls.forEach(function(url, index) {
        var rank = index + 1; // 排名从1开始

        var imgContainer = document.createElement('div');
        imgContainer.className = 'image-container';

        var rankText = document.createElement('p');
        rankText.innerText = 'Rank ' + rank;
        imgContainer.appendChild(rankText);

        var img = document.createElement('img');
        img.src = url;
        img.alt = 'Image Rank ' + rank;
        imgContainer.appendChild(img);

        resultsDiv.appendChild(imgContainer);
    });
}



document.getElementById('uploadForm').addEventListener('submit', function(e) {
    e.preventDefault();

    var formData = new FormData(e.target);
    fetch('/upload', { method: 'POST', body: formData })
        .then(response => response.json())
        .then(data => {
            alert(data.message); // 显示上传成功的消息
        })
        .catch(error => console.error('Error:', error));
});

document.getElementById('searchForm').addEventListener('submit', function(e) {
    e.preventDefault();

    var formData = new FormData(e.target);

    fetch('/search', { method: 'POST', body: formData })
        .then(response => response.json())
        .then(data => {
            // 处理搜索结果
            displaySearchResults(data);
            //alert(data); // 显示上传成功的消息
        })
        .catch(error => console.error('Error:', error));
});