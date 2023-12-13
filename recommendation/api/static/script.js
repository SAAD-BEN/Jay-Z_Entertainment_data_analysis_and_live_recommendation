function displayMovies(movies) {
    const movieList = document.getElementById('movieList');
    movieList.innerHTML = '';

    movies.forEach(movie => {
        const card = document.createElement('div');
        card.classList.add('movie-card');

        if (movie.rating_avg >= 4.0) {
            card.classList.add('highlight');
        }

        card.innerHTML = `
            <h3>${movie.title}</h3>
            <p>Genres: ${movie.genres.join(', ')}</p>
            <p>Rating: ${movie.rating_avg.toFixed(2)}</p>
            <p>Rating Count: ${movie.rating_count}</p>
        `;

        movieList.appendChild(card);
    });
}
function getRecommendations() {
    const titleInput = document.getElementById('movieTitle');
    const button = document.getElementById('getRecommendationBtn');
    const loadingSpinner = document.getElementById('loadingSpinner');

    // Disable input and button during the recommendation process
    titleInput.disabled = true;
    button.disabled = true;
    loadingSpinner.style.display = 'block';

    const title = titleInput.value;

    fetch(`/recommendation/movie?title=${title}`)
        .then(response => response.json())
        .then(data => {
            // Enable input and button after recommendations are received
            titleInput.disabled = false;
            button.disabled = false;
            loadingSpinner.style.display = 'none';

            displayMovies(data);
        })
        .catch(error => {
            console.error('Error fetching recommendations:', error);
            // Handle error and re-enable input and button
            titleInput.disabled = false;
            button.disabled = false;
            loadingSpinner.style.display = 'none';
        });
}
