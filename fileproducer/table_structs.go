package fileproducer

var Tables map[string]string

func init() {
	Tables = make(map[string]string)
	Tables["links"] = "Links"
	Tables["movies"] = "Movies"
}

// Links is used to link other sources of movie data
type Links struct {
	MovieID uint32 `json:"movieid"`
	ImdbID  string `json:"imdbid"`
	TmdbID  string `json:"tmdbid"`
}

// Movies keep info about different movies
type Movies struct {
	MovieID uint32 `json:"movieid"`
	Title   string `json:"title"`
	Genres  string `json:"genres"`
}
