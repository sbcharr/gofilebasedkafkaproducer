package fileproducer

import "time"

// Tables contains the mapping between actual table
// and its corresponding struct name
var Tables map[string]string

func init() {
	Tables = make(map[string]string)
	Tables["links"] = "Links"
	Tables["movies"] = "Movies"
	Tables["ratings"] = "Ratings"
	Tables["tags"] = "Tags"
}

// Links is used to link other sources of movie data
type Links struct {
	MovieID     uint32    `json:"movieid"`
	ImdbID      string    `json:"imdbid"`
	TmdbID      string    `json:"tmdbid"`
	CurrentTime time.Time `json:"load_datetime_utc"`
}

// Movies keep info about different movies
type Movies struct {
	MovieID     uint32    `json:"movieid"`
	Title       string    `json:"title"`
	Genres      string    `json:"genres"`
	CurrentTime time.Time `json:"load_datetime_utc"`
}

// Ratings keep info about different movie ratings
type Ratings struct {
	UserID      uint32    `json:"userid"`
	MovieID     uint32    `json:"movieid"`
	Rating      float32   `json:"rating"`
	RecordTime  time.Time `json:"timestamp_utc"`
	CurrentTime time.Time `json:"load_datetime_utc"`
}

// Tags keep info about different tags
type Tags struct {
	UserID      uint32    `json:"userid"`
	MovieID     uint32    `json:"movieid"`
	Tag         string    `json:"tag"`
	RecordTime  time.Time `json:"timestamp_utc"`
	CurrentTime time.Time `json:"load_datetime_utc"`
}
