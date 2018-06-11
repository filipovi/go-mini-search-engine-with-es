package elastic

import (
	"context"
	"encoding/json"

	"github.com/icrowley/fake"
	"github.com/olivere/elastic"
)

// Client is the ElasticSearch Client structure
type Client struct {
	*elastic.Client
}

// User struct represents a user in ElasticSearch
type User struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	RealName string `json:"real_name"`
}

// Populate adds X user to ES
func (client Client) Populate(number int) error {
	tableName := "users"
	idxExists, err := client.IndexExists(tableName).Do(context.Background())
	if err != nil {
		return err
	}

	if !idxExists {
		client.CreateIndex(tableName).Do(context.Background())
	}

	for i := 0; i < number; i++ {
		user := User{
			Username: fake.UserName(),
			Email:    fake.EmailAddress(),
			RealName: fake.FullName(),
		}
		_, err = client.Index().
			Index(tableName).
			Type("doc").
			BodyJson(user).
			Do(context.Background())
		if err != nil {
			return err
		}
	}

	return nil
}

// NewSearchQuery returns the users matching the given term
func (client Client) NewSearchQuery(term string, from, size int) ([]*User, error) {
	q := elastic.NewMultiMatchQuery(term, "username", "email", "real_name").Fuzziness("AUTO:2,5")

	res, err := client.Search().Index("users").Query(q).From(from).Size(size).Do(context.Background())
	if err != nil {
		return nil, err
	}

	users := make([]*User, 0)

	for _, hit := range res.Hits.Hits {
		var user User
		err := json.Unmarshal(*hit.Source, &user)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}
	return users, nil
}

// New returns an Elastic Connection
func New(url string) (*Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		return nil, err
	}

	return &Client{client}, nil
}
