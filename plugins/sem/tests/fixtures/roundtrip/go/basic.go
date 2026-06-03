package fixtures

import "fmt"

type User struct {
	ID   string
	Name string
}

func Greeting(user User) string {
	return fmt.Sprintf("Hello, %s!", user.Name)
}

func IsEmpty(user User) bool {
	return user.ID == "" && user.Name == ""
}
