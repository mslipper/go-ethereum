package pkg

import "crypto/rand"

func Rand32() ([]byte) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}