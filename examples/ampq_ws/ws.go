package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gubsky90/spubtream/v2"
)

type Stream = spubtream.Stream[[]byte, net.Conn]

func auth(conn net.Conn) ([]string, error) {
	_ = conn.SetReadDeadline(time.Now().Add(time.Second * 2))
	msg, _, err := wsutil.ReadClientData(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, err
	}

	var authMsg struct {
		Token string `json:"token"` // sub tags here (tag1,tag2,...)
	}
	if err := json.Unmarshal(msg, &authMsg); err != nil {
		return nil, err
	}

	var claims struct {
		jwt.RegisteredClaims
		Tags []string `json:"tags"`
	}
	if _, err := jwt.ParseWithClaims(authMsg.Token, &claims, func(*jwt.Token) (any, error) {
		return []byte("test"), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()})); err != nil {
		return nil, err
	}

	// jwt.SigningMethodHS256.Sign()

	fmt.Println(claims)

	return claims.Tags, nil
}

// eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpcCI6IjE3Mi4yMi4wLjEiLCJ1c2VyX2FnZW50IjoiUG9zdG1hblJ1bnRpbWUvNy40My4wIiwidGFncyI6WyJhbGwiLCJ1c2VyI3N1cGVyIl0sImlhdCI6MTc0NTI4NTcwOH0.jKZovuhT1ORWYL0wFAPXsLgX47_G2lCBVr7s5BxnOh8
