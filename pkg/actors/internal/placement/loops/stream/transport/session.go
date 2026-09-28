/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package transport

import (
	"errors"

	"google.golang.org/grpc"
)

type session struct {
	Transport
	conn *grpc.ClientConn
}

// NewSession takes ownership of the dedicated connection for an established
// placement stream. The caller must call Session.Close when the session ends.
// CloseSend retains its half-close behavior and does not release the connection.
func NewSession(stream Transport, conn *grpc.ClientConn) Session {
	return &session{Transport: stream, conn: conn}
}

func (s *session) Close() error {
	return errors.Join(s.CloseSend(), s.conn.Close())
}
