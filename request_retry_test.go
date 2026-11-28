// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
)

func TestMessageRetryWaitSharesCallerDeadline(t *testing.T) {
	client := NewClient(&store.Device{}, nil)
	client.MessageRetryTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.retryFrame(ctx, "message send", "stable", nil, xmlStreamEndNode, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("retry must respect cancellation without an open socket: %v", err)
	}
	client.MessageRetryTimeout = 10 * time.Millisecond
	if _, err := client.retryFrame(context.Background(), "message send", "stable", nil, xmlStreamEndNode, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry must stop if reconnection never succeeds: %v", err)
	}
}

func TestMessageRetryReusesFrameAndWaitsForOriginalID(t *testing.T) {
	client := NewClient(&store.Device{}, nil)
	const id = "same-message-id"
	frame := []byte("already-serialized-fixture")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sends := 0
	ack := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": id}}
	got, err := client.sendRetriedFrame(ctx, "message send", id, frame, func(_ context.Context, replay []byte) error {
		sends++
		if !bytes.Equal(frame, replay) {
			t.Fatal("retry changed the serialized message")
		}
		other := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": "other-message"}}
		if client.receiveResponse(ctx, other) || !client.receiveResponse(ctx, ack) {
			t.Fatal("retry waiter is not bound to the original ID")
		}
		return nil
	}, 0)
	if err != nil || sends != 1 || got != ack {
		t.Fatalf("retry sends=%d, ack=%v, err=%v", sends, got, err)
	}
}

func TestRetryCancellationAllowsLateAckDispatcher(t *testing.T) {
	client := NewClient(&store.Device{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var taken chan<- *waBinary.Node
	const id = "late-ack"
	_, err := client.sendRetriedFrame(ctx, "message send", id, nil, func(context.Context, []byte) error {
		client.responseWaitersLock.Lock()
		taken = client.responseWaiters[id]
		delete(client.responseWaiters, id)
		client.responseWaitersLock.Unlock()
		cancel()
		return nil
	}, 0)
	if !errors.Is(err, context.Canceled) || taken == nil {
		t.Fatalf("retry wait should be canceled: %v", err)
	}
	// receiveResponse already took the mailbox, but was preempted before delivery.
	taken <- &waBinary.Node{Tag: "ack"}
}

func TestMessageRetryStopsOnAuthDisconnect(t *testing.T) {
	client := NewClient(&store.Device{}, nil)
	client.MessageRetryTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	auth := &waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": "401"}}
	_, err := client.retryFrame(ctx, "message send", "stable", nil, auth, 0)
	var disconnected *DisconnectedError
	if !errors.As(err, &disconnected) || disconnected.Node != auth {
		t.Fatalf("auth failure should be returned before a retry: %v", err)
	}
}
