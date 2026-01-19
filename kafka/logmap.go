package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	defaultOffset    = 1
	offsetInc        = 1
	defaultKVTimeout = 1
	defaultKVRetries = 10
)

var (
	// ErrKeyNotFound is returned when a key does not exist in the KV store.
	ErrKeyNotFound = errors.New("key does not exist")
	// ErrMaxRetriesExceeded is returned when an operation fails after max retries.
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

type LogMsg struct {
	offset int
	msg    int
}

// keyData holds per-key data with its own lock for atomic read-modify-write operations.
type keyData struct {
	mu           sync.RWMutex
	msgs         []LogMsg
	commitOffset int
}

type LocalLog struct {
	data  sync.Map // map[string]*keyData
	LinKV *maelstrom.KV
}

func errIsPreconditionFailed(err error) bool {
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code == 21
	}
	return false
}

// callWithRetry executes function f up to retries times, retrying only on DeadlineExceeded errors.
func callWithRetry[T any](retries int, f func() (T, error)) (T, error) {
	var lastErr error
	var zero T

	for range retries {
		result, err := f()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if !errors.Is(err, context.DeadlineExceeded) {
			return zero, err
		}
	}

	return zero, lastErr
}

// getOrCreateKeyData returns the keyData for key k, creating it if it doesn't exist.
func (lm *LocalLog) getOrCreateKeyData(k string) *keyData {
	if val, ok := lm.data.Load(k); ok {
		return val.(*keyData)
	}

	newData := &keyData{
		msgs:         make([]LogMsg, 0, 16),
		commitOffset: 0,
	}

	// LoadOrStore returns existing value if present, or stores and returns new value
	actual, _ := lm.data.LoadOrStore(k, newData)
	return actual.(*keyData)
}

func (lm *LocalLog) getLocalOffset(k string) (int, bool) {
	val, ok := lm.data.Load(k)
	if !ok {
		return 0, false
	}

	kd := val.(*keyData)
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	if kd.commitOffset == 0 {
		return 0, false
	}
	return kd.commitOffset, true
}

// getLocationOfOffset returns the index of the first message with offset >= the requested offset.
// Returns the index and true if such a message exists, or 0 and false if all messages have lower offsets.
func getLocationOfOffset(offset int, msgs []LogMsg) (int, bool) {
	idx, _ := slices.BinarySearchFunc(msgs, offset,
		func(m LogMsg, o int) int { return cmp.Compare(m.offset, o) })
	if idx >= len(msgs) {
		return 0, false
	}
	return idx, true
}

func (lm *LocalLog) getCurrentOffsetKV(k string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultKVTimeout*time.Second)
	defer cancel()
	offset, err := lm.LinKV.ReadInt(ctx, k)
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == 20 {
			return 0, nil
		}
		return 0, err
	}
	return offset, nil
}

// trySetKVOffset tries to set the current offset remotely. Returns the set offset, or
// the remote offset if it was greater than the amount to be set.
func (lm *LocalLog) trySetKVOffset(k string, offset int) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*defaultKVTimeout*time.Second)
	defer cancel()

	readOffset, err := lm.LinKV.ReadInt(ctx, k)
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == 20 {
			writeErr := lm.LinKV.Write(ctx, k, offset)
			if writeErr != nil {
				var writeRPCErr *maelstrom.RPCError
				if errors.As(writeErr, &writeRPCErr) && writeRPCErr.Code == 21 {
					return lm.trySetKVOffset(k, offset)
				}
				return 0, writeErr
			}
			return offset, nil
		}
		return 0, err
	}

	if readOffset >= offset {
		return readOffset, nil
	}

	err = lm.LinKV.CompareAndSwap(ctx, k, readOffset, offset, true)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (lm *LocalLog) setKVOffset(k string, offset int) (int, error) {
	var newOffset int
	var err error

	for range defaultKVRetries {
		newOffset, err = lm.trySetKVOffset(k, offset)
		if err == nil {
			break
		}

		shouldRetry := errIsPreconditionFailed(err) || errors.Is(err, context.DeadlineExceeded)
		if !shouldRetry {
			return 0, err
		}
	}

	return newOffset, nil
}

func (lm *LocalLog) setCommitOffsetSync(k string, offset int) error {
	newOffset, err := lm.setKVOffset(k, offset)
	if err != nil {
		return err
	}

	kd := lm.getOrCreateKeyData(k)
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.commitOffset = newOffset
	return nil
}

// GetCurrentOffsetSync synchronously gets the current offset from the KV store.
func (lm *LocalLog) GetCurrentOffsetSync(k string) (int, error) {
	offset, err := callWithRetry(defaultKVRetries, func() (int, error) {
		return lm.getCurrentOffsetKV(k)
	})
	if err != nil {
		return 0, err
	}
	if offset == 0 {
		return 0, ErrKeyNotFound
	}

	kd := lm.getOrCreateKeyData(k)
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.commitOffset = offset
	return offset, nil
}

// GetMsgsFromOffset reads msgs starting from offset with caching.
// Returns an empty slice if the key doesn't exist or offset is not found.
func (lm *LocalLog) GetMsgsFromOffset(k string, offset int) ([]LogMsg, error) {
	val, ok := lm.data.Load(k)
	if !ok {
		return []LogMsg{}, nil
	}

	kd := val.(*keyData)
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	if len(kd.msgs) == 0 {
		return []LogMsg{}, nil
	}

	idx, found := getLocationOfOffset(offset, kd.msgs)
	if !found {
		return []LogMsg{}, nil
	}

	result := make([]LogMsg, len(kd.msgs)-idx)
	copy(result, kd.msgs[idx:])
	return result, nil
}

// CommitOffset commits the offset to the KV store if it's greater than the stored offset.
func (lm *LocalLog) CommitOffset(k string, offset int) error {
	localOffset, exists := lm.getLocalOffset(k)
	if exists && localOffset >= offset {
		return nil
	}
	return lm.setCommitOffsetSync(k, offset)
}

func (lm *LocalLog) getNextOffsetKV(k string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*defaultKVTimeout*time.Second)
	defer cancel()

	for range defaultKVRetries {
		current, err := lm.LinKV.ReadInt(ctx, k)
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == 20 {
				current = defaultOffset
			} else {
				return 0, err
			}
		}

		next := current + offsetInc

		err = lm.LinKV.CompareAndSwap(ctx, k, current, next, true)
		if err != nil {
			var casErr *maelstrom.RPCError
			if errors.As(err, &casErr) && casErr.Code == 22 {
				continue
			}
			return 0, err
		}

		return current, nil
	}

	return 0, fmt.Errorf("%w: getNextOffsetKV", ErrMaxRetriesExceeded)
}

func (lm *LocalLog) AppendMsg(k string, msg int) (int, error) {
	offset, err := lm.getNextOffsetKV(k)
	if err != nil {
		return 0, fmt.Errorf("getNextOffsetKV: %w", err)
	}

	kd := lm.getOrCreateKeyData(k)
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.msgs = append(kd.msgs, LogMsg{offset, msg})
	kd.commitOffset = offset
	return offset, nil
}

func (lm *LocalLog) AppendMsgLocal(k string, msg int, offset int) error {
	kd := lm.getOrCreateKeyData(k)
	kd.mu.Lock()
	defer kd.mu.Unlock()

	// Update high water mark for local offset tracking
	if offset > kd.commitOffset {
		kd.commitOffset = offset
	}

	newMsg := LogMsg{offset, msg}
	idx, found := slices.BinarySearchFunc(kd.msgs, offset,
		func(m LogMsg, o int) int { return cmp.Compare(m.offset, o) })
	if found {
		return nil // Already exists
	}

	// Insert at correct position to maintain sorted order
	kd.msgs = slices.Insert(kd.msgs, idx, newMsg)
	return nil
}
