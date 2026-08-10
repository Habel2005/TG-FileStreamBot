package transcode

import (
	"EverythingSuckz/fsb/internal/stream"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

const sessionTTL = 30 * time.Minute

type Session struct {
	Dir string
	mu sync.Mutex
	lastAccess time.Time
	cmd *exec.Cmd
	finished bool
}

var sessions = struct {
	sync.RWMutex
	items map[string]*Session
}{items: make(map[string]*Session)}

var cleanupOnce sync.Once

func Start(client *gotgproto.Client, location tg.InputFileLocationClass, size int64, height int, log *zap.Logger) (string, *Session, error) {
	if size <= 0 { return "", nil, fmt.Errorf("invalid source size: %d", size) }
	if height < 144 { height = 144 }
	if height > 1080 { height = 1080 }

	id, err := randomID(); if err != nil { return "", nil, err }
	dir, err := os.MkdirTemp("", "fsb-hls-"); if err != nil { return "", nil, err }

	pipe, err := stream.NewStreamPipe(contextBackground{}, client, location, 0, size-1, log)
	if err != nil { _ = os.RemoveAll(dir); return "", nil, err }

	playlist := filepath.Join(dir, "index.m3u8")
	segments := filepath.Join(dir, "segment-%06d.ts")
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-re", "-i", "pipe:0",
		"-map", "0:v:0?", "-map", "0:a:0?",
		"-vf", fmt.Sprintf("scale=-2:%d", height),
		"-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", "96k",
		"-f", "hls", "-hls_time", "6", "-hls_list_size", "8",
		"-hls_flags", "delete_segments",
		"-hls_segment_filename", segments, playlist,
	}

	cmd := exec.Command("ffmpeg", args...)
	cmd.Stdin = pipe
	s := &Session{Dir: dir, lastAccess: time.Now(), cmd: cmd}
	sessions.Lock(); sessions.items[id] = s; sessions.Unlock()

	if err := cmd.Start(); err != nil {
		_ = pipe.Close(); _ = os.RemoveAll(dir)
		sessions.Lock(); delete(sessions.items, id); sessions.Unlock()
		return "", nil, fmt.Errorf("start ffmpeg: %w", err)
	}

	go func() {
		err := cmd.Wait(); _ = pipe.Close()
		s.mu.Lock(); s.finished = true; s.mu.Unlock()
		if err != nil { log.Error("HLS transcoder exited", zap.String("session", id), zap.Error(err)) }
	}()

	cleanupOnce.Do(func() { go cleanupLoop() })
	return id, s, nil
}

type contextBackground struct{}
func (contextBackground) Deadline() (time.Time, bool) { return time.Time{}, false }
func (contextBackground) Done() <-chan struct{} { return nil }
func (contextBackground) Err() error { return nil }
func (contextBackground) Value(any) any { return nil }

func Touch(id string) (*Session, bool) {
	sessions.RLock(); s := sessions.items[id]; sessions.RUnlock()
	if s == nil { return nil, false }
	s.mu.Lock(); s.lastAccess = time.Now(); s.mu.Unlock()
	return s, true
}

func FilePath(s *Session, name string) (string, bool) {
	name = filepath.Base(name)
	if name != "index.m3u8" && !strings.HasSuffix(name, ".ts") { return "", false }
	path := filepath.Join(s.Dir, name)
	if _, err := os.Stat(path); err != nil { return "", false }
	return path, true
}

func Remove(id string) {
	sessions.Lock(); s := sessions.items[id]; delete(sessions.items, id); sessions.Unlock()
	if s != nil {
		s.mu.Lock()
		if s.cmd != nil && s.cmd.Process != nil && !s.finished { _ = s.cmd.Process.Kill() }
		s.mu.Unlock()
		_ = os.RemoveAll(s.Dir)
	}
}

func cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second); defer ticker.Stop()
	for range ticker.C {
		now := time.Now(); var expired []string
		sessions.RLock()
		for id, s := range sessions.items {
			s.mu.Lock(); last := s.lastAccess; s.mu.Unlock()
			if now.Sub(last) > sessionTTL { expired = append(expired, id) }
		}
		sessions.RUnlock()
		for _, id := range expired { Remove(id) }
	}
}

func randomID() (string, error) {
	b := make([]byte, 18); if _, err := rand.Read(b); err != nil { return "", err }
	return hex.EncodeToString(b), nil
}
