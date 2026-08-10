package transcode

import (
	"EverythingSuckz/fsb/internal/stream"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
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

const (
	segmentTTL = 10 * time.Minute
	idleTTL    = 2 * time.Minute
)

type Session struct {
	Dir string

	mu         sync.Mutex
	lastAccess time.Time
	cmd        *exec.Cmd
	finished   bool
}

var sessions = struct {
	sync.RWMutex
	items map[string]*Session
}{items: make(map[string]*Session)}

func Start(client *gotgproto.Client, location tg.InputFileLocationClass, size int64, height int, log *zap.Logger) (string, *Session, error) {
	if height < 144 {
		height = 144
	}
	if height > 1080 {
		height = 1080
	}

	id, err := randomID()
	if err != nil {
		return "", nil, err
	}

	dir, err := os.MkdirTemp("", "fsb-hls-")
	if err != nil {
		return "", nil, err
	}

	pipe, err := stream.NewStreamPipe(nilContext{}, client, location, 0, size-1, log)
	if err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}

	playlist := filepath.Join(dir, "index.m3u8")
	segments := filepath.Join(dir, "segment-%06d.ts")

	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-i", "pipe:0",
		"-map", "0:v:0?",
		"-map", "0:a:0?",
		"-vf", fmt.Sprintf("scale=-2:%d", height),
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "28",
		"-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-b:a", "96k",
		"-f", "hls",
		"-hls_time", "6",
		"-hls_list_size", "0",
		"-hls_segment_filename", segments,
		playlist,
	}

	cmd := exec.Command("ffmpeg", args...)
	cmd.Stdin = pipe

	s := &Session{Dir: dir, lastAccess: time.Now(), cmd: cmd}
	sessions.Lock()
	sessions.items[id] = s
	sessions.Unlock()

	if err := cmd.Start(); err != nil {
		pipe.Close()
		os.RemoveAll(dir)
		sessions.Lock()
		delete(sessions.items, id)
		sessions.Unlock()
		return "", nil, err
	}

	go func() {
		err := cmd.Wait()
		pipe.Close()
		s.mu.Lock()
		s.finished = true
		s.mu.Unlock()
		if err != nil {
			log.Error("HLS transcoder exited", zap.String("session", id), zap.Error(err))
		}
	}()

	go cleanupLoop()
	return id, s, nil
}

// nilContext is a tiny context implementation whose Done channel never fires.
// StreamPipe only needs a context; the session lifecycle is controlled by the manager.
type nilContext struct{}

func (nilContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (nilContext) Done() <-chan struct{}       { return nil }
func (nilContext) Err() error                  { return nil }
func (nilContext) Value(any) any               { return nil }

func Touch(id string) (*Session, bool) {
	sessions.RLock()
	s := sessions.items[id]
	sessions.RUnlock()
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	s.lastAccess = time.Now()
	s.mu.Unlock()
	return s, true
}

func FilePath(s *Session, name string) (string, bool) {
	name = filepath.Base(name)
	if name != "index.m3u8" && !strings.HasSuffix(name, ".ts") {
		return "", false
	}
	path := filepath.Join(s.Dir, name)
	if _, err := os.Stat(path); err != nil {
		return "", false
	}
	return path, true
}

func cleanupLoop() {
	// A cheap self-cleaning loop is enough here; only active sessions remain in memory.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			var expired []string

			sessions.RLock()
			for id, s := range sessions.items {
				s.mu.Lock()
				last := s.lastAccess
				finished := s.finished
				cmd := s.cmd
				s.mu.Unlock()
				if now.Sub(last) > segmentTTL || (!finished && now.Sub(last) > idleTTL) {
					if cmd != nil && cmd.Process != nil && !finished {
						_ = cmd.Process.Kill()
					}
					expired = append(expired, id)
				}
			}
			sessions.RUnlock()

			for _, id := range expired {
				Remove(id)
			}
		}
	}()
}

var cleanupOnce sync.Once

func Remove(id string) {
	sessions.Lock()
	s := sessions.items[id]
	delete(sessions.items, id)
	sessions.Unlock()
	if s != nil {
		_ = os.RemoveAll(s.Dir)
	}
}

func randomID() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func IsNotReady(path string) bool {
	_, err := os.Stat(path)
	return err != nil && (err == fs.ErrNotExist || os.IsNotExist(err))
}
