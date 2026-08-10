package routes

import (
	"EverythingSuckz/fsb/config"
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/stream"
	"EverythingSuckz/fsb/internal/transcode"
	"EverythingSuckz/fsb/internal/types"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
	r.Engine.GET("/hls/:sessionID/*filename", getHLSFile)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "missing hash param", http.StatusBadRequest)
		return
	}

	worker := bot.GetNextWorker()

	file, err := utils.TimeFuncWithResult(log, "FileFromMessage", func() (*types.File, error) {
		return utils.FileFromMessage(ctx, worker.Client, messageID)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	expectedHash := utils.PackFile(file.FileName, file.FileSize, file.MimeType, file.ID)
	if !utils.CheckHash(authHash, expectedHash) {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	// Optional on-demand transcoding. Existing URLs are unchanged unless
	// ?transcode=480 (or another requested height) is added.
	if r.Method == http.MethodGet && strings.HasPrefix(strings.ToLower(file.MimeType), "video/") {
		if requested := ctx.Query("transcode"); requested != "" && config.ValueOf.TranscodeEnabled {
			height, parseErr := strconv.Atoi(requested)
			if parseErr != nil || height <= 0 {
				http.Error(w, "invalid transcode height", http.StatusBadRequest)
				return
			}
			sessionID, _, startErr := transcode.Start(worker.Client, file.Location, file.FileSize, height, log)
			if startErr != nil {
				log.Error("Failed to start transcoder", zap.Error(startErr))
				http.Error(w, "failed to start transcoder", http.StatusServiceUnavailable)
				return
			}
			ctx.Redirect(http.StatusFound, "/hls/"+sessionID+"/index.m3u8")
			return
		}
	}

	// for photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset: 0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected response", http.StatusInternalServerError)
			return
		}
		fileBytes := result.GetBytes()
		ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		if r.Method != "HEAD" {
			ctx.Data(http.StatusOK, file.MimeType, fileBytes)
		}
		return
	}

	ctx.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(file.FileSize, r.Header.Get("Range"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		log.Info("Content-Range", zap.Int64("start", start), zap.Int64("end", end), zap.Int64("fileSize", file.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := file.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"
	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}
	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	if r.Method != "HEAD" {
		pipe, err := stream.NewStreamPipe(ctx, worker.Client, file.Location, start, end, log)
		if err != nil {
			log.Error("Failed to create stream pipe", zap.Error(err))
			return
		}
		defer pipe.Close()
		if _, err := io.CopyN(w, pipe, contentLength); err != nil {
			if !utils.IsClientDisconnectError(err) {
				log.Error("Error while copying stream", zap.Error(err))
			}
		}
	}
}

func getHLSFile(ctx *gin.Context) {
	sessionID := ctx.Param("sessionID")
	name := filepath.Base(ctx.Param("filename"))
	if name == "." || name == "" {
		name = "index.m3u8"
	}

	session, ok := transcode.Touch(sessionID)
	if !ok {
		http.Error(ctx.Writer, "stream session expired", http.StatusNotFound)
		return
	}

	// FFmpeg needs a moment to produce the first playlist/segment. Wait briefly
	// rather than failing the player immediately on the first request.
	var path string
	for i := 0; i < 100; i++ {
		if candidate, exists := transcode.FilePath(session, name); exists {
			path = candidate
			break
		}
		select {
		case <-ctx.Request.Context().Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if path == "" {
		http.Error(ctx.Writer, "stream segment not ready", http.StatusNotFound)
		return
	}

	if strings.HasSuffix(name, ".m3u8") {
		ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
		ctx.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	} else {
		ctx.Header("Content-Type", "video/mp2t")
		ctx.Header("Cache-Control", "public, max-age=60")
	}
	ctx.Header("Access-Control-Allow-Origin", "*")
	http.ServeFile(ctx.Writer, ctx.Request, path)
}
