// Package update discovers and installs GitHub Release binaries.
package update

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/x/term"
)

// Version is set by the release build. Development builds skip updates.
var Version = "dev"

const api = "https://api.github.com/repos/eyotang/s3flow/releases/latest"
const maxDownload = 256 << 20

type asset struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}
type release struct {
	Tag        string  `json:"tag_name"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	Assets     []asset `json:"assets"`
}

func newer(candidate, current string) bool {
	parse := func(s string) ([]uint64, bool) {
		parts := strings.Split(strings.TrimPrefix(s, "v"), ".")
		if len(parts) != 3 {
			return nil, false
		}
		values := make([]uint64, 3)
		for i, p := range parts {
			if p == "" || (len(p) > 1 && p[0] == '0') {
				return nil, false
			}
			for _, c := range p {
				if c < '0' || c > '9' {
					return nil, false
				}
			}
			n, err := strconv.ParseUint(p, 10, 64)
			if err != nil {
				return nil, false
			}
			values[i] = n
		}
		return values, true
	}
	a, ok := parse(candidate)
	if !ok {
		return false
	}
	b, ok := parse(current)
	if !ok {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return false
}

func get(ctx context.Context, client *http.Client, url, token, accept string, limit int64) ([]byte, error) {
	if !strings.HasPrefix(url, "https://api.github.com/") {
		return nil, fmt.Errorf("无效的 Release 地址")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", "s3flow/"+Version)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("Release 文件超过大小限制")
	}
	return data, nil
}

// BeforeRun returns true after installing an update, so the caller exits before
// starting a transfer. Failed checks never prevent the original command.
func BeforeRun(command string) bool {
	if Version == "dev" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	token := os.Getenv("GH_TOKEN")
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}
	if token == "" {
		// Reuse an existing gh login for private repositories, within the same deadline.
		data, err := exec.CommandContext(ctx, "gh", "auth", "token", "--hostname", "github.com").Output()
		if err == nil {
			token = strings.TrimSpace(string(data))
		}
	}
	client := &http.Client{}
	data, err := get(ctx, client, api, token, "application/vnd.github+json", 1<<20)
	if err != nil {
		return false
	}
	var r release
	if json.Unmarshal(data, &r) != nil || r.Draft || r.Prerelease || !newer(r.Tag, Version) {
		return false
	}
	cancel()
	fmt.Fprintf(os.Stderr, "s3flow 有新版本：%s → %s\n", Version, r.Tag)
	if !term.IsTerminal(os.Stdin.Fd()) || !term.IsTerminal(os.Stderr.Fd()) {
		return false
	}
	if !confirm(os.Stdin, os.Stderr) {
		return false
	}
	installCtx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stop()
	if err := install(installCtx, client, r, token, command); err != nil {
		fmt.Fprintf(os.Stderr, "更新失败：%v；继续使用当前版本。\n", err)
		return false
	}
	fmt.Fprintf(os.Stderr, "已更新至 %s，请重新运行本次命令。\n", r.Tag)
	return true
}

func install(ctx context.Context, client *http.Client, r release, token, command string) error {
	extension := ".tar.gz"
	binary := command
	if runtime.GOOS == "windows" {
		extension = ".zip"
		binary += ".exe"
	}
	archiveName := fmt.Sprintf("s3flow_%s_%s_%s%s", r.Tag, runtime.GOOS, runtime.GOARCH, extension)
	download := func(name string, limit int64) ([]byte, error) {
		for _, a := range r.Assets {
			if a.Name == name {
				return get(ctx, client, a.URL, token, "application/octet-stream", limit)
			}
		}
		return nil, fmt.Errorf("Release 缺少 %s", name)
	}
	sums, err := download("checksums.txt", 1<<20)
	if err != nil {
		return err
	}
	archive, err := download(archiveName, maxDownload)
	if err != nil {
		return err
	}
	if err = verify(archive, sums, archiveName); err != nil {
		return err
	}
	contents, err := extract(archive, binary, extension == ".zip")
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return err
	}
	return replace(executable, contents)
}

func verify(data, sums []byte, name string) error {
	sum := sha256.Sum256(data)
	for _, line := range strings.Split(string(sums), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && strings.TrimPrefix(strings.TrimPrefix(fields[1], "*"), "./") == name && fields[0] == hex.EncodeToString(sum[:]) {
			return nil
		}
	}
	return fmt.Errorf("Release SHA-256 校验失败")
}

func extract(data []byte, binary string, isZip bool) ([]byte, error) {
	read := func(r io.Reader) ([]byte, error) {
		b, e := io.ReadAll(io.LimitReader(r, maxDownload+1))
		if len(b) == 0 || len(b) > maxDownload {
			return nil, fmt.Errorf("无效的可执行文件大小")
		}
		return b, e
	}
	if isZip {
		archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		for _, f := range archive.File {
			if filepath.Base(f.Name) == binary && f.Mode().IsRegular() {
				r, err := f.Open()
				if err != nil {
					return nil, err
				}
				defer r.Close()
				return read(r)
			}
		}
	} else {
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		archive := tar.NewReader(gz)
		for {
			h, err := archive.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if filepath.Base(h.Name) == binary && h.Typeflag == tar.TypeReg {
				return read(archive)
			}
		}
	}
	return nil, fmt.Errorf("Release 未包含 %s", binary)
}

func replace(destination string, data []byte) error {
	info, err := os.Stat(destination)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".s3flow-update-*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer os.Remove(temp)
	if _, err = file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err = file.Chmod(info.Mode().Perm()); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		return os.Rename(temp, destination)
	}
	// Windows permits renaming a running image, but not overwriting it.
	backup := temp + ".old"
	if err = os.Rename(destination, backup); err != nil {
		return err
	}
	if err = os.Rename(temp, destination); err != nil {
		if restore := os.Rename(backup, destination); restore != nil {
			return fmt.Errorf("%v；恢复失败：%v（旧文件：%s）", err, restore, backup)
		}
		return err
	}
	_ = os.Remove(backup) // A running image may remain locked until this process exits.
	return nil
}

func confirm(input io.Reader, output io.Writer) bool {
	fmt.Fprint(output, "是否立即更新？[y/N] ")
	answer, err := bufio.NewReader(input).ReadString('\n')
	if err != nil {
		return false
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "y" || answer == "yes"
}
