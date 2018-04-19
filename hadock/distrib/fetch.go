package distrib

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/busoc/panda"
	img "github.com/busoc/panda/internal/image"
	"github.com/busoc/panda/internal/science"
	"github.com/gorilla/handlers"
)

var (
	ErrNotModified    = errors.New("not modified")
	ErrNotFound       = errors.New("not found")
	ErrNotImplemented = errors.New("not implemented")
)

type fetcher struct {
	rawdir  string
	datadir string
}

type file struct {
	fcc  uint32
	seq  uint32
	when int64
	sum  []byte
	buf  *bytes.Buffer
}

func (f *file) AsRaw() (io.Reader, error) {
	r := new(bytes.Buffer)
	binary.Write(r, binary.BigEndian, f.fcc)
	binary.Write(r, binary.BigEndian, f.seq)
	binary.Write(r, binary.BigEndian, f.when)
	return io.MultiReader(r, f.buf), nil
}

func (f *file) AsScience() (io.Reader, error) {
	fcc := make([]byte, 4)
	binary.BigEndian.PutUint32(fcc, f.fcc)
	var (
		r   bytes.Buffer
		err error
	)
	switch bs, m := f.buf.Bytes(), f.ModTime(); {
	case bytes.Equal(fcc, mud.SYNC):
		err = science.ExportSyncUnit(&r, bs, m)
	case bytes.Equal(fcc, mud.MMA):
		err = science.ExportScienceData(&r, bs, m)
	case bytes.Equal(fcc, mud.SVS):
		err = science.ExportSVSData(&r, bs)
	default:
		return nil, ErrNotImplemented
	}
	return &r, err
}

func (f *file) AsImage(t string) (io.Reader, error) {
	var x, y uint16
	binary.Read(f.buf, binary.BigEndian, &x)
	binary.Read(f.buf, binary.BigEndian, &y)

	var (
		i   image.Image
		err error
	)
	fcc := make([]byte, 4)
	binary.BigEndian.PutUint32(fcc, f.fcc)
	switch {
	default:
		err = fmt.Errorf("not supported %x", f.fcc)
	case bytes.Equal(fcc, mud.JPEG) || bytes.Equal(fcc, mud.PNG):
		i, _, err = image.Decode(f.buf)
	case bytes.Equal(fcc, mud.JPEG) || bytes.Equal(fcc, mud.YUY2):
		i = img.ImageLBR(int(x), int(y), f.buf.Bytes())
	case bytes.Equal(fcc, mud.JPEG) || bytes.Equal(fcc, mud.Y800):
		i = img.ImageGray8(int(x), int(y), f.buf.Bytes())
	case bytes.Equal(fcc, mud.JPEG) || bytes.Equal(fcc, mud.I420):
		i = img.ImageI420(int(x), int(y), f.buf.Bytes())
	case bytes.Equal(fcc, mud.JPEG) || bytes.Equal(fcc, mud.RGB):
		i = img.ImageRGB(int(x), int(y), f.buf.Bytes())
	}
	if err != nil {
		return nil, err
	}
	w := new(bytes.Buffer)
	switch t {
	case "jpg", "jpeg":
		err = jpeg.Encode(w, i, &jpeg.Options{100})
	case "png":
		err = png.Encode(w, i)
	case "gif":
		err = gif.Encode(w, i, new(gif.Options))
	default:
		err = png.Encode(w, i)
	}
	return w, err
}

func (f file) ModTime() time.Time {
	return mud.AdjustGenerationTime(f.when)
	// return time.Unix(f.when, 0)
}

func Fetch(r, d string) (http.Handler, error) {
	i, err := os.Stat(r)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("not a directory", r)
	}
	return handlers.CompressHandler(fetcher{rawdir: r, datadir: d}), nil
}

type Mime string

func (m Mime) String() string {
	return string(m)
}

func (m Mime) MainType() string {
	return strings.Split(string(m), "/")[0]
}

func (m Mime) SubType() string {
	return strings.Split(string(m), "/")[1]
}

const (
	MimeOctet = Mime("application/octet-stream")
	MimeGif   = Mime("image/gif")
	MimeJPG   = Mime("image/jpeg")
	MimePNG   = Mime("image/png")
	MimeCSV   = Mime("text/csv")
)

var types = []Mime{
	MimeOctet,
	MimeGif,
	MimePNG,
	MimeJPG,
	MimeCSV,
}

func (f fetcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isAcceptable(r.Header.Get("accept"), "application/xml") {
		f, err := os.Open(filepath.Join(f.rawdir, r.URL.Path))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer f.Close()
		io.Copy(w, f)

		return
	}
	// if ok := f.copyFile(w, r.URL.Path); ok {
	// 	return
	// }
	var mod time.Time
	bs, err := readFile(filepath.Join(f.rawdir, r.URL.Path), mod)
	switch err {
	case nil:
		break
	case ErrNotFound:
		w.WriteHeader(http.StatusNotFound)
		return
	case ErrNotModified:
		w.WriteHeader(http.StatusNotModified)
		return
	case ErrNotImplemented:
		w.WriteHeader(http.StatusNotImplemented)
		return
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	mime, ok := accept(r.Header.Get("accept"), types)
	if !ok {
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}
	var rs io.Reader
	switch mime {
	case MimeOctet:
		rs, err = bs.AsRaw()
	case MimeGif, MimeJPG, MimePNG:
		rs, err = bs.AsImage(mime.SubType())
	case MimeCSV:
		rs, err = bs.AsScience()
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("etag", fmt.Sprintf("%x", bs.sum))
	w.Header().Set("last-modified", bs.ModTime().Format(time.RFC1123))
	w.Header().Set("expires", time.Now().Add(time.Hour*24).Format(time.RFC1123))
	w.Header().Set("content-type", mime.String())
	io.Copy(w, rs)
}

func (f fetcher) copyFile(w io.Writer, p string) bool {
	r, err := os.Open(filepath.Join(f.datadir, p))
	if err != nil {
		return false
	}
	defer r.Close()
	io.Copy(w, r)
	return true
}

func readFile(p string, m time.Time) (*file, error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer f.Close()
	if i, err := f.Stat(); err == nil && (!m.IsZero() && m.Sub(i.ModTime()) <= 0) {
		return nil, ErrNotModified
	}

	fs := &file{buf: new(bytes.Buffer)}
	binary.Read(f, binary.BigEndian, &fs.fcc)
	binary.Read(f, binary.BigEndian, &fs.seq)
	binary.Read(f, binary.BigEndian, &fs.when)

	s := md5.New()
	if _, err := io.Copy(io.MultiWriter(fs.buf, s), f); err != nil {
		return nil, err
	} else {
		fs.sum = s.Sum(nil)
	}
	return fs, nil
}

func isAcceptable(a string, vs ...string) bool {
	if len(vs) == 0 {
		return true
	}
	ms := make([]Mime, len(vs))
	for i := 0; i < len(ms); i++ {
		ms[i] = Mime(vs[i])
	}

	_, ok := accept(a, ms)
	return ok
}

func accept(a string, vs []Mime) (Mime, bool) {
	for _, v := range vs {
		if ix := strings.Index(a, v.String()); ix >= 0 {
			return v, true
		}
	}
	return "", false
}
