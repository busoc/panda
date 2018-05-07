package image

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
)

func ImageRGB(x, y int, points []byte) image.Image {
	g := image.NewRGBA(image.Rect(0, 0, x, y))
	buf := bytes.NewBuffer(points)
	for i := 0; i < y; i++ {
		for j := 0; j < x; j++ {
			var red, green, blue uint8
			binary.Read(buf, binary.BigEndian, &red)
			binary.Read(buf, binary.BigEndian, &green)
			binary.Read(buf, binary.BigEndian, &blue)

			g.Set(j, i, color.RGBA{R: red, G: green, B: blue, A: 255})
		}
	}
	return g
}

func ImageLBR(x, y int, points []byte) image.Image {
	g := image.NewYCbCr(image.Rect(0, 0, x, y), image.YCbCrSubsampleRatio422)

	ls := make([]byte, 0, len(points)/2)
	bs := make([]byte, 0, len(points)/4)
	rs := make([]byte, 0, len(points)/4)

	for i := 0; i < len(points); i += 4 {
		ls = append(ls, points[i], points[i+2])
		bs = append(rs, points[i+1])
		rs = append(rs, points[i+3])
	}
	copy(g.Y, ls)
	copy(g.Cb, bs)
	copy(g.Cr, rs)

	return g
}

func ImageI420(x, y int, points []byte) image.Image {
	g := image.NewYCbCr(image.Rect(0, 0, x, y), image.YCbCrSubsampleRatio420)

	s := x * y
	z := s / 4
	copy(g.Y, points[:s])

	copy(g.Cb, points[s:s+z])
	copy(g.Cr, points[len(points)-z:])

	return g
}

func ImageGray8(x, y int, points []byte) image.Image {
	g := image.NewGray(image.Rect(0, 0, x, y))
	buf := bytes.NewBuffer(points)
	for i := 0; i < y; i++ {
		for j, c := range buf.Next(x) {
			g.Set(j, i, color.Gray{Y: uint8(c)})
		}
	}
	return g
}

func ImageGray16(x, y int, points []byte, order binary.ByteOrder) image.Image {
	g := image.NewGray16(image.Rect(0, 0, x, y))
	buf := bytes.NewBuffer(points)
	for i := 0; i < y; i++ {
		for j := 0; j < x; j++ {
			var c uint16
			binary.Read(buf, order, &c)
			g.Set(j, i, color.Gray16{Y: c})
		}
	}
	return g
}
