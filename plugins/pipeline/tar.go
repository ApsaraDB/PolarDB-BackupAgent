package main

import (
	"archive/tar"
	"io"
	"os"
)

type ArchiveFile struct {
	name string
	r    *os.File
	w    *os.File
	tw   *tar.Writer
	tr   *tar.Reader
}

func NewArchive(name string) (*ArchiveFile, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(w)
	tf := &ArchiveFile{
		name: name,
		r:    r,
		w:    w,
		tw:   tw,
	}
	return tf, nil
}

func (tf *ArchiveFile) AddMeta(name string, size int64) error {
	hdr := &tar.Header{
		Name: name,
		Mode: 0600,
		Size: size,
	}
	err := tf.tw.WriteHeader(hdr)
	if err != nil {
		return err
	}
	return nil
}

func (tf *ArchiveFile) AddDir(name string) error {
	hdr := &tar.Header{
		Name:     name,
		Mode:     0700,
		Typeflag: tar.TypeDir,
	}
	return tf.tw.WriteHeader(hdr)
}

func (tf *ArchiveFile) Add(name string, size int64, reader io.Reader, buf []byte) error {
	hdr := &tar.Header{
		// remove leading \ from the filename
		Name: name[1:],
		Mode: 0600,
		Size: size,
	}
	err := tf.tw.WriteHeader(hdr)
	if err != nil {
		return err
	}

	_, err = io.CopyBuffer(tf.tw, reader, buf)
	return err
}

func NewExtraArchive(name string, reader io.Reader) *ArchiveFile {
	tr := tar.NewReader(reader)

	tf := &ArchiveFile{
		name: name,
		tr:   tr,
	}
	return tf
}

func (tf *ArchiveFile) Next() (*tar.Header, io.Reader, error) {
	hdr, err := tf.tr.Next()
	if err == io.EOF {
		return nil, nil, io.EOF
	}

	if err != nil {
		return nil, nil, err
	}

	return hdr, tf.tr, nil
}
