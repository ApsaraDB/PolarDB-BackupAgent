package main

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/chacha20"
)

type ChaCha20 struct {
	baseReader io.Reader
	baseWriter io.Writer
	passByte   [32]byte
	cipher     *chacha20.Cipher
}

func NewChaCha20Reader(pass string, reader io.Reader) *ChaCha20 {
	c := new(ChaCha20)
	c.baseReader = reader
	c.passByte = sha256.Sum256([]byte(pass))
	nonce := make([]byte, 24)
	c.cipher, _ = chacha20.NewUnauthenticatedCipher(c.passByte[:32], nonce)
	return c
}

func NewChaCha20Writer(pass string, writer io.Writer) *ChaCha20 {
	c := new(ChaCha20)
	c.baseWriter = writer
	c.passByte = sha256.Sum256([]byte(pass))
	nonce := make([]byte, 24)
	c.cipher, _ = chacha20.NewUnauthenticatedCipher(c.passByte[:32], nonce)
	return c
}

func (c *ChaCha20) Read(p []byte) (n int, err error) {
	n, err = c.baseReader.Read(p)
	if n > 0 {
		p = p[:n]
		c.cipher.XORKeyStream(p, p)
	}
	return n, err
}

func (c *ChaCha20) Write(p []byte) (n int, err error) {
	n = len(p)
	dst := make([]byte, n)
	c.cipher.XORKeyStream(dst, p)
	c.baseWriter.Write(dst)
	return n, nil
}
