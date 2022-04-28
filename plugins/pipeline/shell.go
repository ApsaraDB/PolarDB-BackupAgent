package main

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

func exeBash(shellname string) error {
	ex, err := os.Executable()
	if err != nil {
		return err
	}
	exPath := filepath.Dir(ex)

	shellPath := exPath + "/" + shellname
	command := exec.Command(shellPath)
	err = command.Start()
	if nil != err {
		errstr := "execute" + shellPath + " faild, error: " + err.Error()
		return errors.New(errstr)
	}
	err = command.Wait()
	if nil != err {
		errstr := "execute" + shellPath + " faild, error: " + err.Error()
		return errors.New(errstr)
	}
	return nil
}

func exeBashWithParams(action string, shellpath string, pbd string, hostlist string) error {
	ex, err := os.Executable()
	if err != nil {
		return err
	}
	exPath := filepath.Dir(ex)

	atcionShellPath := exPath + "/" + action
	cmd := atcionShellPath + " " + shellpath + " " + pbd + " " + hostlist
	command := exec.Command("/bin/bash", "-c", cmd)
	var out bytes.Buffer

	command.Stdout = &out
	err = command.Run()
	if err != nil {
		errstr := "execute" + cmd + " faild, error: " + err.Error()
		return errors.New(errstr)
	}
	return nil
}

func readLastLine(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var lineText string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineText = scanner.Text()
	}
	return string(lineText), nil
}

func getPbd() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}
	exPath := filepath.Dir(ex)
	pbdPath := exPath + "pbd.tmp"
	pbd, err := readLastLine(pbdPath)
	return pbd, err
}
