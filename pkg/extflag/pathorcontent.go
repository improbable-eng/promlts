// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extflag

import (
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

// PathOrContent is a flag type that defines two flags to fetch bytes. Either from file (*-file flag) or content (* flag).
type PathOrContent struct {
	flagName string

	required bool

	path    *string
	content *string
}

type FlagClause interface {
	Flag(name, help string) *kingpin.FlagClause
}

// RegisterPathOrContent registers PathOrContent flag in kingpinCmdClause.
func RegisterPathOrContent(cmd FlagClause, flagName string, help string, required bool) *PathOrContent {
	fileFlagName := fmt.Sprintf("%s-file", flagName)
	contentFlagName := flagName

	fileHelp := fmt.Sprintf("Path to %s", help)
	fileFlag := cmd.Flag(fileFlagName, fileHelp).PlaceHolder("<file-path>").String()

	contentHelp := fmt.Sprintf("Alternative to '%s' flag (mutually exclusive). Content of %s", fileFlagName, help)
	contentFlag := cmd.Flag(contentFlagName, contentHelp).PlaceHolder("<content>").String()

	return &PathOrContent{
		flagName: flagName,
		required: required,
		path:     fileFlag,
		content:  contentFlag,
	}
}

// Content returns the content of the file when given or directly the content that has been passed to the flag.
// It returns an error when:
// * The file and content flags are both not empty.
// * The file flag is not empty but the file can't be read.
// * The content is empty and the flag has been defined as required.
func (p *PathOrContent) Content() ([]byte, error) {
	fileFlagName := fmt.Sprintf("%s-file", p.flagName)

	if len(*p.path) > 0 && len(*p.content) > 0 {
		return nil, errors.Errorf("both %s and %s flags set.", fileFlagName, p.flagName)
	}

	var content []byte
	if len(*p.path) > 0 {
		c, err := ioutil.ReadFile(*p.path)
		if err != nil {
			return nil, errors.Wrapf(err, "loading file %s for %s", *p.path, fileFlagName)
		}
		content = c
	} else {
		content = []byte(*p.content)
	}

	if len(content) == 0 && p.required {
		return nil, errors.Errorf("flag %s or %s is required for running this command and content cannot be empty.", fileFlagName, p.flagName)
	}

	return content, nil
}

func (p *PathOrContent) GetPath() ([]string, error) {
	var path []string
	if len(*p.path) > 0 {
		path = append(path, *p.path)
		return path, nil
	}
	return path, errors.Errorf("path is empty.")
}
