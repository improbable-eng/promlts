// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package runutil

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type testCloser struct {
	err error
}

func (c testCloser) Close() error {
	return c.err
}

func TestCloseWithErrCapture(t *testing.T) {
	for _, tcase := range []struct {
		err    error
		closer io.Closer

		expectedErrStr string
	}{
		{
			err:            nil,
			closer:         testCloser{err: nil},
			expectedErrStr: "",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: nil},
			expectedErrStr: "test",
		},
		{
			err:            nil,
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "close: test",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "2 errors: test; close: test",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			ret := tcase.err
			CloseWithErrCapture(&ret, tcase.closer, "close")

			if tcase.expectedErrStr == "" {
				if ret != nil {
					t.Error("Expected error to be nil")
					t.Fail()
				}
			} else {
				if ret == nil {
					t.Error("Expected error to be not nil")
					t.Fail()
				}

				if tcase.expectedErrStr != ret.Error() {
					t.Errorf("%s != %s", tcase.expectedErrStr, ret.Error())
					t.Fail()
				}
			}

		}); !ok {
			return
		}
	}
}

type loggerCapturer struct {
	// WasCalled is true if the Log() function has been called.
	WasCalled bool
}

func (lc *loggerCapturer) Log(keyvals ...interface{}) error {
	lc.WasCalled = true
	return nil
}

type emulatedCloser struct {
	io.Reader

	calls int
}

func (e *emulatedCloser) Close() error {
	e.calls++
	if e.calls == 1 {
		return nil
	}
	if e.calls == 2 {
		return errors.Wrap(os.ErrClosed, "can even be a wrapped one")
	}
	return errors.New("something very bad happened")
}

// newEmulatedCloser returns a ReadCloser with a Close method
// that at first returns success but then returns that
// it has been closed already. After that, it returns that
// something very bad had happened.
func newEmulatedCloser(r io.Reader) io.ReadCloser {
	return &emulatedCloser{Reader: r}
}

func TestCloseMoreThanOnce(t *testing.T) {
	lc := &loggerCapturer{}
	r := newEmulatedCloser(strings.NewReader("somestring"))

	CloseWithLogOnErr(lc, r, "should not be called")
	CloseWithLogOnErr(lc, r, "should not be called")
	testutil.Equals(t, false, lc.WasCalled)

	CloseWithLogOnErr(lc, r, "should be called")
	testutil.Equals(t, true, lc.WasCalled)
}

func TestClearsDirectoriesFilesProperly(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	testutil.Ok(t, err)

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	f, err := os.Create(filepath.Join(dir, "test123"))
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())

	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW0TN8"), os.ModePerm))
	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW1TN8"), os.ModePerm))
	f, err = os.Create(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW0TN9"))
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())

	testutil.Ok(t, DeleteAll(dir, "01EHBQRN4RF0HSRR1772KW0TN8", "01EHBQRN4RF0HSRR1772KW0TN9"))

	_, err = os.Stat(filepath.Join(dir, "test123"))
	testutil.Assert(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW0TN9"))
	testutil.Assert(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW1TN8/"))
	testutil.Assert(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(dir, "01EHBQRN4RF0HSRR1772KW0TN8/"))
	testutil.Ok(t, err)
}
