// Package walker wraps calls to the the fswalker Walker
package walker

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
)

// Walk performs a walk governed by the contents of the provided
// Policy, and writes the output to the provided io.Writer.
func Walk(ctx context.Context, policy *fspb.Policy, w io.Writer) error { //nolint:interfacer
	f, err := ioutil.TempFile("", "fswalker-policy-")
	if err != nil {
		return err
	}

	f.Close() //nolint:errcheck

	policyFileName := f.Name()
	defer os.RemoveAll(policyFileName) //nolint:errcheck

	err = writeTextProto(policyFileName, policy)
	if err != nil {
		return err
	}

	walker, err := fswalker.WalkerFromPolicyFile(ctx, policyFileName)
	if err != nil {
		return err
	}

	walker.WalkCallback = func(ctx context.Context, walk *fspb.Walk) error {
		return writeWalk(walk, w)
	}

	err = walker.Run(ctx)
	if err != nil {
		return err
	}

	return nil
}

func writeWalk(walk *fspb.Walk, w io.Writer) error {
	walkBytes, err := proto.Marshal(walk)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(walkBytes))
	if err != nil {
		return err
	}

	return nil
}

// writeTextProto writes a text format proto buf for the provided proto message.
func writeTextProto(path string, pb proto.Message) error {
	blob := proto.MarshalTextString(pb)
	// replace message boundary characters as curly braces look nicer (both is fine to parse)
	blob = strings.Replace(strings.Replace(blob, "<", "{", -1), ">", "}", -1)

	return ioutil.WriteFile(path, []byte(blob), 0644)
}
