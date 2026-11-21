//go:build linux

package sif

import (
	"bytes"
	"testing"
	"time"

	apsif "github.com/apptainer/sif/v2/pkg/sif"
	"github.com/google/uuid"
)

type sifObject struct {
	typ  int32
	data []byte
	opts []apsif.DescriptorInputOpt
}

// createSIF writes a SIF with the apptainer/sif library.
func createSIF(t testing.TB, out string, objs []sifObject) {
	t.Helper()
	var dis []apsif.DescriptorInput
	for _, o := range objs {
		di, err := apsif.NewDescriptorInput(apsif.DataType(o.typ), bytes.NewReader(o.data), o.opts...)
		if err != nil {
			t.Fatal(err)
		}
		dis = append(dis, di)
	}
	f, err := apsif.CreateContainerAtPath(out,
		apsif.OptCreateWithDescriptors(dis...),
		apsif.OptCreateWithID(uuid.NewString()),
		apsif.OptCreateWithTime(time.Unix(1700000000, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.UnloadContainer(); err != nil {
		t.Fatal(err)
	}
}

func TestConstantsMatchLibrary(t *testing.T) {
	for got, want := range map[int32]apsif.DataType{
		DataDeffile: apsif.DataDeffile, DataEnvVar: apsif.DataEnvVar, DataLabels: apsif.DataLabels,
		DataPartition: apsif.DataPartition, DataSignature: apsif.DataSignature,
		DataGenericJSON: apsif.DataGenericJSON, DataGeneric: apsif.DataGeneric,
		DataCryptoMsg: apsif.DataCryptoMessage, DataSBOM: apsif.DataSBOM,
		DataOCIRootIndex: apsif.DataOCIRootIndex, DataOCIBlob: apsif.DataOCIBlob,
	} {
		if got != int32(want) {
			t.Errorf("data type %#x, library has %#x (%v)", got, int32(want), want)
		}
	}
	for got, want := range map[int32]apsif.FSType{
		FsSquash: apsif.FsSquash, FsExt3: apsif.FsExt3, FsImmuObj: apsif.FsImmuObj,
		FsRaw: apsif.FsRaw, FsEncryptedSquashfs: apsif.FsEncryptedSquashfs,
	} {
		if got != int32(want) {
			t.Errorf("fs type %d, library has %d (%v)", got, int32(want), want)
		}
	}
	for got, want := range map[int32]apsif.PartType{
		PartSystem: apsif.PartSystem, PartPrimSys: apsif.PartPrimSys,
		PartData: apsif.PartData, PartOverlay: apsif.PartOverlay,
	} {
		if got != int32(want) {
			t.Errorf("partition type %d, library has %d (%v)", got, int32(want), want)
		}
	}
}
