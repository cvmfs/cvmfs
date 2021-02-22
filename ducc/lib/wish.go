package lib

import (
	"fmt"
	"sync"

	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

type Wish struct {
	Id          int
	InputImage  int
	OutputImage int
	CvmfsRepo   string
}

type WishFriendly struct {
	Id                     int
	InputName              string
	OutputName             string
	CvmfsRepo              string
	Converted              bool
	UserInput              string
	UserOutput             string
	InputImage             *Image
	OutputImage            *Image
	ExpandedTagImagesLayer []*Image
	ExpandedTagImagesFlat  []*Image
}

func CreateWish(inputImage, outputImage, cvmfsRepo, userInput, userOutput string) (wish WishFriendly, err error) {

	inputImg, err := ParseImage(inputImage)
	if err != nil {
		err = fmt.Errorf("%s | %s", err.Error(), "Error in parsing the input image")
		return
	}
	inputImg.User = userInput

	wish.InputName = inputImg.WholeName()

	outputImg, err := ParseImage(outputImage)
	if err != nil {
		err = fmt.Errorf("%s | %s", err.Error(), "Error in parsing the output image")
		return
	}
	outputImg.User = userOutput
	outputImg.IsThin = true
	wish.OutputName = outputImg.WholeName()

	wish.Id = 0
	wish.CvmfsRepo = cvmfsRepo
	wish.UserInput = userInput
	wish.UserOutput = userOutput

	iImage, errI := ParseImage(wish.InputName)
	wish.InputImage = &iImage
	wish.InputImage.User = wish.UserInput
	if errI != nil {
		wish.InputImage = nil
		err = errI
		return
	}
	r1, r2, errEx := iImage.ExpandWildcard()
	if errEx != nil {
		err = errEx
		l.LogE(err).WithFields(log.Fields{
			"input image": inputImage}).
			Error("Error in retrieving all the tags from the image")
		return
	}
	var expandedTagImagesLayer, expandedTagImagesFlat []*Image
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for img := range r1 {
			expandedTagImagesLayer = append(expandedTagImagesLayer, img)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for img := range r2 {
			expandedTagImagesFlat = append(expandedTagImagesFlat, img)
		}
	}()
	wg.Wait()

	wish.ExpandedTagImagesLayer = expandedTagImagesLayer
	wish.ExpandedTagImagesFlat = expandedTagImagesFlat

	oImage, errO := ParseImage(wish.OutputName)
	wish.OutputImage = &oImage
	wish.OutputImage.User = wish.UserOutput
	if errO != nil {
		wish.OutputImage = nil
		err = errO
		return
	}

	return
}
