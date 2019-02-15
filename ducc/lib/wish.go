package lib

import (
	"fmt"
)

type Wish struct {
	Id          int
	InputImage  int
	OutputImage int
	CvmfsRepo   string
}

type WishFriendly struct {
	Id         int
	InputName  string
	OutputName string
	CvmfsRepo  string
	Converted  bool
	UserInput  string
	UserOutput string
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
	return
}
